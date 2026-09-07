# AstrBot 群复读机插件（适配 NapCatQQ / OneBot v11）
#
# 功能：当群内连续 N 个【不同的人】发送了相同内容时，机器人自动跟一条相同消息。
#  - N 可通过 WebUI 配置或群内指令自定义（默认 3）。
#  - 每个群可独立开关复读机模式（/复读机 开|关）。
#  - 支持触发冷却时间，防止机器人刷屏。
#
# 说明：核心逻辑在 process_message() 中，为纯同步函数，便于本地单元测试；
#      事件监听 / 指令处理只是薄封装。持久化数据存放于 AstrBot 的 data 目录。

import asyncio
import base64
import hashlib
import json
import os
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import At, Face, Image, Plain
from astrbot.api.star import Context, Star


# 指令组：群内通过 `/复读机 子指令` 控制复读机
@filter.command_group("复读机")
def repeat_group():
    """群复读机控制指令组：/复读机 开|关|阈值 N|冷却 N|状态"""
    pass


# 匹配指令调用本身（`/复读机 ...`、`!复读机 ...` 或 `复读机 ...`），
# 避免把指令消息误当成复读内容参与计数。
_CMD_PATTERN = re.compile(r"^[/!]?复读机(?:\s|$)")

_TERMINAL_CONTENT_HASH = re.compile(
    r"(?i)(?<![0-9a-f])([0-9a-f]{64}|[0-9a-f]{32})"
    r"(?=(?:\.[a-z0-9]{1,10})?(?:$|[?#&]))"
)
_UUID_CONTENT_HASH = re.compile(
    r"(?i)\{?([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-"
    r"([0-9a-f]{4})-([0-9a-f]{12})\}?"
)

_PERMISSION_RANK = {
    "member": 0,
    "admin": 1,
    "owner": 2,
    "bot_owner": 3,
}
_PERMISSION_LABEL = {
    "member": "群成员",
    "admin": "群管理员",
    "owner": "群主",
    "bot_owner": "BOT主人",
}

_CHAIN_FINGERPRINT_VERSION = "chain-v1"
_MAX_RAW_SEGMENTS = 64
_MAX_MEANINGFUL_PARTS = 32
_MAX_MEDIA_PARTS = 8
_MAX_TEXT_CHARS = 4096
_MAX_AT_PARTS = 8
_MAX_IMAGE_BYTES = 10 * 1024 * 1024
_MAX_TOTAL_IMAGE_BYTES = 20 * 1024 * 1024
_MAX_BASE64_OVERHEAD = 256


@dataclass(slots=True)
class _ChainPart:
    kind: str
    value: str = ""
    media_index: int = -1


class _MediaLimitError(ValueError):
    pass


@dataclass(slots=True)
class _RepeatCandidate:
    kind: str
    source: str
    component: object | None = None
    data: object | None = None
    fingerprint: str | None = None
    strategy: str = "pending"
    reason: str = "-"
    error_type: str = "-"
    channel: str = "event"
    outbound: object | None = None
    outbounds: tuple[object, ...] = ()
    probe: object | None = None
    probes: tuple[object, ...] = ()
    content_base64: str | None = None
    chain: tuple[tuple[str, str], ...] = ()
    raw_types: str = "none"
    parsed_types: str = "none"
    parts: tuple[_ChainPart, ...] = ()
    media: tuple["_RepeatCandidate", ...] = ()
    content_size: int | None = None


class _GroupState:
    """单个群的运行状态（含实时计数）"""

    __slots__ = (
        "enabled",
        "threshold",
        "cooldown",
        "lock",
        "current_fingerprint",
        "senders",
        "repeated_in_segment",
        "last_repeat_time",
    )

    def __init__(self, enabled: bool, threshold: int, cooldown: float):
        self.enabled = enabled
        self.threshold = threshold
        self.cooldown = cooldown
        self.lock = asyncio.Lock()
        self.current_fingerprint: str | None = None  # 当前正在统计的消息指纹
        self.senders: set[str] = set()  # 已发送该消息的不同人（去重）
        self.repeated_in_segment = False  # 当前连续消息段是否已经成功复读
        self.last_repeat_time: float | None = None  # 上次成功复读的单调时钟


class RepeatPlugin(Star):
    """群复读机插件"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._groups: dict[str, _GroupState] = {}
        self._state_file = self._data_dir() / "state.json"
        self._load_state()
        logger.info("复读机插件已加载，群状态数据文件: %s", self._state_file)

    # ------------------------------------------------------------------
    # 核心逻辑（纯同步函数，便于本地测试）
    # ------------------------------------------------------------------
    def process_message(
        self, group_id: str, sender_id: str, fingerprint: str, now: float
    ) -> str | None:
        """处理一条群消息指纹，达到复读条件时返回该指纹。

        - 只统计【连续】发送的相同消息，内容一变就重新计数；
        - 同一个人的多次重复只算 1 人；
        - 同一连续消息段成功复读一次后不再重复触发；
        - 支持冷却时间（seconds）。

        调用方须持有该群的 ``state.lock``；返回指纹仅代表一次发送候选，
        发送成功后再由 ``_commit_repeat`` 提交复读状态。
        """
        state = self._get_group(group_id)
        fingerprint = (fingerprint or "").strip()
        if not state.enabled or not fingerprint:
            return None
        if fingerprint != state.current_fingerprint:
            # 有效消息发生变化：开始新的连续消息段。
            state.current_fingerprint = fingerprint
            state.senders.clear()
            state.repeated_in_segment = False
        if (
            state.last_repeat_time is not None
            and state.cooldown > 0
            and now - state.last_repeat_time < state.cooldown
        ):
            # 冷却期不累计发送者；消息切段已经在上方完成。
            return None
        if state.repeated_in_segment:
            return None
        state.senders.add(sender_id)
        if len(state.senders) < state.threshold:
            return None
        return fingerprint

    def _extract_candidate(self, event: AstrMessageEvent):
        """Build a bounded ordered chain; raw OneBot order is authoritative."""
        raw_types, parsed_types = self._message_type_summaries(event)
        raw = self._value_of(
            getattr(event, "message_obj", None), "raw_message"
        )
        raw_segments = self._value_of(raw, "message")
        if isinstance(raw_segments, (list, tuple)) and raw_segments:
            return self._raw_chain_candidate(
                event, raw_segments, raw_types, parsed_types
            )
        return self._parsed_chain_candidate(event, raw_types, parsed_types)

    def _parsed_chain_candidate(
        self, event: AstrMessageEvent, raw_types: str, parsed_types: str
    ):
        components = list(event.get_messages() or [])
        parts = []
        media = []
        self_id = self._clean_value(
            self._value_of(getattr(event, "message_obj", None), "self_id")
        )
        for component in components:
            name = component.__class__.__name__.lower()
            if isinstance(component, Plain):
                raw_text = getattr(component, "text", None)
                value = "" if raw_text is None else str(raw_text)
                if value.strip():
                    parts.append(_ChainPart("text", value))
                continue
            if isinstance(component, Face):
                value = self._clean_value(getattr(component, "id", None))
                if value:
                    parts.append(_ChainPart("face", value))
                    continue
            elif isinstance(component, Image):
                media_index = len(media)
                media.append(
                    _RepeatCandidate(
                        kind="image", source="parsed", component=component
                    )
                )
                parts.append(_ChainPart("image", media_index=media_index))
                continue
            elif isinstance(component, At):
                value = self._clean_value(getattr(component, "qq", None))
                if value and value == self_id:
                    continue
                if value:
                    parts.append(_ChainPart("at", value))
                    continue
            elif name == "reply":
                continue
            return self._skip_extraction(
                event,
                "parsed",
                "unsupported-segment",
                raw_types,
                parsed_types,
            )
        return self._candidate_from_parts(
            event, "parsed", parts, media, raw_types, parsed_types
        )

    @classmethod
    def _message_type_summaries(cls, event: AstrMessageEvent):
        allowed = {"image", "mface", "face", "text", "reply", "at"}
        raw = cls._value_of(getattr(event, "message_obj", None), "raw_message")
        raw_segments = cls._value_of(raw, "message")
        raw_types = []
        if isinstance(raw_segments, (list, tuple)):
            for segment in raw_segments:
                value = cls._clean_value(
                    cls._value_of(segment, "type")
                ).lower()
                raw_types.append(value if value in allowed else "other")

        parsed_types = []
        for component in list(event.get_messages() or []):
            if isinstance(component, Plain):
                parsed_types.append("text")
            elif isinstance(component, Face):
                parsed_types.append("face")
            elif isinstance(component, Image):
                parsed_types.append("image")
            else:
                name = component.__class__.__name__.lower()
                parsed_types.append(name if name in allowed else "other")
        return ",".join(raw_types) or "none", ",".join(parsed_types) or "none"

    def _raw_chain_candidate(
        self,
        event: AstrMessageEvent,
        segments,
        raw_types: str,
        parsed_types: str,
    ):
        if len(segments) > _MAX_RAW_SEGMENTS:
            return self._skip_extraction(
                event, "raw", "raw-segment-limit", raw_types, parsed_types
            )
        raw = self._value_of(
            getattr(event, "message_obj", None), "raw_message"
        )
        self_id = self._clean_value(
            self._value_of(getattr(event, "message_obj", None), "self_id")
            or self._value_of(raw, "self_id")
        )
        parts = []
        media = []
        for segment in segments:
            segment_type = self._clean_value(
                self._value_of(segment, "type")
            ).lower()
            data = self._value_of(segment, "data")
            if segment_type == "reply":
                continue
            if segment_type == "text":
                raw_text = self._value_of(data, "text")
                value = "" if raw_text is None else str(raw_text)
                if value.strip():
                    parts.append(_ChainPart("text", value))
                continue
            if segment_type == "face":
                value = self._clean_value(self._value_of(data, "id"))
                if value:
                    parts.append(_ChainPart("face", value))
                    continue
            elif segment_type == "at":
                value = self._clean_value(self._value_of(data, "qq"))
                if value and value == self_id:
                    continue
                if value:
                    parts.append(_ChainPart("at", value))
                    continue
            elif segment_type in ("image", "mface"):
                media_index = len(media)
                media.append(
                    _RepeatCandidate(
                        kind=segment_type,
                        source="raw",
                        data=data,
                        channel="mface" if segment_type == "mface" else "event",
                    )
                )
                parts.append(
                    _ChainPart(segment_type, media_index=media_index)
                )
                continue
            return self._skip_extraction(
                event,
                "raw",
                "unsupported-segment",
                raw_types,
                parsed_types,
            )

        parsed_images = [
            item for item in list(event.get_messages() or [])
            if isinstance(item, Image)
        ]
        self._match_parsed_images(media, parsed_images)
        return self._candidate_from_parts(
            event, "raw", parts, media, raw_types, parsed_types
        )

    def _candidate_from_parts(
        self,
        event: AstrMessageEvent,
        source: str,
        parts,
        media,
        raw_types: str,
        parsed_types: str,
    ):
        reason = self._parts_limit_reason(parts, media)
        if reason is None:
            reason = self._known_image_size_reason(media)
        if reason is not None:
            return self._skip_extraction(
                event, source, reason, raw_types, parsed_types
            )
        if not parts:
            return None
        kind = "chain" if len(parts) > 1 else parts[0].kind
        return _RepeatCandidate(
            kind=kind,
            source=source,
            raw_types=raw_types,
            parsed_types=parsed_types,
            parts=tuple(parts),
            media=tuple(media),
        )

    @staticmethod
    def _parts_limit_reason(parts, media):
        if len(parts) > _MAX_MEANINGFUL_PARTS:
            return "meaningful-segment-limit"
        if len(media) > _MAX_MEDIA_PARTS:
            return "media-limit"
        if sum(len(part.value) for part in parts if part.kind == "text") > _MAX_TEXT_CHARS:
            return "text-limit"
        if sum(part.kind == "at" for part in parts) > _MAX_AT_PARTS:
            return "at-limit"
        return None

    @classmethod
    def _known_image_size_reason(cls, media):
        total = 0
        for item in media:
            if item.kind != "image":
                continue
            size = cls._known_image_size(item)
            if size is None:
                continue
            if size > _MAX_IMAGE_BYTES:
                return "image-size-limit"
            total += size
        return "total-image-size-limit" if total > _MAX_TOTAL_IMAGE_BYTES else None

    @classmethod
    def _known_image_size(cls, media):
        component = media.component
        values = [(getattr(component, "base64_data", None), True)]
        values.extend(
            (value, False)
            for value in (
                *(getattr(component, key, None) for key in ("file", "url", "path")),
                *(cls._value_of(media.data, key) for key in ("file", "url", "path")),
            )
        )
        for value, allow_bare in values:
            if value is None:
                continue
            text = value if isinstance(value, (str, bytes)) else str(value)
            if not text:
                continue
            prefix = text[:64].lstrip().lower()
            base64_prefixes = (
                (b"base64://", b"data:")
                if isinstance(prefix, bytes)
                else ("base64://", "data:")
            )
            if not allow_bare and not prefix.startswith(
                base64_prefixes
            ):
                continue
            try:
                size = cls._estimated_base64_size(
                    text, max_bytes=_MAX_IMAGE_BYTES
                )
            except Exception:
                continue
            if size:
                return size
        return None

    @classmethod
    def _match_parsed_images(cls, media, parsed_images):
        unmatched = set(range(len(parsed_images)))
        raw_images = [item for item in media if item.kind == "image"]
        parsed_values = [
            cls._parsed_image_component_values(component)
            for component in parsed_images
        ]

        for field in ("file_unique", "url", "path", "file"):
            raw_by_value = {}
            for raw_index, item in enumerate(raw_images):
                if item.component is not None:
                    continue
                value = cls._raw_image_values(item)[field]
                if value:
                    raw_by_value.setdefault(value, []).append(raw_index)

            parsed_by_value = {}
            for parsed_index in unmatched:
                value = parsed_values[parsed_index][field]
                if value:
                    parsed_by_value.setdefault(value, []).append(parsed_index)

            unique_pairs = []
            for value, raw_indices in raw_by_value.items():
                parsed_indices = parsed_by_value.get(value, ())
                if len(raw_indices) == 1 and len(parsed_indices) == 1:
                    unique_pairs.append((raw_indices[0], parsed_indices[0]))
            for raw_index, parsed_index in sorted(unique_pairs):
                raw_images[raw_index].component = parsed_images[parsed_index]
                unmatched.remove(parsed_index)

        remaining_media = [
            item for item in raw_images if item.component is None
        ]
        if len(remaining_media) == len(unmatched):
            for item, parsed_index in zip(remaining_media, sorted(unmatched)):
                item.component = parsed_images[parsed_index]

    @classmethod
    def _parsed_image_component_values(cls, component):
        return {
            key: cls._clean_value(getattr(component, key, None))
            for key in ("file", "url", "path", "file_unique")
        }

    def _skip_extraction(
        self,
        event: AstrMessageEvent,
        source: str,
        reason: str,
        raw_types: str,
        parsed_types: str,
    ):
        self._debug_skip(
            "chain",
            source,
            reason,
            raw_types,
            parsed_types,
            self._diagnostic_threshold(event),
        )
        return None

    async def _resolve_candidate(
        self, event: AstrMessageEvent, candidate: _RepeatCandidate, now: float
    ):
        total_image_bytes = 0
        for media in candidate.media:
            remaining_image_bytes = _MAX_TOTAL_IMAGE_BYTES - total_image_bytes
            if media.kind == "image" and remaining_image_bytes <= 0:
                candidate.reason = "total-image-size-limit"
                candidate.strategy = "chain-size-check"
                return None
            resolved = await self._resolve_media_candidate(
                event,
                media,
                now,
                remaining_image_bytes=(
                    remaining_image_bytes if media.kind == "image" else None
                ),
            )
            if resolved is None:
                candidate.strategy = media.strategy
                candidate.reason = media.reason
                candidate.error_type = media.error_type
                return None
            if media.kind == "image" and media.content_size is not None:
                total_image_bytes += media.content_size

        if any(media.kind == "mface" for media in candidate.media):
            bot = getattr(event, "bot", None)
            if callable(getattr(bot, "send_group_msg", None)):
                candidate.channel = "send_group_msg"
            elif callable(getattr(bot, "call_action", None)):
                candidate.channel = "call_action"
            else:
                mfaces = [
                    media for media in candidate.media if media.kind == "mface"
                ]
                if not all(self._set_mface_gif_outbound(media) for media in mfaces):
                    candidate.strategy = "missing-mface-channel"
                    candidate.reason = "missing-channel"
                    return None
                candidate.channel = "event-gif"

        identity_parts = self._normalized_chain_parts(candidate)
        serialized = json.dumps(
            [(part.kind, part.value) for part in identity_parts],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        candidate.fingerprint = f"{_CHAIN_FINGERPRINT_VERSION}:{digest}"
        candidate.strategy = _CHAIN_FINGERPRINT_VERSION
        return candidate

    async def _resolve_media_candidate(
        self,
        event: AstrMessageEvent,
        candidate: _RepeatCandidate,
        now: float,
        remaining_image_bytes: int | None = None,
    ):
        if candidate.fingerprint:
            return candidate
        if candidate.kind not in ("image", "mface"):
            return None

        market = self._market_fingerprint(candidate)
        if market is not None:
            candidate.fingerprint, candidate.strategy = market
            if candidate.kind == "mface":
                return candidate
            candidate.outbounds = self._image_outbound_candidates(candidate)
            candidate.outbound = (
                candidate.outbounds[0] if candidate.outbounds else None
            )
            if not candidate.outbounds:
                candidate.reason = "missing-source"
                return None
            return candidate
        if candidate.kind == "mface":
            candidate.strategy = "missing-market-identity"
            candidate.reason = "missing-identity"
            return None

        strong = self._strong_image_identity(candidate)
        if strong is not None:
            candidate.fingerprint, candidate.strategy = strong
            candidate.outbounds = self._image_outbound_candidates(candidate)
            candidate.outbound = (
                candidate.outbounds[0] if candidate.outbounds else None
            )
            if not candidate.outbounds:
                candidate.reason = "missing-source"
                return None
            return candidate

        probe_entries = self._image_probe_candidates(candidate)
        if not probe_entries:
            candidate.strategy = "missing-image-source"
            candidate.reason = "missing-source"
            return None
        saw_probe_error = False
        for index, (label, probe) in enumerate(probe_entries):
            try:
                base64_data = await probe.convert_to_base64()
                estimated_size = self._estimated_base64_size(
                    base64_data,
                    max_bytes=min(
                        _MAX_IMAGE_BYTES,
                        remaining_image_bytes or _MAX_IMAGE_BYTES,
                    ),
                )
                if estimated_size > _MAX_IMAGE_BYTES:
                    candidate.strategy = "content-size-check"
                    candidate.reason = "image-size-limit"
                    return None
                if (
                    remaining_image_bytes is not None
                    and estimated_size > remaining_image_bytes
                ):
                    candidate.strategy = "content-size-check"
                    candidate.reason = "total-image-size-limit"
                    return None
                content_bytes, base64_text = self._decode_base64_content(
                    base64_data
                )
            except Exception as e:
                saw_probe_error = True
                candidate.error_type = type(e).__name__
                continue
            if not content_bytes:
                continue
            digest = hashlib.sha256(content_bytes).hexdigest()
            candidate.fingerprint = f"image:content:{digest}"
            candidate.strategy = f"content-sha256-{label}"
            candidate.content_base64 = base64_text
            candidate.content_size = len(content_bytes)
            candidate.outbound = self._image_from_base64(base64_text)
            candidate.probe = probe
            candidate.probes = tuple(
                item[1] for item in probe_entries[index:]
            )
            return candidate

        fallback = self._fallback_image_identity(candidate)
        if fallback is not None:
            candidate.fingerprint, candidate.strategy = fallback
            candidate.outbounds = self._image_outbound_candidates(candidate)
            candidate.outbound = (
                candidate.outbounds[0] if candidate.outbounds else None
            )
            if candidate.outbounds:
                return candidate
            candidate.reason = "missing-source"
            return None

        candidate.strategy = "probe-exhausted"
        candidate.reason = "probe-failed" if saw_probe_error else "empty-content"
        return None

    @classmethod
    def _market_fingerprint(cls, candidate: _RepeatCandidate):
        package_id = cls._candidate_data_value(
            candidate, "emoji_package_id", "emojiPackageId"
        )
        emoji_id = cls._candidate_data_value(
            candidate, "emoji_id", "emojiId"
        )
        is_market = candidate.kind == "mface" or package_id or emoji_id
        if not is_market:
            return None
        if emoji_id:
            return f"market:emoji:{emoji_id}", "market-emoji-id"
        key = cls._candidate_data_value(candidate, "key")
        if key:
            digest = hashlib.sha256(key.encode()).hexdigest()
            return f"market:key:{digest}", "market-key-sha256"
        return None

    @classmethod
    def _raw_image_values(cls, candidate: _RepeatCandidate):
        return {
            key: cls._clean_value(cls._value_of(candidate.data, key))
            for key in ("file", "url", "path", "file_unique")
        }

    @classmethod
    def _parsed_image_values(cls, candidate: _RepeatCandidate):
        component = candidate.component
        return {
            key: cls._clean_value(getattr(component, key, None))
            for key in ("file", "url", "path", "file_unique")
        }

    @classmethod
    def _strong_image_identity(cls, candidate: _RepeatCandidate):
        raw = cls._raw_image_values(candidate)
        parsed = cls._parsed_image_values(candidate)

        for value in (raw["file_unique"], parsed["file_unique"]):
            if not value:
                continue
            digest = cls._hash_shaped_file_unique(value)
            if digest:
                return f"image:unique:{digest}", "file-unique-hash"
            digest = hashlib.sha256(value.encode()).hexdigest()
            return f"image:unique:{digest}", "file-unique"

        values = parsed if isinstance(candidate.component, Image) else raw
        for field in ("file", "url", "path"):
            value = values[field]
            if not value or cls._requires_content_probe(value):
                continue
            digest = cls._terminal_content_hash(value)
            if digest:
                return f"image:hash:{digest}", "terminal-hash"
            digest = cls._uuid_content_hash(value)
            if digest:
                return f"image:hash:{digest}", "uuid-hash"

        return None

    @classmethod
    def _fallback_image_identity(cls, candidate: _RepeatCandidate):
        parsed_image = isinstance(candidate.component, Image)
        values = (
            cls._parsed_image_values(candidate)
            if parsed_image
            else cls._raw_image_values(candidate)
        )
        scope = "parsed" if parsed_image else "raw"
        for field in ("file", "url", "path"):
            value = values[field]
            if field == "url" or not cls._is_stable_local_identity(value, field):
                continue
            digest = hashlib.sha256(value.encode()).hexdigest()
            return f"image:{scope}-{field}:{digest}", f"fallback-{scope}-{field}"
        return None

    @classmethod
    def _requires_content_probe(cls, value: str) -> bool:
        lowered = cls._clean_value(value).lower()
        return lowered.startswith(
            ("http://", "https://", "base64://", "data:")
        )

    @classmethod
    def _is_stable_local_identity(cls, value: str, field: str) -> bool:
        text = cls._clean_value(value)
        lowered = text.lower()
        if not text or len(text) > 260:
            return False
        if re.match(r"^[a-z][a-z0-9+.-]*://", lowered):
            return False
        if lowered.startswith(("base64://", "data:")) or any(
            marker in lowered
            for marker in ("token", "opaque", "temporary", "rkey", "fileid")
        ):
            return False
        compact = re.sub(r"[^a-zA-Z0-9]", "", text)
        if len(compact) >= 48 and len(compact) / max(len(text), 1) > 0.85:
            return False
        if field == "path":
            return True
        return bool(
            re.search(r"[\\/]", text)
            or re.search(r"\.[a-zA-Z0-9]{1,10}$", text)
        )

    @classmethod
    def _image_source_entries(cls, candidate: _RepeatCandidate):
        parsed = cls._parsed_image_values(candidate)
        raw = cls._raw_image_values(candidate)
        entries = []
        seen = set()
        for field in ("url", "path", "file"):
            for scope, values in (("parsed", parsed), ("raw", raw)):
                source = values[field]
                if not source or source in seen:
                    continue
                seen.add(source)
                entries.append((f"{scope}-{field}", field, source))
        return entries

    @classmethod
    def _image_probe_candidates(cls, candidate: _RepeatCandidate):
        entries = []
        seen_sources = set()
        parsed = cls._parsed_image_values(candidate)
        raw = cls._raw_image_values(candidate)
        if isinstance(candidate.component, Image):
            component_source = parsed["url"] or parsed["file"]
            if component_source:
                entries.append(("parsed-component", candidate.component))
                seen_sources.add(component_source)
        for scope, values, fields in (
            ("parsed", parsed, ("file", "url", "path")),
            ("raw", raw, ("url", "path", "file")),
        ):
            for field in fields:
                if scope == "parsed" and field == "file" and parsed["url"]:
                    # AstrBot converts an existing parsed Image via ``url or
                    # file``.  Rebuilding its shadowed file as a fresh Image
                    # would invent a transport path the adapter did not use.
                    continue
                source = values[field]
                if not source or source in seen_sources:
                    continue
                seen_sources.add(source)
                probe = cls._image_from_source(field, source)
                if probe is not None:
                    label = f"{scope}-{field}"
                    entries.append((label, probe))
        return entries

    @classmethod
    def _image_outbound_candidates(cls, candidate: _RepeatCandidate):
        candidates = []
        for _, field, source in cls._image_source_entries(candidate):
            outbound = cls._image_from_source(field, source)
            if outbound is not None:
                candidates.append(outbound)
        return tuple(candidates)

    @classmethod
    def _normalized_output_parts(cls, candidate: _RepeatCandidate):
        normalized = []
        text_run = []

        def flush_text():
            if not text_run:
                return
            value = "".join(text_run).strip()
            text_run.clear()
            if value:
                normalized.append(_ChainPart("text", value))

        for part in candidate.parts:
            if part.kind == "text":
                text_run.append(part.value)
                continue
            flush_text()
            normalized.append(part)
        flush_text()
        return tuple(normalized)

    @classmethod
    def _normalized_chain_parts(cls, candidate: _RepeatCandidate):
        identity = []
        for part in cls._normalized_output_parts(candidate):
            if part.kind in ("image", "mface"):
                media = candidate.media[part.media_index]
                kind = (
                    "market"
                    if media.strategy.startswith("market-")
                    else part.kind
                )
                identity.append(_ChainPart(kind, media.fingerprint or ""))
            else:
                identity.append(_ChainPart(part.kind, part.value))
        return tuple(identity)

    @classmethod
    def _set_mface_gif_outbound(cls, media: _RepeatCandidate) -> bool:
        gif_url = cls._candidate_data_value(
            media, "url", "gif_url", "gifUrl"
        )
        if not cls._is_gif_url(gif_url):
            return False
        media.outbound = cls._image_from_source("url", gif_url)
        return media.outbound is not None

    @classmethod
    def _candidate_data_value(cls, candidate: _RepeatCandidate, *keys):
        for key in keys:
            value = cls._clean_value(cls._value_of(candidate.data, key))
            if value:
                return value
        for key in keys:
            value = cls._clean_value(getattr(candidate.component, key, None))
            if value:
                return value
        return ""

    @staticmethod
    def _clean_value(value) -> str:
        return "" if value is None else str(value).strip()

    @staticmethod
    def _terminal_content_hash(value) -> str | None:
        text = RepeatPlugin._clean_value(value)
        match = _TERMINAL_CONTENT_HASH.search(text)
        return match.group(1).lower() if match else None

    @staticmethod
    def _uuid_content_hash(value) -> str | None:
        text = RepeatPlugin._clean_value(value)
        match = _UUID_CONTENT_HASH.search(text)
        return "".join(match.groups()).lower() if match else None

    @classmethod
    def _hash_shaped_file_unique(cls, value) -> str | None:
        return cls._terminal_content_hash(value) or cls._uuid_content_hash(value)

    @staticmethod
    def _normalize_image_value(value) -> str:
        text = RepeatPlugin._clean_value(value)
        if not text:
            return ""
        if text.lower().startswith(("http://", "https://")):
            parts = urlsplit(text)
            query_items = [
                (key, item)
                for key, item in parse_qsl(
                    parts.query, keep_blank_values=True
                )
                if key.lower() != "rkey"
            ]
            query = urlencode(sorted(query_items), doseq=True)
            return urlunsplit(
                (
                    parts.scheme.lower(),
                    parts.netloc.lower(),
                    parts.path,
                    query,
                    parts.fragment,
                )
            )
        return text

    @staticmethod
    def _has_stable_fileid(normalized_url: str) -> bool:
        try:
            query = parse_qsl(
                urlsplit(normalized_url).query, keep_blank_values=True
            )
        except ValueError:
            return False
        return any(
            key.lower() in ("fileid", "file_id") and bool(value.strip())
            for key, value in query
        )

    @staticmethod
    def _base64_payload(value) -> str:
        if isinstance(value, bytes):
            text = value.decode("ascii")
        else:
            text = str(value or "")
        text = re.sub(r"\s+", "", text)
        lowered = text.lower()
        if lowered.startswith("data:"):
            header, separator, text = text.partition(",")
            if not separator or ";base64" not in header.lower():
                raise ValueError("invalid data URI")
        elif lowered.startswith("base64://"):
            text = text[len("base64://") :]
        return re.sub(r"\s+", "", text)

    @classmethod
    def _estimated_base64_size(
        cls, value, max_bytes: int | None = None
    ) -> int:
        if isinstance(value, bytes):
            raw = value
        elif isinstance(value, str):
            raw = value
        else:
            raw = str(value or "")
        if max_bytes is not None:
            encoded_limit = 4 * ((max_bytes + 2) // 3)
            if len(raw) > encoded_limit + _MAX_BASE64_OVERHEAD:
                return max_bytes + 1
        text = cls._base64_payload(raw)
        if not text:
            return 0
        if len(text) % 4 == 1:
            raise ValueError("invalid Base64 length")
        padded = text + "=" * (-len(text) % 4)
        padding = len(padded) - len(padded.rstrip("="))
        if padding > 2 or "=" in padded[:-padding or None]:
            raise ValueError("invalid Base64 padding")
        return len(padded) // 4 * 3 - padding

    @classmethod
    def _decode_base64_content(cls, value):
        text = cls._base64_payload(value)
        if not text:
            return b"", ""
        padded = text + "=" * (-len(text) % 4)
        decoded = base64.b64decode(padded, validate=True)
        canonical = base64.b64encode(decoded).decode("ascii")
        return decoded, canonical

    @classmethod
    def _image_from_candidate(cls, candidate: _RepeatCandidate):
        candidates = cls._image_outbound_candidates(candidate)
        return candidates[0] if candidates else None

    @staticmethod
    def _image_from_source(field: str, source: str):
        if not source:
            return None
        if source.lower().startswith(("http://", "https://")):
            from_url = getattr(Image, "fromURL", None)
            return from_url(source) if callable(from_url) else Image(file=source)
        is_local = field == "path" or source.startswith(("/", "\\"))
        is_local = is_local or bool(re.match(r"^[a-zA-Z]:[\\/]", source))
        if is_local:
            from_file = getattr(Image, "fromFileSystem", None)
            if callable(from_file):
                return from_file(source)
            return Image(file=source, path=source)
        return Image(file=source)

    @staticmethod
    def _image_from_base64(base64_text: str):
        from_base64 = getattr(Image, "fromBase64", None)
        if callable(from_base64):
            return from_base64(base64_text)
        return Image(file=f"base64://{base64_text}")

    @classmethod
    def _resolve_mface_channel(
        cls, event: AstrMessageEvent, candidate: _RepeatCandidate
    ):
        bot = getattr(event, "bot", None)
        if callable(getattr(bot, "send_group_msg", None)):
            candidate.channel = "send_group_msg"
            return candidate
        if callable(getattr(bot, "call_action", None)):
            candidate.channel = "call_action"
            return candidate
        gif_url = cls._candidate_data_value(
            candidate, "url", "gif_url", "gifUrl"
        )
        if cls._is_gif_url(gif_url):
            candidate.channel = "event-gif"
            candidate.outbound = cls._image_from_source("url", gif_url)
            return candidate
        candidate.strategy = "missing-mface-channel"
        candidate.reason = "missing-channel"
        return None

    @staticmethod
    def _is_gif_url(value: str) -> bool:
        if not value.lower().startswith(("http://", "https://")):
            return False
        return urlsplit(value).path.lower().endswith(".gif")

    async def _send_candidate(
        self,
        event: AstrMessageEvent,
        candidate: _RepeatCandidate,
        group_id: str,
        self_id: str,
    ) -> None:
        materialized = {}
        total_image_bytes = 0
        for index, media in enumerate(candidate.media):
            if media.kind != "image":
                continue
            remaining_image_bytes = _MAX_TOTAL_IMAGE_BYTES - total_image_bytes
            if remaining_image_bytes <= 0:
                raise ValueError("total-image-size-limit")
            outbound, size = await self._materialize_image(
                media, remaining_image_bytes
            )
            total_image_bytes += size
            materialized[index] = outbound

        if candidate.channel in ("send_group_msg", "call_action"):
            payload = {
                "group_id": group_id,
                "message": self._onebot_message(candidate, materialized),
            }
            if self_id:
                payload["self_id"] = self_id
            bot = getattr(event, "bot", None)
            if candidate.channel == "send_group_msg":
                await bot.send_group_msg(**payload)
            else:
                await bot.call_action("send_group_msg", **payload)
            return

        chain = self._event_message_chain(candidate, materialized)
        await event.send(chain)

    async def _materialize_image(
        self, media: _RepeatCandidate, remaining_image_bytes: int
    ):
        if media.content_base64:
            size = media.content_size
            if size is None:
                size = self._estimated_base64_size(
                    media.content_base64,
                    max_bytes=min(_MAX_IMAGE_BYTES, remaining_image_bytes),
                )
            self._check_image_size(size, remaining_image_bytes)
            return self._image_from_base64(media.content_base64), size

        last_error = None
        for _, probe in self._image_probe_candidates(media):
            try:
                base64_data = await probe.convert_to_base64()
                size = self._estimated_base64_size(
                    base64_data,
                    max_bytes=min(_MAX_IMAGE_BYTES, remaining_image_bytes),
                )
                self._check_image_size(size, remaining_image_bytes)
                content, canonical = self._decode_base64_content(base64_data)
            except _MediaLimitError:
                raise
            except Exception as e:
                last_error = e
                continue
            if content:
                return self._image_from_base64(canonical), len(content)
        if last_error is not None:
            raise last_error
        raise ValueError("empty image content")

    @staticmethod
    def _check_image_size(size: int, remaining_image_bytes: int) -> None:
        if size > _MAX_IMAGE_BYTES:
            raise _MediaLimitError("image-size-limit")
        if size > remaining_image_bytes:
            raise _MediaLimitError("total-image-size-limit")

    @classmethod
    def _event_message_chain(cls, candidate, materialized):
        chain = []
        for part in cls._normalized_output_parts(candidate):
            if part.kind == "text":
                chain.append(Plain(part.value))
            elif part.kind == "face":
                chain.append(Face(id=int(part.value)))
            elif part.kind == "at":
                chain.append(At(qq=part.value))
            elif part.kind == "image":
                chain.append(materialized[part.media_index])
            elif part.kind == "mface":
                chain.append(candidate.media[part.media_index].outbound)
        return MessageChain(chain)

    @classmethod
    def _onebot_message(cls, candidate, materialized):
        message = []
        for part in cls._normalized_output_parts(candidate):
            if part.kind == "text":
                data = {"text": part.value}
            elif part.kind == "face":
                data = {"id": part.value}
            elif part.kind == "at":
                data = {"qq": part.value}
            elif part.kind == "image":
                image = materialized[part.media_index]
                data = {"file": cls._clean_value(getattr(image, "file", None))}
            else:
                data = cls._mface_data(candidate.media[part.media_index])
            message.append({"type": part.kind, "data": data})
        return message

    @classmethod
    def _mface_data(cls, media):
        data = {}
        aliases = {
            "emoji_package_id": ("emoji_package_id", "emojiPackageId"),
            "emoji_id": ("emoji_id", "emojiId"),
            "key": ("key",),
            "summary": ("summary",),
        }
        for output_key, keys in aliases.items():
            value = cls._candidate_data_value(media, *keys)
            if value:
                data[output_key] = value
        return data

    @classmethod
    def _mface_payload(
        cls, candidate: _RepeatCandidate, group_id: str, self_id: str
    ):
        payload = {
            "group_id": group_id,
            "message": [
                {"type": "mface", "data": cls._mface_data(candidate)}
            ],
        }
        if self_id:
            payload["self_id"] = self_id
        return payload

    def _media_log(self, template: str, *args) -> None:
        enabled = self.config.get("media_diagnostics", False)
        if isinstance(enabled, str):
            enabled = enabled.strip().lower() in ("1", "true", "yes", "on")
        log = logger.info if enabled else logger.debug
        log(template, *args)

    def _debug_media(
        self,
        candidate: _RepeatCandidate,
        state: _GroupState,
        error_type: str | None = None,
        result: str = "observed",
    ) -> None:
        if not candidate.media and candidate.kind not in ("image", "mface"):
            return
        fingerprint = candidate.fingerprint or ""
        short_fingerprint = (
            hashlib.sha256(fingerprint.encode()).hexdigest()[:12]
            if fingerprint
            else "-"
        )
        self._media_log(
            "[repeater.media] type=%s source=%s raw_types=%s parsed_types=%s "
            "strategy=%s fp=%s "
            "count=%d threshold=%d result=%s channel=%s reason=%s error=%s",
            candidate.kind,
            candidate.source,
            candidate.raw_types,
            candidate.parsed_types,
            candidate.strategy,
            short_fingerprint,
            len(state.senders),
            state.threshold,
            result,
            candidate.channel,
            candidate.reason,
            error_type or candidate.error_type,
        )

    def _debug_skip(
        self,
        kind: str,
        source: str,
        reason: str,
        raw_types: str,
        parsed_types: str,
        threshold: int,
    ) -> None:
        self._media_log(
            "[repeater.media] type=%s source=%s raw_types=%s parsed_types=%s "
            "strategy=skip fp=- "
            "count=0 threshold=%d result=skipped channel=- reason=%s error=-",
            kind,
            source,
            raw_types,
            parsed_types,
            threshold,
            reason,
        )

    def _diagnostic_threshold(self, event: AstrMessageEvent) -> int:
        message_obj = getattr(event, "message_obj", None)
        group_id = self._clean_value(getattr(message_obj, "group_id", None))
        state = self._groups.get(group_id) if group_id else None
        return state.threshold if state is not None else self._default_threshold()

    @staticmethod
    def _commit_repeat(state: _GroupState, now: float) -> None:
        state.repeated_in_segment = True
        state.senders.clear()
        state.last_repeat_time = now

    # ------------------------------------------------------------------
    # 事件监听：群消息
    # ------------------------------------------------------------------
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def on_group_message(self, event: AstrMessageEvent):
        """检测群内连续相同消息并复读。

        复读消息使用 event.send() 直接发送，绕过 AstrBot 的「结果装饰
        阶段」：避免在全局开启「回复时引用发送人消息 / 回复时 @ 用户」
        时，机器人复读还会带上对最后一位发送者的引用或 @，破坏复读效果。
        """
        msg = event.message_obj
        group_id = str(getattr(msg, "group_id", "") or "")
        if not group_id:
            return
        # 忽略机器人自己发出的消息（防止自我复读死循环）
        sender_id = str(getattr(msg.sender, "user_id", "") or "")
        if not sender_id or sender_id == str(getattr(msg, "self_id", "") or ""):
            return
        text = event.message_str or ""
        # 忽略复读机指令本身
        if text.strip() and _CMD_PATTERN.match(text.strip()):
            return
        candidate = self._extract_candidate(event)
        if candidate is None:
            return
        state = self._get_group(group_id)
        async with state.lock:
            unresolved = candidate
            try:
                resolved = await self._resolve_candidate(
                    event, candidate, time.monotonic()
                )
            except Exception as e:
                unresolved.strategy = "resolve-error"
                self._debug_media(
                    unresolved, state, type(e).__name__, "resolve-error"
                )
                logger.warning(
                    "解析复读媒体失败（群 %s，%s）",
                    group_id,
                    type(e).__name__,
                )
                return
            if resolved is None:
                self._debug_media(unresolved, state, result="skipped")
                return
            candidate = resolved
            repeat_fingerprint = self.process_message(
                group_id,
                sender_id,
                candidate.fingerprint,
                time.monotonic(),
            )
            self._debug_media(candidate, state, result="evaluated")
            if repeat_fingerprint is None:
                return
            try:
                await self._send_candidate(
                    event,
                    candidate,
                    group_id,
                    str(getattr(msg, "self_id", "") or ""),
                )
            except Exception as e:
                self._debug_media(
                    candidate, state, type(e).__name__, "send-error"
                )
                logger.warning(
                    "发送复读消息失败（群 %s，%s）",
                    group_id,
                    type(e).__name__,
                )
                return
            self._commit_repeat(state, time.monotonic())
            self._debug_media(candidate, state, result="committed")

    # ------------------------------------------------------------------
    # 指令组：群内控制
    # ------------------------------------------------------------------
    @repeat_group.command("开")
    async def repeat_on(self, event: AstrMessageEvent):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        permission_error = self._command_permission_error(event)
        if permission_error:
            yield event.plain_result(permission_error)
            return
        state = self._get_group(gid)
        async with state.lock:
            state.enabled = True
            threshold = state.threshold
            self._save_state()
        yield event.plain_result(
            f"✅ 复读机模式已开启！群内连续 {threshold} 个不同的人"
            "发送相同消息时，我会自动跟一条。"
        )

    @repeat_group.command("关")
    async def repeat_off(self, event: AstrMessageEvent):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        permission_error = self._command_permission_error(event)
        if permission_error:
            yield event.plain_result(permission_error)
            return
        state = self._get_group(gid)
        async with state.lock:
            state.enabled = False
            self._save_state()
        yield event.plain_result("🛑 复读机模式已关闭")

    @repeat_group.command("阈值")
    async def repeat_threshold(self, event: AstrMessageEvent, num: int = None):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        permission_error = self._command_permission_error(event)
        if permission_error:
            yield event.plain_result(permission_error)
            return
        if num is None or not (2 <= num <= 50):
            yield event.plain_result("用法：/复读机 阈值 <人数>（2~50 的整数）")
            return
        state = self._get_group(gid)
        async with state.lock:
            state.threshold = num
            self._save_state()
        yield event.plain_result(
            f"🔢 已设置：连续 {num} 个不同的人发送相同消息后，我会跟一条"
        )

    @repeat_group.command("冷却")
    async def repeat_cooldown(self, event: AstrMessageEvent, num: int = None):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        permission_error = self._command_permission_error(event)
        if permission_error:
            yield event.plain_result(permission_error)
            return
        if num is None or not (0 <= num <= 3600):
            yield event.plain_result("用法：/复读机 冷却 <秒数>（0~3600，0 表示无冷却）")
            return
        state = self._get_group(gid)
        async with state.lock:
            state.cooldown = float(num)
            self._save_state()
        yield event.plain_result(
            f"⏱️ 已设置：触发复读后冷却 {num} 秒（0 表示无冷却）"
        )

    @repeat_group.command("状态")
    async def repeat_status(self, event: AstrMessageEvent):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        permission_error = self._command_permission_error(event)
        if permission_error:
            yield event.plain_result(permission_error)
            return
        state = self._get_group(gid)
        async with state.lock:
            status = "已开启" if state.enabled else "已关闭"
            threshold = state.threshold
            cooldown = int(state.cooldown)
        yield event.plain_result(
            "📋 复读机状态\n"
            f"状态：{status}\n"
            f"触发人数：{threshold} 人\n"
            f"冷却时间：{cooldown} 秒\n"
            "——\n"
            "指令：/复读机 开 | 关 | 阈值 N | 冷却 N | 状态"
        )

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------
    def _event_group_id(self, event: AstrMessageEvent) -> str:
        return str(getattr(event.message_obj, "group_id", "") or "")

    def _read_command_permission(self) -> str:
        permission = str(self.config.get("command_permission", "bot_owner"))
        if permission in _PERMISSION_RANK:
            return permission
        logger.warning(
            "无效的 command_permission 配置 %r，按 bot_owner 处理",
            permission,
        )
        return "bot_owner"

    def _command_permission_error(self, event: AstrMessageEvent) -> str | None:
        required = self._read_command_permission()

        is_admin = getattr(event, "is_admin", None)
        try:
            bot_owner = bool(is_admin() if callable(is_admin) else is_admin)
        except Exception as e:
            logger.warning("检查 BOT 主人权限失败: %s", e)
            bot_owner = False

        if bot_owner:
            actual = "bot_owner"
        else:
            raw_message = self._value_of(event.message_obj, "raw_message")
            sender = self._value_of(raw_message, "sender")
            role = self._value_of(sender, "role")
            actual = role if role in ("admin", "owner") else "member"

        if _PERMISSION_RANK[actual] >= _PERMISSION_RANK[required]:
            return None
        return f"权限不足：该指令需要{_PERMISSION_LABEL[required]}权限"

    @staticmethod
    def _value_of(value, key: str):
        if isinstance(value, Mapping):
            return value.get(key)
        return getattr(value, key, None)

    def _get_group(self, group_id: str) -> _GroupState:
        gid = str(group_id)
        state = self._groups.get(gid)
        if state is None:
            state = _GroupState(
                enabled=self._default_enabled(),
                threshold=self._default_threshold(),
                cooldown=self._default_cooldown(),
            )
            self._groups[gid] = state
            self._save_state()
        return state

    def _default_enabled(self) -> bool:
        return bool(self.config.get("enable", True))

    def _default_threshold(self) -> int:
        try:
            v = int(self.config.get("threshold", 3))
        except (TypeError, ValueError):
            v = 3
        return max(2, min(50, v))

    def _default_cooldown(self) -> float:
        try:
            v = float(self.config.get("cooldown", 0))
        except (TypeError, ValueError):
            v = 0.0
        return max(0.0, v)

    def _data_dir(self) -> Path:
        """返回 AstrBot 的 data 目录下的专属数据目录。

        插件位于 data/plugins/<插件名>/main.py，因此 data 目录是
        __file__ 向上三级。持久化数据不放进插件自身目录，防止插件
        更新/重装时数据被覆盖。
        """
        p = Path(os.path.abspath(__file__))
        candidates = [p.parents[2], Path(os.getcwd()) / "data"]
        for base in candidates:
            try:
                target = base / "astrbot_plugin_repeater"
                target.mkdir(parents=True, exist_ok=True)
                return target
            except Exception:
                continue
        return p.parent  # 最终兜底：插件自身目录

    def _load_state(self):
        try:
            if not self._state_file.exists():
                return
            raw = json.loads(self._state_file.read_text(encoding="utf-8"))
            for gid, item in (raw.get("groups") or {}).items():
                state = _GroupState(
                    enabled=bool(item.get("enabled", self._default_enabled())),
                    threshold=max(
                        2,
                        min(
                            50,
                            int(item.get("threshold", self._default_threshold())),
                        ),
                    ),
                    cooldown=max(
                        0.0,
                        float(item.get("cooldown", self._default_cooldown())),
                    ),
                )
                self._groups[str(gid)] = state
        except Exception as e:
            logger.warning("读取复读机群状态失败: %s", e)

    def _save_state(self):
        try:
            data = {
                "groups": {
                    gid: {
                        "enabled": s.enabled,
                        "threshold": s.threshold,
                        "cooldown": s.cooldown,
                    }
                    for gid, s in self._groups.items()
                }
            }
            self._state_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            logger.warning("保存复读机群状态失败: %s", e)

    async def terminate(self):
        """插件被卸载/停用时保存状态。"""
        self._save_state()
