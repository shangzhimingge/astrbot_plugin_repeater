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
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Face, Image, Plain
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

_MEDIA_CACHE_LIMIT = 128
_MEDIA_CACHE_TTL = 300.0


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
        self._media_digest_cache: OrderedDict[
            str, tuple[float, str]
        ] = OrderedDict()
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
        """Raw OneBot 媒体优先；没有 raw 媒体时才回退解析后组件。"""
        raw_types, parsed_types = self._message_type_summaries(event)
        found_raw_media, candidate = self._raw_media_candidate(
            event, raw_types, parsed_types
        )
        if found_raw_media:
            if candidate is not None:
                candidate.raw_types = raw_types
                candidate.parsed_types = parsed_types
            return candidate

        components = list(event.get_messages() or [])
        if len(components) != 1:
            return None
        component = components[0]
        if isinstance(component, Plain):
            text = self._clean_value(getattr(component, "text", None))
            if not text:
                return None
            return _RepeatCandidate(
                kind="text",
                source="parsed",
                component=component,
                fingerprint=f"text:{text}",
                strategy="plain",
                outbound=component,
                raw_types=raw_types,
                parsed_types=parsed_types,
            )
        if isinstance(component, Face):
            face_id = self._clean_value(getattr(component, "id", None))
            if not face_id:
                return None
            return _RepeatCandidate(
                kind="face",
                source="parsed",
                component=component,
                fingerprint=f"face:{face_id}",
                strategy="face-id",
                outbound=component,
                raw_types=raw_types,
                parsed_types=parsed_types,
            )
        if isinstance(component, Image):
            return _RepeatCandidate(
                kind="image",
                source="parsed",
                component=component,
                raw_types=raw_types,
                parsed_types=parsed_types,
            )
        return None

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

    def _raw_media_candidate(
        self,
        event: AstrMessageEvent,
        raw_types: str,
        parsed_types: str,
    ):
        raw = self._value_of(
            getattr(event, "message_obj", None), "raw_message"
        )
        segments = self._value_of(raw, "message")
        if not isinstance(segments, (list, tuple)):
            return False, None

        media = []
        for segment in segments:
            segment_type = self._clean_value(
                self._value_of(segment, "type")
            ).lower()
            if segment_type in ("image", "mface"):
                media.append((segment_type, segment))
        if not media:
            return False, None
        diagnostic_threshold = self._diagnostic_threshold(event)
        if len(media) != 1:
            self._debug_skip(
                "multiple",
                "raw",
                "multiple-media",
                raw_types,
                parsed_types,
                diagnostic_threshold,
            )
            return True, None

        kind, target_segment = media[0]
        self_id = self._clean_value(
            self._value_of(getattr(event, "message_obj", None), "self_id")
            or self._value_of(raw, "self_id")
        )
        chain = []
        for segment in segments:
            segment_type = self._clean_value(
                self._value_of(segment, "type")
            ).lower()
            data = self._value_of(segment, "data")
            if segment is target_segment:
                chain.append(("image", ""))
                continue
            if segment_type == "reply":
                continue
            if segment_type == "text" and not self._clean_value(
                self._value_of(data, "text")
            ):
                continue
            if segment_type == "text":
                self._debug_skip(
                    kind,
                    "raw",
                    "nonblank-text",
                    raw_types,
                    parsed_types,
                    diagnostic_threshold,
                )
                return True, None
            if segment_type == "at" and self_id:
                qq = self._clean_value(self._value_of(data, "qq"))
                if qq == self_id:
                    continue
            if segment_type == "at":
                self._debug_skip(
                    kind,
                    "raw",
                    "other-at",
                    raw_types,
                    parsed_types,
                    diagnostic_threshold,
                )
                return True, None
            if segment_type == "face" and kind == "image":
                face_id = self._clean_value(self._value_of(data, "id"))
                if face_id:
                    chain.append(("face", face_id))
                    continue
            self._debug_skip(
                kind,
                "raw",
                "unsupported-helper",
                raw_types,
                parsed_types,
                diagnostic_threshold,
            )
            return True, None

        data = self._value_of(target_segment, "data")
        parsed_image = next(
            (
                item
                for item in list(event.get_messages() or [])
                if isinstance(item, Image)
            ),
            None,
        )
        return True, _RepeatCandidate(
            kind=kind,
            source="raw",
            component=parsed_image,
            data=data,
            channel="mface" if kind == "mface" else "event",
            chain=(
                tuple(chain)
                if kind == "image" and any(item[0] == "face" for item in chain)
                else ()
            ),
        )

    async def _resolve_candidate(
        self, event: AstrMessageEvent, candidate: _RepeatCandidate, now: float
    ):
        if candidate.fingerprint:
            return candidate
        if candidate.kind not in ("image", "mface"):
            return None

        market = self._market_fingerprint(candidate)
        if market is not None:
            candidate.fingerprint, candidate.strategy = market
            if candidate.kind == "mface":
                return self._resolve_mface_channel(event, candidate)
            candidate.outbounds = self._image_outbound_candidates(candidate)
            candidate.outbound = (
                candidate.outbounds[0] if candidate.outbounds else None
            )
            if not candidate.outbounds:
                candidate.reason = "missing-source"
                return None
            return self._apply_image_chain_fingerprint(candidate)
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
            return self._apply_image_chain_fingerprint(candidate)

        probe_entries = self._image_probe_candidates(candidate)
        if not probe_entries:
            candidate.strategy = "missing-image-source"
            candidate.reason = "missing-source"
            return None
        cache = self._media_cache()
        saw_probe_error = False
        for index, (label, probe, cache_material) in enumerate(probe_entries):
            cache_key = hashlib.sha256(cache_material.encode()).hexdigest()
            cached = cache.get(cache_key)
            if cached is not None:
                cached_at, digest = cached
                if now - cached_at <= _MEDIA_CACHE_TTL:
                    cache.move_to_end(cache_key)
                    candidate.fingerprint = f"image:content:{digest}"
                    candidate.strategy = f"content-cache-{label}"
                    candidate.probe = probe
                    candidate.probes = tuple(
                        item[1] for item in probe_entries[index:]
                    )
                    return self._apply_image_chain_fingerprint(candidate)
                cache.pop(cache_key, None)
            try:
                base64_data = await probe.convert_to_base64()
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
            cache[cache_key] = (now, digest)
            cache.move_to_end(cache_key)
            while len(cache) > _MEDIA_CACHE_LIMIT:
                cache.popitem(last=False)
            candidate.fingerprint = f"image:content:{digest}"
            candidate.strategy = f"content-sha256-{label}"
            candidate.content_base64 = base64_text
            candidate.outbound = self._image_from_base64(base64_text)
            candidate.probe = probe
            candidate.probes = tuple(
                item[1] for item in probe_entries[index:]
            )
            return self._apply_image_chain_fingerprint(candidate)
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

        if isinstance(candidate.component, Image):
            for field in ("file", "url", "path"):
                value = parsed[field]
                if field == "url" or not value:
                    continue
                digest = cls._terminal_content_hash(value)
                if digest:
                    return f"image:hash:{digest}", "terminal-hash"
                digest = cls._uuid_content_hash(value)
                if digest:
                    return f"image:hash:{digest}", "uuid-hash"
                if cls._is_stable_local_identity(value, field):
                    digest = hashlib.sha256(value.encode()).hexdigest()
                    return f"image:parsed-{field}:{digest}", f"parsed-{field}"

        for value in (raw["file_unique"], parsed["file_unique"]):
            if not value:
                continue
            digest = cls._hash_shaped_file_unique(value)
            if digest:
                return f"image:unique:{digest}", "file-unique-hash"
            digest = hashlib.sha256(value.encode()).hexdigest()
            return f"image:unique:{digest}", "file-unique"

        if not isinstance(candidate.component, Image):
            for field in ("file", "path"):
                value = raw[field]
                if not value:
                    continue
                digest = cls._terminal_content_hash(value)
                if digest:
                    return f"image:hash:{digest}", "terminal-hash"
                digest = cls._uuid_content_hash(value)
                if digest:
                    return f"image:hash:{digest}", "uuid-hash"
                if cls._is_stable_local_identity(value, field):
                    digest = hashlib.sha256(value.encode()).hexdigest()
                    return f"image:raw-{field}:{digest}", f"raw-{field}"

        return None

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
        if isinstance(candidate.component, Image):
            parsed = cls._parsed_image_values(candidate)
            component_source = parsed["url"] or parsed["file"]
            material = f"{candidate.kind}:parsed-component:{component_source}"
            if not component_source:
                material = (
                    f"{candidate.kind}:parsed-component:{id(candidate.component)}"
                )
            entries.append(
                ("parsed-component", candidate.component, material)
            )
            if component_source:
                seen_sources.add(component_source)
        for label, field, source in cls._image_source_entries(candidate):
            if source in seen_sources:
                continue
            seen_sources.add(source)
            probe = cls._image_from_source(field, source)
            if probe is not None:
                entries.append(
                    (label, probe, f"{candidate.kind}:{label}:{source}")
                )
        return entries

    @classmethod
    def _image_outbound_candidates(cls, candidate: _RepeatCandidate):
        candidates = []
        for _, field, source in cls._image_source_entries(candidate):
            outbound = cls._image_from_source(field, source)
            if outbound is not None:
                candidates.append(outbound)
        return tuple(candidates)

    @staticmethod
    def _apply_image_chain_fingerprint(candidate: _RepeatCandidate):
        if not any(kind == "face" for kind, _ in candidate.chain):
            return candidate
        parts = [
            (kind, value if kind == "face" else candidate.fingerprint)
            for kind, value in candidate.chain
        ]
        serialized = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        candidate.fingerprint = f"image-chain:{digest}"
        candidate.strategy = f"chain-{candidate.strategy}"
        return candidate

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
    def _decode_base64_content(value):
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
        text = re.sub(r"\s+", "", text)
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

    def _media_cache(self):
        cache = getattr(self, "_media_digest_cache", None)
        if cache is None:
            cache = OrderedDict()
            self._media_digest_cache = cache
        return cache

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
        if candidate.kind == "mface":
            if candidate.channel == "event-gif":
                await event.send(MessageChain([candidate.outbound]))
                return
            payload = self._mface_payload(candidate, group_id, self_id)
            bot = getattr(event, "bot", None)
            if candidate.channel == "send_group_msg":
                await bot.send_group_msg(**payload)
                return
            if candidate.channel == "call_action":
                await bot.call_action("send_group_msg", **payload)
                return
            raise RuntimeError("mface channel unavailable")

        outbound = candidate.outbound
        if candidate.kind == "image" and outbound is None:
            probes = candidate.probes or (
                (candidate.probe,) if candidate.probe is not None else ()
            )
            last_error = None
            for probe in probes:
                try:
                    base64_data = await probe.convert_to_base64()
                    content_bytes, base64_text = self._decode_base64_content(
                        base64_data
                    )
                except Exception as e:
                    last_error = e
                    continue
                if content_bytes:
                    outbound = self._image_from_base64(base64_text)
                    break
            if outbound is None:
                if last_error is not None:
                    raise last_error
                raise ValueError("empty image content")
        if outbound is None and not candidate.outbounds:
            raise ValueError("empty outbound component")
        outbounds = candidate.outbounds or (outbound,)
        last_error = None
        for current in outbounds:
            chain = self._image_message_chain(candidate, current)
            try:
                await event.send(chain)
                return
            except Exception as e:
                last_error = e
        if last_error is not None:
            raise last_error
        raise ValueError("empty outbound component")

    @staticmethod
    def _image_message_chain(candidate: _RepeatCandidate, outbound):
        if candidate.chain:
            chain = []
            for kind, value in candidate.chain:
                chain.append(Face(id=int(value)) if kind == "face" else outbound)
        else:
            chain = [outbound]
        return MessageChain(chain)

    @classmethod
    def _mface_payload(
        cls, candidate: _RepeatCandidate, group_id: str, self_id: str
    ):
        data = {}
        aliases = {
            "emoji_package_id": ("emoji_package_id", "emojiPackageId"),
            "emoji_id": ("emoji_id", "emojiId"),
            "key": ("key",),
            "summary": ("summary",),
        }
        for output_key, keys in aliases.items():
            value = cls._candidate_data_value(candidate, *keys)
            if value:
                data[output_key] = value
        payload = {
            "group_id": group_id,
            "message": [{"type": "mface", "data": data}],
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
        if candidate.kind not in ("image", "mface"):
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
