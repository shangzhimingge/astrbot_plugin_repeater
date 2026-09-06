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
    channel: str = "event"
    outbound: object | None = None
    probe: object | None = None
    content_base64: str | None = None


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

    @classmethod
    def _extract_candidate(cls, event: AstrMessageEvent):
        """Raw OneBot 媒体优先；没有 raw 媒体时才回退解析后组件。"""
        found_raw_media, candidate = cls._raw_media_candidate(event)
        if found_raw_media:
            return candidate

        components = list(event.get_messages() or [])
        if len(components) != 1:
            return None
        component = components[0]
        if isinstance(component, Plain):
            text = cls._clean_value(getattr(component, "text", None))
            if not text:
                return None
            return _RepeatCandidate(
                kind="text",
                source="parsed",
                component=component,
                fingerprint=f"text:{text}",
                strategy="plain",
                outbound=component,
            )
        if isinstance(component, Face):
            face_id = cls._clean_value(getattr(component, "id", None))
            if not face_id:
                return None
            return _RepeatCandidate(
                kind="face",
                source="parsed",
                component=component,
                fingerprint=f"face:{face_id}",
                strategy="face-id",
                outbound=component,
            )
        if isinstance(component, Image):
            return _RepeatCandidate(
                kind="image", source="parsed", component=component
            )
        return None

    @classmethod
    def _raw_media_candidate(cls, event: AstrMessageEvent):
        raw = cls._value_of(getattr(event, "message_obj", None), "raw_message")
        segments = cls._value_of(raw, "message")
        if not isinstance(segments, (list, tuple)):
            return False, None

        media = []
        for segment in segments:
            segment_type = cls._clean_value(
                cls._value_of(segment, "type")
            ).lower()
            if segment_type in ("image", "mface"):
                media.append((segment_type, segment))
        if not media:
            return False, None
        if len(media) != 1:
            return True, None

        self_id = cls._clean_value(
            cls._value_of(getattr(event, "message_obj", None), "self_id")
            or cls._value_of(raw, "self_id")
        )
        target_segment = media[0][1]
        for segment in segments:
            if segment is target_segment:
                continue
            segment_type = cls._clean_value(
                cls._value_of(segment, "type")
            ).lower()
            data = cls._value_of(segment, "data")
            if segment_type == "reply":
                continue
            if segment_type == "text" and not cls._clean_value(
                cls._value_of(data, "text")
            ):
                continue
            if segment_type == "at" and self_id:
                qq = cls._clean_value(cls._value_of(data, "qq"))
                if qq == self_id:
                    continue
            return True, None

        kind, segment = media[0]
        data = cls._value_of(segment, "data")
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
            candidate.outbound = self._image_from_candidate(candidate)
            return candidate if candidate.outbound is not None else None
        if candidate.kind == "mface":
            candidate.strategy = "missing-market-identity"
            return None

        values = self._image_values(candidate)
        hash_values = (
            values["file"],
            self._normalize_image_value(values["url"]),
            values["path"],
        )
        for value in hash_values:
            digest = self._terminal_content_hash(value)
            if digest:
                candidate.fingerprint = f"image:hash:{digest}"
                candidate.strategy = "terminal-hash"
                candidate.outbound = self._image_from_candidate(candidate)
                return candidate if candidate.outbound is not None else None

        for value in hash_values:
            digest = self._uuid_content_hash(value)
            if digest:
                candidate.fingerprint = f"image:hash:{digest}"
                candidate.strategy = "uuid-hash"
                candidate.outbound = self._image_from_candidate(candidate)
                return candidate if candidate.outbound is not None else None

        file_unique = values["file_unique"]
        digest = self._hash_shaped_file_unique(file_unique)
        if digest:
            candidate.fingerprint = f"image:unique:{digest}"
            candidate.strategy = "file-unique-hash"
            candidate.outbound = self._image_from_candidate(candidate)
            return candidate if candidate.outbound is not None else None

        url_value = values["url"]
        if not url_value and values["file"].lower().startswith(
            ("http://", "https://")
        ):
            url_value = values["file"]
        normalized_url = self._normalize_image_value(url_value)
        if normalized_url:
            digest = hashlib.sha256(normalized_url.encode()).hexdigest()
            candidate.fingerprint = f"image:url:{digest}"
            candidate.strategy = "normalized-url"
            candidate.outbound = self._image_from_candidate(candidate)
            return candidate if candidate.outbound is not None else None

        field, source = self._select_image_source(values)
        if not source:
            candidate.strategy = "missing-image-source"
            return None
        probe = self._probe_for_source(candidate.component, field, source)
        if probe is None:
            candidate.strategy = "invalid-image-source"
            return None
        candidate.probe = probe

        cache = self._media_cache()
        cache_key = hashlib.sha256(f"{field}:{source}".encode()).hexdigest()
        cached = cache.get(cache_key)
        if cached is not None:
            cached_at, digest = cached
            if now - cached_at <= _MEDIA_CACHE_TTL:
                cache.move_to_end(cache_key)
                candidate.fingerprint = f"image:content:{digest}"
                candidate.strategy = "content-cache"
                return candidate
            cache.pop(cache_key, None)

        base64_data = await probe.convert_to_base64()
        base64_text = self._clean_value(base64_data)
        if not base64_text:
            candidate.strategy = "empty-content"
            return None
        digest = hashlib.sha256(base64_text.encode()).hexdigest()
        cache[cache_key] = (now, digest)
        cache.move_to_end(cache_key)
        while len(cache) > _MEDIA_CACHE_LIMIT:
            cache.popitem(last=False)
        candidate.fingerprint = f"image:content:{digest}"
        candidate.strategy = "content-sha256"
        candidate.content_base64 = base64_text
        candidate.outbound = self._image_from_base64(base64_text)
        return candidate

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
        if package_id and emoji_id:
            return f"market:{package_id}:{emoji_id}", "market-id"
        key = cls._candidate_data_value(candidate, "key")
        if key:
            digest = hashlib.sha256(key.encode()).hexdigest()
            return f"market:key:{digest}", "market-key-sha256"
        return None

    @classmethod
    def _image_values(cls, candidate: _RepeatCandidate):
        return {
            key: cls._candidate_data_value(candidate, key)
            for key in ("file", "url", "path", "file_unique")
        }

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
    def _select_image_source(values):
        for field in ("url", "path", "file"):
            if values[field]:
                return field, values[field]
        return "", ""

    @classmethod
    def _probe_for_source(cls, component, field: str, source: str):
        if isinstance(component, Image):
            component_url = cls._clean_value(getattr(component, "url", None))
            component_file = cls._clean_value(
                getattr(component, "file", None)
            )
            if field == "url" and component_url == source:
                return component
            if field == "file" and not component_url and component_file == source:
                return component
        return cls._image_from_source(field, source)

    @classmethod
    def _image_from_candidate(cls, candidate: _RepeatCandidate):
        values = cls._image_values(candidate)
        field, source = cls._select_image_source(values)
        return cls._image_from_source(field, source)

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
            base64_data = await candidate.probe.convert_to_base64()
            base64_text = self._clean_value(base64_data)
            if not base64_text:
                raise ValueError("empty image content")
            outbound = self._image_from_base64(base64_text)
        if outbound is None:
            raise ValueError("empty outbound component")
        await event.send(MessageChain([outbound]))

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

    @staticmethod
    def _debug_media(
        candidate: _RepeatCandidate,
        state: _GroupState,
        error_type: str = "-",
    ) -> None:
        if candidate.kind not in ("image", "mface"):
            return
        fingerprint = candidate.fingerprint or ""
        short_fingerprint = (
            hashlib.sha256(fingerprint.encode()).hexdigest()[:12]
            if fingerprint
            else "-"
        )
        logger.debug(
            "[repeater.media] type=%s source=%s strategy=%s fp=%s "
            "count=%d threshold=%d cooldown=%g repeated=%s "
            "channel=%s error=%s",
            candidate.kind,
            candidate.source,
            candidate.strategy,
            short_fingerprint,
            len(state.senders),
            state.threshold,
            state.cooldown,
            state.repeated_in_segment,
            candidate.channel,
            error_type,
        )

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
            try:
                candidate = await self._resolve_candidate(
                    event, candidate, time.monotonic()
                )
            except Exception as e:
                unresolved = candidate
                unresolved.strategy = "resolve-error"
                self._debug_media(unresolved, state, type(e).__name__)
                logger.warning(
                    "解析复读媒体失败（群 %s，%s）",
                    group_id,
                    type(e).__name__,
                )
                return
            if candidate is None:
                return
            repeat_fingerprint = self.process_message(
                group_id,
                sender_id,
                candidate.fingerprint,
                time.monotonic(),
            )
            self._debug_media(candidate, state)
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
                self._debug_media(candidate, state, type(e).__name__)
                logger.warning(
                    "发送复读消息失败（群 %s，%s）",
                    group_id,
                    type(e).__name__,
                )
                return
            self._commit_repeat(state, time.monotonic())
            self._debug_media(candidate, state)

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
