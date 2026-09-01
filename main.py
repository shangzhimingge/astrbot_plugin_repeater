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
import json
import os
import re
import time
from collections.abc import Mapping
from pathlib import Path

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

    @staticmethod
    def _extract_repeatable_component(event: AstrMessageEvent):
        """提取唯一可复读组件，并返回其稳定指纹与原组件。"""
        components = list(event.get_messages() or [])
        if len(components) != 1:
            return None

        component = components[0]
        if isinstance(component, Plain):
            text = str(getattr(component, "text", "") or "").strip()
            if not text:
                return None
            return f"text:{text}", component

        if isinstance(component, Face):
            face_id = getattr(component, "id", None)
            if face_id is None or not str(face_id).strip():
                return None
            return f"face:{face_id}", component

        if isinstance(component, Image):
            image_id = (
                getattr(component, "file", None)
                or getattr(component, "url", None)
                or getattr(component, "path", None)
            )
            if image_id is None or not str(image_id).strip():
                return None
            return f"image:{image_id}", component

        return None

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
        repeatable = self._extract_repeatable_component(event)
        if repeatable is None:
            return
        fingerprint, component = repeatable
        state = self._get_group(group_id)
        async with state.lock:
            repeat_fingerprint = self.process_message(
                group_id, sender_id, fingerprint, time.monotonic()
            )
            if repeat_fingerprint is None:
                return
            try:
                await event.send(MessageChain([component]))
            except Exception as e:
                logger.warning(
                    "发送复读消息失败（群 %s）: %s", group_id, e
                )
                return
            self._commit_repeat(state, time.monotonic())

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
