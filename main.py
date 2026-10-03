# AstrBot 群复读机插件（适配 NapCatQQ / OneBot v11）
#
# 功能：当群内连续 N 个【不同的人】发送了相同内容时，机器人自动跟一条相同消息。
#  - N 可通过 WebUI 配置或群内指令自定义（默认 3）。
#  - 每个群可独立开关复读机模式（/复读机 开|关）。
#  - 支持触发冷却时间，防止机器人刷屏。
#  - 复读会保留 QQ 小黄脸（Face）表情。
#
# 说明：核心逻辑在 process_message() 中，为纯同步函数，便于本地单元测试；
#      事件监听 / 指令处理只是薄封装。持久化数据存放于 AstrBot 的
#      data/plugin_data/<插件名>/ 目录。

import json
import re
import threading
import time
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Face, Plain
from astrbot.api.star import Context, Star, StarTools


# 指令组：群内通过 `/复读机 子指令` 控制复读机
@filter.command_group("复读机")
def repeat_group():
    """群复读机控制指令组：/复读机 开|关|阈值 N|冷却 N|状态"""
    pass


# 匹配指令调用本身（`/复读机 ...`、`!复读机 ...` 或 `复读机 ...`），
# 避免把指令消息误当成复读内容参与计数。
_CMD_PATTERN = re.compile(r"^[/!]?复读机(?:\s|$)")


class _GroupState:
    """单个群的运行状态（含实时计数）"""

    __slots__ = (
        "enabled",
        "threshold",
        "cooldown",
        "current_text",
        "senders",
        "repeated",
        "last_repeat_time",
    )

    def __init__(self, enabled: bool, threshold: int, cooldown: float):
        self.enabled = enabled
        self.threshold = threshold
        self.cooldown = cooldown
        self.current_text: str | None = None  # 当前正在统计的消息指纹
        self.senders: set[str] = set()  # 已发送该消息的不同人（去重）
        self.repeated: bool = False  # 本轮接龙是否已经复读过
        self.last_repeat_time: float = 0.0  # 上次触发复读的时间戳


class RepeatPlugin(Star):
    """群复读机插件"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._lock = threading.Lock()
        self._groups: dict[str, _GroupState] = {}
        self._state_file = self._data_dir() / "state.json"
        self._load_state()
        logger.info("复读机插件已加载，群状态数据文件: %s", self._state_file)

    # ------------------------------------------------------------------
    # 核心逻辑（纯同步函数，便于本地测试）
    # ------------------------------------------------------------------
    def process_message(
        self, group_id: str, sender_id: str, content: str, now: float
    ) -> bool:
        """处理一条群消息，返回是否需要复读。

        - content 为消息内容指纹（文本 + 表情），只统计【连续】发送的相同内容；
        - 同一个人的多次重复只算 1 人；
        - 一轮接龙在未被打断（内容保持不变）前只复读一次；内容被打断
          （变成别的消息）后重新开始计数，之后可再次复读；
        - 支持冷却时间（seconds）。
        """
        state = self._get_group(group_id)
        content = (content or "").strip()
        with self._lock:
            if not state.enabled or not content:
                return False
            if content != state.current_text:
                # 接龙内容变化：开始新的一轮，重置计数与「已复读」标记
                state.current_text = content
                state.senders = {sender_id}
                state.repeated = False
                return False
            if state.repeated:
                # 本轮接龙已经复读过，不再重复，直到被打断（内容变化）
                return False
            state.senders.add(sender_id)
            if len(state.senders) < state.threshold:
                return False
            # 达到阈值：检查冷却时间
            if state.cooldown > 0 and now - state.last_repeat_time < state.cooldown:
                return False
            # 触发复读，并标记本轮已复读（不重置内容指纹，保证接龙判定连续）
            state.repeated = True
            state.last_repeat_time = now
            return True

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
        # 以「文本 + QQ 表情」构造内容指纹与复读用消息链，避免复读丢失小黄脸
        content, reply_chain = self._build_content(text, event.get_messages())
        if not content:
            return
        if self.process_message(group_id, sender_id, content, time.time()):
            await event.send(MessageChain(reply_chain))

    # ------------------------------------------------------------------
    # 指令组：群内控制
    # ------------------------------------------------------------------
    @repeat_group.command("开")
    async def repeat_on(self, event: AstrMessageEvent):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        state = self._get_group(gid)
        with self._lock:
            state.enabled = True
        self._save_state()
        yield event.plain_result(
            f"✅ 复读机模式已开启！群内连续 {state.threshold} 个不同的人"
            "发送相同消息时，我会自动跟一条。"
        )

    @repeat_group.command("关")
    async def repeat_off(self, event: AstrMessageEvent):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        state = self._get_group(gid)
        with self._lock:
            state.enabled = False
        self._save_state()
        yield event.plain_result("🛑 复读机模式已关闭")

    @repeat_group.command("阈值")
    async def repeat_threshold(self, event: AstrMessageEvent, num: int = None):
        gid = self._event_group_id(event)
        if not gid:
            yield event.plain_result("该指令仅支持在群聊中使用")
            return
        if num is None or not (2 <= num <= 50):
            yield event.plain_result("用法：/复读机 阈值 <人数>（2~50 的整数）")
            return
        state = self._get_group(gid)
        with self._lock:
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
        if num is None or not (0 <= num <= 3600):
            yield event.plain_result("用法：/复读机 冷却 <秒数>（0~3600，0 表示无冷却）")
            return
        state = self._get_group(gid)
        with self._lock:
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
        state = self._get_group(gid)
        status = "已开启" if state.enabled else "已关闭"
        yield event.plain_result(
            "📋 复读机状态\n"
            f"状态：{status}\n"
            f"触发人数：{state.threshold} 人\n"
            f"冷却时间：{int(state.cooldown)} 秒\n"
            "——\n"
            "指令：/复读机 开 | 关 | 阈值 N | 冷却 N | 状态"
        )

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------
    @staticmethod
    def _build_content(text: str, components) -> tuple[str, list]:
        """构造内容指纹（用于接龙判定）与复读用消息链。

        文本沿用 message_str（与旧逻辑一致，含 @ 文本），并额外保留下 QQ
        小黄脸（Face）表情：旧实现只用 message_str，表情既不参与判重，
        复读时也会丢失。此处在指纹与复读消息链中都带上表情 id。
        """
        comps = list(components or [])
        faces = [c for c in comps if isinstance(c, Face)]
        key = (text or "").strip()
        if faces:
            key += "".join(f"[Face:{f.id}]" for f in faces)
        chain: list = []
        if (text or "").strip():
            chain.append(Plain(text))
        chain.extend(Face(id=f.id) for f in faces)
        return key, chain

    def _event_group_id(self, event: AstrMessageEvent) -> str:
        return str(getattr(event.message_obj, "group_id", "") or "")

    def _get_group(self, group_id: str) -> _GroupState:
        gid = str(group_id)
        with self._lock:
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
        """返回 AstrBot 官方规定的插件数据目录。

        即 ``data/plugin_data/<插件名>/``，通过 AstrBot 的 ``StarTools``
        统一获取，便于数据备份、迁移与审计；不写入插件自身目录，避免插件
        更新/重装时数据被覆盖。
        """
        return StarTools.get_data_dir("astrbot_plugin_repeater")

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