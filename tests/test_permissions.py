from __future__ import annotations

import ast
import json
import unittest
from types import SimpleNamespace
from pathlib import Path

from tests._astrbot_stubs import install

LOGGER = install()

import main


class PermissionEvent:
    def __init__(
        self,
        *,
        role=None,
        mapping=False,
        bot_owner=False,
        admin_error=False,
        group_id="g",
    ):
        if mapping:
            raw_message = {"sender": {"role": role}}
        else:
            raw_message = SimpleNamespace(sender=SimpleNamespace(role=role))
        self.message_obj = SimpleNamespace(
            group_id=group_id,
            raw_message=raw_message,
        )
        self._bot_owner = bot_owner
        self._admin_error = admin_error

    def is_admin(self):
        if self._admin_error:
            raise RuntimeError("admin lookup failed")
        return self._bot_owner

    def plain_result(self, text):
        return text


def make_plugin(permission):
    plugin = object.__new__(main.RepeatPlugin)
    plugin.config = {
        "enable": True,
        "threshold": 2,
        "cooldown": 0,
        "command_permission": permission,
    }
    plugin._groups = {}
    plugin._save_state = lambda: None
    return plugin


async def collect(result):
    return [item async for item in result]


class PermissionDecoratorTests(unittest.TestCase):
    def test_subcommands_use_runtime_permission_gate_not_static_admin_decorator(self):
        tree = ast.parse(Path("main.py").read_text(encoding="utf-8"))
        functions = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

        for name in (
            "repeat_on",
            "repeat_off",
            "repeat_threshold",
            "repeat_cooldown",
            "repeat_status",
        ):
            with self.subTest(name=name):
                decorators = [ast.unparse(item) for item in functions[name].decorator_list]
                self.assertEqual(len(decorators), 1)
                self.assertTrue(decorators[0].startswith("repeat_group.command("))

    def test_schema_lists_all_permission_levels_and_defaults_to_bot_owner(self):
        schema = json.loads(Path("_conf_schema.json").read_text(encoding="utf-8"))

        self.assertIn("command_permission", schema)
        permission = schema["command_permission"]
        self.assertEqual(permission["type"], "string")
        self.assertEqual(permission["default"], "bot_owner")
        self.assertEqual(
            permission["options"], ["member", "admin", "owner", "bot_owner"]
        )
        self.assertEqual(
            permission["labels"], ["群成员", "群管理员", "群主", "BOT主人"]
        )


class RuntimePermissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_config_changes_are_applied_on_the_next_command(self):
        plugin = make_plugin("member")
        event = PermissionEvent(role="member")
        first = await collect(main.RepeatPlugin.repeat_status(plugin, event))
        plugin.config["command_permission"] = "bot_owner"

        second = await collect(main.RepeatPlugin.repeat_status(plugin, event))

        self.assertNotIn("权限不足", first[0])
        self.assertIn("权限不足", second[0])

    async def test_config_change_to_invalid_warns_and_fails_closed_immediately(self):
        LOGGER.warnings.clear()
        plugin = make_plugin("member")
        event = PermissionEvent(role="member")
        await collect(main.RepeatPlugin.repeat_status(plugin, event))
        plugin.config["command_permission"] = "invalid"

        replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))

        self.assertIn("权限不足", replies[0])
        self.assertTrue(LOGGER.warnings)

    async def test_default_gate_denies_before_all_handlers_touch_group_state(self):
        event = PermissionEvent(role="member")
        cases = (
            (main.RepeatPlugin.repeat_on, ()),
            (main.RepeatPlugin.repeat_off, ()),
            (main.RepeatPlugin.repeat_threshold, (3,)),
            (main.RepeatPlugin.repeat_cooldown, (10,)),
            (main.RepeatPlugin.repeat_status, ()),
        )

        for handler, args in cases:
            with self.subTest(handler=handler.__name__):
                plugin = make_plugin("bot_owner")

                def forbidden(_gid):
                    self.fail("denied command accessed group state")

                plugin._get_group = forbidden
                replies = await collect(handler(plugin, event, *args))
                self.assertEqual(len(replies), 1)
                self.assertIn("权限不足", replies[0])

    async def test_permission_hierarchy_supports_mapping_and_attribute_roles(self):
        cases = (
            ("member", PermissionEvent(role=None), True),
            ("admin", PermissionEvent(role="admin", mapping=True), True),
            ("admin", PermissionEvent(role="owner"), True),
            ("owner", PermissionEvent(role="owner", mapping=True), True),
            ("bot_owner", PermissionEvent(role="owner", bot_owner=True), True),
            ("admin", PermissionEvent(role="unexpected"), False),
            ("owner", PermissionEvent(role="admin"), False),
            ("bot_owner", PermissionEvent(role="owner"), False),
        )

        for required, event, allowed in cases:
            with self.subTest(required=required, role=event.message_obj.raw_message):
                plugin = make_plugin(required)
                replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))
                self.assertEqual("权限不足" not in replies[0], allowed)

    async def test_bot_owner_uses_event_is_admin_even_without_onebot_role(self):
        plugin = make_plugin("bot_owner")
        event = PermissionEvent(role=None, bot_owner=True)

        replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))

        self.assertNotIn("权限不足", replies[0])

    async def test_bot_owner_satisfies_every_configured_level(self):
        event = PermissionEvent(role=None, bot_owner=True)

        for required in ("member", "admin", "owner", "bot_owner"):
            with self.subTest(required=required):
                plugin = make_plugin(required)
                replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))
                self.assertNotIn("权限不足", replies[0])

    async def test_admin_lookup_error_fails_bot_owner_but_preserves_onebot_role(self):
        cases = (
            ("bot_owner", "owner", False),
            ("admin", "admin", True),
            ("owner", "owner", True),
        )

        for required, role, allowed in cases:
            with self.subTest(required=required, role=role):
                plugin = make_plugin(required)
                event = PermissionEvent(role=role, admin_error=True)
                replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))
                self.assertEqual("权限不足" not in replies[0], allowed)

    async def test_missing_raw_sender_is_member_only(self):
        for raw_message in (None, {}, {"sender": None}):
            for required, allowed in (("member", True), ("admin", False), ("owner", False)):
                with self.subTest(raw=raw_message, required=required):
                    plugin = make_plugin(required)
                    event = PermissionEvent(role="owner")
                    event.message_obj.raw_message = raw_message
                    replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))
                    self.assertEqual("权限不足" not in replies[0], allowed)

    async def test_authorized_handlers_execute_original_business_paths(self):
        plugin = make_plugin("member")
        plugin.config["enable"] = False
        event = PermissionEvent(role="member")

        on_reply = await collect(main.RepeatPlugin.repeat_on(plugin, event))
        self.assertTrue(plugin._groups["g"].enabled)
        self.assertIn("已开启", on_reply[0])

        threshold_reply = await collect(
            main.RepeatPlugin.repeat_threshold(plugin, event, 3)
        )
        self.assertEqual(plugin._groups["g"].threshold, 3)
        self.assertIn("3", threshold_reply[0])

        cooldown_reply = await collect(
            main.RepeatPlugin.repeat_cooldown(plugin, event, 10)
        )
        self.assertEqual(plugin._groups["g"].cooldown, 10)
        self.assertIn("10", cooldown_reply[0])

        status_reply = await collect(main.RepeatPlugin.repeat_status(plugin, event))
        self.assertIn("触发人数：3", status_reply[0])
        self.assertIn("冷却时间：10", status_reply[0])

        off_reply = await collect(main.RepeatPlugin.repeat_off(plugin, event))
        self.assertFalse(plugin._groups["g"].enabled)
        self.assertIn("已关闭", off_reply[0])

    async def test_invalid_config_warns_and_falls_back_to_bot_owner(self):
        LOGGER.warnings.clear()
        plugin = make_plugin("superuser")
        event = PermissionEvent(role="owner", bot_owner=False)

        replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))

        self.assertIn("权限不足", replies[0])
        self.assertTrue(LOGGER.warnings)

    async def test_private_invocation_is_rejected_before_permission_and_state(self):
        plugin = make_plugin("member")
        event = PermissionEvent(role="owner", bot_owner=True, group_id="")

        def forbidden(_gid):
            self.fail("private command accessed group state")

        plugin._get_group = forbidden
        replies = await collect(main.RepeatPlugin.repeat_status(plugin, event))

        self.assertIn("仅支持在群聊", replies[0])


if __name__ == "__main__":
    unittest.main()
