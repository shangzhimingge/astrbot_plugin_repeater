from __future__ import annotations

import asyncio
import contextvars
import unittest
from types import SimpleNamespace

from tests._astrbot_stubs import install

LOGGER = install()

import main


_NOW = contextvars.ContextVar("test_now", default=0.0)


class _FakeTime:
    @staticmethod
    def monotonic():
        return _NOW.get()

    @staticmethod
    def time():
        return _NOW.get() + 1000


class FakeEvent:
    def __init__(self, group_id, sender_id, text, send_hook=None):
        self.message_obj = SimpleNamespace(
            group_id=group_id,
            self_id="bot",
            sender=SimpleNamespace(user_id=sender_id),
        )
        self.message_str = text
        self.sent = []
        self._send_hook = send_hook

    async def send(self, chain):
        if self._send_hook:
            await self._send_hook()
        self.sent.append(chain[0].text)

    def plain_result(self, text):
        return text


def make_plugin(*, threshold=2, cooldown=0):
    plugin = object.__new__(main.RepeatPlugin)
    plugin.config = {"enable": True, "threshold": threshold, "cooldown": cooldown}
    plugin._groups = {}
    plugin._save_state = lambda: None
    return plugin


async def deliver(plugin, group_id, sender_id, text, now, send_hook=None):
    event = FakeEvent(group_id, sender_id, text, send_hook)
    token = _NOW.set(now)
    try:
        await plugin.on_group_message(event)
    finally:
        _NOW.reset(token)
    return event


class RepeatStateTests(unittest.TestCase):
    def test_group_state_owns_async_lock_and_transaction_fields(self):
        state = main._GroupState(True, 2, 10)

        self.assertIsInstance(state.lock, asyncio.Lock)
        self.assertFalse(state.repeated_in_segment)
        self.assertIsNone(state.last_repeat_time)


class RepeatBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.real_time = main.time
        main.time = _FakeTime

    async def asyncTearDown(self):
        main.time = self.real_time

    async def test_same_segment_repeats_once_but_a_new_segment_can_repeat(self):
        plugin = make_plugin()
        sent = []

        async def send(group, sender, text, now):
            event = await deliver(plugin, group, sender, text, now)
            sent.extend(event.sent)

        await send("g", "u1", "A", 1)
        await send("g", "u2", "A", 2)
        await send("g", "u3", "A", 3)
        await send("g", "u4", "A", 4)
        await send("g", "u1", "B", 5)
        await send("g", "u2", "A", 6)
        await send("g", "u3", "A", 7)

        self.assertEqual(sent, ["A", "A"])

    async def test_cooldown_drops_senders_but_still_changes_segment(self):
        plugin = make_plugin(cooldown=10)

        first = await deliver(plugin, "g", "u1", "A", 100)
        second = await deliver(plugin, "g", "u2", "A", 101)
        during_one = await deliver(plugin, "g", "u3", "B", 105)
        during_two = await deliver(plugin, "g", "u4", "B", 106)
        after_one = await deliver(plugin, "g", "u3", "B", 111)
        after_two = await deliver(plugin, "g", "u4", "B", 112)

        sent = sum((e.sent for e in (first, second, during_one, during_two, after_one, after_two)), [])
        self.assertEqual(sent, ["A", "B"])
        self.assertEqual(plugin._groups["g"].last_repeat_time, 112)

    async def test_failed_send_does_not_commit_and_next_message_retries(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "A", 1)

        async def fail():
            raise RuntimeError("network down")

        failed = await deliver(plugin, "g", "u2", "A", 2, fail)
        retried = await deliver(plugin, "g", "u3", "A", 3)

        self.assertEqual(failed.sent, [])
        self.assertEqual(retried.sent, ["A"])
        self.assertTrue(LOGGER.warnings)
        self.assertEqual(plugin._groups["g"].last_repeat_time, 3)

    async def test_same_group_waits_for_send_before_evaluating_next_message(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "A", 1)
        started = asyncio.Event()
        release = asyncio.Event()

        async def hold_send():
            started.set()
            await release.wait()

        first = asyncio.create_task(deliver(plugin, "g", "u2", "A", 2, hold_send))
        await asyncio.wait_for(started.wait(), 1.0)
        second = asyncio.create_task(deliver(plugin, "g", "u3", "A", 3))
        await asyncio.sleep(0)
        self.assertFalse(second.done())
        release.set()
        one, two = await asyncio.gather(first, second)
        self.assertEqual(one.sent + two.sent, ["A"])

    async def test_different_groups_can_send_in_parallel(self):
        plugin = make_plugin()
        await deliver(plugin, "g1", "u1", "A", 1)
        await deliver(plugin, "g2", "u1", "B", 1)
        started_one = asyncio.Event()
        started_two = asyncio.Event()
        release = asyncio.Event()

        async def hold(started):
            started.set()
            await release.wait()

        one = asyncio.create_task(
            deliver(plugin, "g1", "u2", "A", 2, lambda: hold(started_one))
        )
        two = asyncio.create_task(
            deliver(plugin, "g2", "u2", "B", 2, lambda: hold(started_two))
        )
        await asyncio.wait_for(
            asyncio.gather(started_one.wait(), started_two.wait()), 1.0
        )
        release.set()
        event_one, event_two = await asyncio.gather(one, two)
        self.assertEqual(event_one.sent, ["A"])
        self.assertEqual(event_two.sent, ["B"])

    async def test_success_uses_monotonic_clock(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "A", 320)
        await deliver(plugin, "g", "u2", "A", 321)

        self.assertEqual(plugin._groups["g"].last_repeat_time, 321)


if __name__ == "__main__":
    unittest.main()
