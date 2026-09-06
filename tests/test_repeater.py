from __future__ import annotations

import asyncio
import contextvars
import unittest
from types import SimpleNamespace

from tests._astrbot_stubs import Face, Image, Plain, install

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
    def __init__(
        self,
        group_id,
        sender_id,
        text,
        send_hook=None,
        messages=None,
        raw_message=None,
    ):
        self.message_obj = SimpleNamespace(
            group_id=group_id,
            self_id="bot",
            sender=SimpleNamespace(user_id=sender_id),
            raw_message=raw_message,
        )
        self.message_str = text
        self._messages = list(messages if messages is not None else [Plain(text)])
        self.sent = []
        self._send_hook = send_hook

    def get_messages(self):
        return self._messages

    async def send(self, chain):
        if self._send_hook:
            await self._send_hook()
        if isinstance(chain[0], Image):
            chain[0].convert_to_base64()
        self.sent.append(chain[0])

    def plain_result(self, text):
        return text


def make_plugin(*, threshold=2, cooldown=0):
    plugin = object.__new__(main.RepeatPlugin)
    plugin.config = {"enable": True, "threshold": threshold, "cooldown": cooldown}
    plugin._groups = {}
    plugin._save_state = lambda: None
    return plugin


async def deliver(
    plugin,
    group_id,
    sender_id,
    text,
    now,
    send_hook=None,
    messages=None,
    raw_message=None,
):
    event = FakeEvent(
        group_id, sender_id, text, send_hook, messages, raw_message
    )
    token = _NOW.set(now)
    try:
        await plugin.on_group_message(event)
    finally:
        _NOW.reset(token)
    return event


def sent_values(*events):
    values = []
    for event in events:
        for component in event.sent:
            if isinstance(component, Plain):
                values.append(component.text)
            elif isinstance(component, Face):
                values.append(("face", component.id))
            elif isinstance(component, Image):
                values.append(
                    ("image", component.file, component.url, component.path)
                )
    return values


class RepeatStateTests(unittest.TestCase):
    def test_group_state_owns_async_lock_and_transaction_fields(self):
        state = main._GroupState(True, 2, 10)

        self.assertIsInstance(state.lock, asyncio.Lock)
        self.assertIsNone(state.current_fingerprint)
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
            sent.extend(sent_values(event))

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

        sent = sent_values(
            first, second, during_one, during_two, after_one, after_two
        )
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
        self.assertEqual(sent_values(retried), ["A"])
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
        self.assertEqual(sent_values(one, two), ["A"])

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
        self.assertEqual(sent_values(event_one), ["A"])
        self.assertEqual(sent_values(event_two), ["B"])

    async def test_success_uses_monotonic_clock(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "A", 320)
        await deliver(plugin, "g", "u2", "A", 321)

        self.assertEqual(plugin._groups["g"].last_repeat_time, 321)

    async def test_face_is_sent_unchanged_and_repeats_only_once_per_segment(self):
        plugin = make_plugin()
        first_face = Face(14)
        triggering_face = Face(14)

        first = await deliver(plugin, "g", "u1", "", 1, messages=[first_face])
        trigger = await deliver(
            plugin, "g", "u2", "", 2, messages=[triggering_face]
        )
        later = await deliver(plugin, "g", "u3", "", 3, messages=[Face(14)])

        self.assertEqual(first.sent, [])
        self.assertEqual(later.sent, [])
        self.assertEqual(trigger.sent, [triggering_face])
        self.assertIs(trigger.sent[0], triggering_face)

    async def test_plain_is_sent_unchanged(self):
        plugin = make_plugin()
        first = Plain("A")
        triggering = Plain("A")

        await deliver(plugin, "g", "u1", "A", 1, messages=[first])
        event = await deliver(
            plugin, "g", "u2", "A", 2, messages=[triggering]
        )

        self.assertEqual(event.sent, [triggering])
        self.assertIs(event.sent[0], triggering)

    async def test_raw_file_unique_stabilizes_message_level_file_tokens(self):
        plugin = make_plugin()
        unique = "0123456789abcdef0123456789abcdef"
        raw = {
            "message": [
                {"type": "image", "data": {"file_unique": unique}}
            ]
        }
        first = Image(
            file="message-token-one",
            url="https://example.test/a.png?rkey=one&fileid=42",
        )
        triggering = Image(
            file="message-token-two",
            url="https://example.test/a.png?rkey=two&fileid=42",
        )

        await deliver(
            plugin, "g", "u1", "", 1, messages=[first], raw_message=raw
        )
        trigger = await deliver(
            plugin,
            "g",
            "u2",
            "",
            2,
            messages=[triggering],
            raw_message=raw,
        )

        self.assertEqual(len(trigger.sent), 1)
        output = trigger.sent[0]
        self.assertIsNot(output, triggering)
        self.assertEqual(output.constructed_by, "url")
        self.assertEqual(output.converted_source, triggering.url)

    async def test_long_tokens_with_same_trailing_hash_are_the_same_image(self):
        plugin = make_plugin()
        digest = "0123456789abcdef0123456789abcdef"
        first = Image(
            file=f"opaque-message-token-one/{digest}.jpg",
            url="https://example.test/one",
        )
        triggering = Image(
            file=f"opaque-message-token-two/{digest}.jpg",
            url="https://example.test/two",
        )

        await deliver(plugin, "g", "u1", "", 1, messages=[first])
        trigger = await deliver(
            plugin, "g", "u2", "", 2, messages=[triggering]
        )

        self.assertEqual(len(trigger.sent), 1)

    async def test_hash_filename_supports_modern_md5_and_sha256(self):
        digests = (
            "F5EF2DCCB5E7A9A24A1B932F13BD2C08",
            "0123456789ABCDEF" * 4,
        )
        for digest in digests:
            with self.subTest(length=len(digest)):
                plugin = make_plugin()
                first = Image(file=f"token-a/{digest}.jpg")
                triggering = Image(file=f"token-b/{digest.lower()}.jpg")

                await deliver(plugin, "g", "u1", "", 1, messages=[first])
                trigger = await deliver(
                    plugin, "g", "u2", "", 2, messages=[triggering]
                )

                self.assertEqual(len(trigger.sent), 1)

    async def test_image_send_rebuilds_with_url_path_then_file_priority(self):
        cases = (
            (
                Image(
                    file="message-token",
                    url="https://example.test/a.png",
                    path="/tmp/not-selected.png",
                ),
                "url",
                "https://example.test/a.png",
            ),
            (Image(file="message-token", path="/tmp/a.png"), "path", "/tmp/a.png"),
            (
                Image(file="https://example.test/from-file.png"),
                "url",
                "https://example.test/from-file.png",
            ),
            (Image(file="/tmp/from-file.png"), "path", "/tmp/from-file.png"),
        )

        for triggering, expected_constructor, expected_source in cases:
            with self.subTest(image=vars(triggering)):
                plugin = make_plugin()
                first = Image(
                    file=triggering.file,
                    url=triggering.url,
                    path=triggering.path,
                )
                await deliver(plugin, "g", "u1", "", 1, messages=[first])
                event = await deliver(
                    plugin, "g", "u2", "", 2, messages=[triggering]
                )

                self.assertEqual(len(event.sent), 1)
                output = event.sent[0]
                self.assertIsNot(output, triggering)
                self.assertEqual(output.constructed_by, expected_constructor)
                self.assertEqual(output.converted_source, expected_source)
                self.assertTrue(output.file)

    async def test_url_ignores_only_rkey_and_preserves_image_query_identity(self):
        plugin = make_plugin()
        first = Image(
            file="message-token-one",
            url=(
                "https://example.test/download?appid=1407&fileid=one&"
                f"rkey={'1' * 32}"
            ),
        )
        triggering = Image(
            file="message-token-two",
            url=(
                "https://example.test/download?appid=1407&fileid=one&"
                f"rkey={'2' * 32}"
            ),
        )
        different = Image(
            file="message-token-three",
            url=(
                "https://example.test/download?appid=1407&fileid=two&"
                f"rkey={'2' * 32}"
            ),
        )

        await deliver(plugin, "g", "u1", "", 1, messages=[first])
        trigger = await deliver(
            plugin, "g", "u2", "", 2, messages=[triggering]
        )
        different_event = await deliver(
            plugin, "g", "u1", "", 3, messages=[different]
        )
        second_fileid_trigger = await deliver(
            plugin,
            "g",
            "u2",
            "",
            4,
            messages=[
                Image(
                    file="message-token-four",
                    url=different.url.replace("2" * 32, "3" * 32),
                )
            ],
        )

        self.assertEqual(len(trigger.sent), 1)
        self.assertEqual(different_event.sent, [])
        self.assertEqual(len(second_fileid_trigger.sent), 1)

    async def test_component_change_starts_a_new_segment(self):
        plugin = make_plugin()
        events = []
        events.append(await deliver(plugin, "g", "u1", "", 1, messages=[Face(1)]))
        events.append(await deliver(plugin, "g", "u2", "", 2, messages=[Face(1)]))
        events.append(
            await deliver(plugin, "g", "u1", "", 3, messages=[Image(file="x")])
        )
        events.append(
            await deliver(plugin, "g", "u2", "", 4, messages=[Image(file="x")])
        )

        self.assertEqual(
            sent_values(*events),
            [("face", 1), ("image", "x", None, None)],
        )

    async def test_mixed_or_multi_component_message_is_ignored_without_cutting_segment(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "", 1, messages=[Face(7)])

        mixed = await deliver(
            plugin, "g", "ignored", "hello", 2, messages=[Plain("hello"), Face(7)]
        )
        unsupported = await deliver(
            plugin, "g", "ignored2", "", 3, messages=[object()]
        )
        trigger = await deliver(plugin, "g", "u2", "", 4, messages=[Face(7)])

        self.assertEqual(mixed.sent, [])
        self.assertEqual(unsupported.sent, [])
        self.assertEqual(sent_values(trigger), [("face", 7)])

    async def test_image_without_effective_identifier_is_ignored_without_cutting_segment(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "", 1, messages=[Face(7)])
        ignored = await deliver(plugin, "g", "x", "", 2, messages=[Image()])
        trigger = await deliver(plugin, "g", "u2", "", 3, messages=[Face(7)])

        self.assertEqual(ignored.sent, [])
        self.assertEqual(sent_values(trigger), [("face", 7)])

    async def test_image_and_face_send_failure_can_retry(self):
        for component_factory in (lambda: Face(9), lambda: Image(url="image-url")):
            with self.subTest(component=type(component_factory()).__name__):
                plugin = make_plugin()
                await deliver(
                    plugin, "g", "u1", "", 1, messages=[component_factory()]
                )

                async def fail():
                    raise RuntimeError("network down")

                failed = await deliver(
                    plugin,
                    "g",
                    "u2",
                    "",
                    2,
                    send_hook=fail,
                    messages=[component_factory()],
                )
                retry_component = component_factory()
                retried = await deliver(
                    plugin, "g", "u3", "", 3, messages=[retry_component]
                )

                self.assertEqual(failed.sent, [])
                self.assertEqual(len(retried.sent), 1)
                if isinstance(retry_component, Image):
                    self.assertIsNot(retried.sent[0], retry_component)
                    self.assertTrue(retried.sent[0].converted_source)
                else:
                    self.assertIs(retried.sent[0], retry_component)

    async def test_component_senders_do_not_accumulate_during_cooldown(self):
        plugin = make_plugin(cooldown=10)
        await deliver(plugin, "g", "u1", "", 100, messages=[Face(1)])
        first = await deliver(plugin, "g", "u2", "", 101, messages=[Face(1)])
        during_one = await deliver(
            plugin, "g", "u3", "", 105, messages=[Image(file="x")]
        )
        during_two = await deliver(
            plugin, "g", "u4", "", 106, messages=[Image(file="x")]
        )
        after_one = await deliver(
            plugin, "g", "u3", "", 111, messages=[Image(file="x")]
        )
        after_two = await deliver(
            plugin, "g", "u4", "", 112, messages=[Image(file="x")]
        )

        self.assertEqual(
            sent_values(first, during_one, during_two, after_one, after_two),
            [("face", 1), ("image", "x", None, None)],
        )

    async def test_unicode_emoji_remains_a_plain_message(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "😀", 1)
        trigger = await deliver(plugin, "g", "u2", "😀", 2)

        self.assertEqual(sent_values(trigger), ["😀"])


if __name__ == "__main__":
    unittest.main()
