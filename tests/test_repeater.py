from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import unittest
from collections import OrderedDict
from types import SimpleNamespace

from tests._astrbot_stubs import At, Face, Image, Plain, Reply, install

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
        bot=None,
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
        self.send_attempts = []
        self._send_hook = send_hook
        self.bot = bot

    def get_messages(self):
        return self._messages

    async def send(self, chain):
        self.send_attempts.append(tuple(chain))
        if self._send_hook:
            await self._send_hook()
        for component in chain:
            if isinstance(component, Image):
                await component.convert_to_base64()
        self.sent.extend(chain)

    def plain_result(self, text):
        return text


def make_plugin(*, threshold=2, cooldown=0, media_diagnostics=False):
    plugin = object.__new__(main.RepeatPlugin)
    plugin.config = {
        "enable": True,
        "threshold": threshold,
        "cooldown": cooldown,
        "media_diagnostics": media_diagnostics,
    }
    plugin._groups = {}
    plugin._media_digest_cache = OrderedDict()
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
    bot=None,
):
    event = FakeEvent(
        group_id, sender_id, text, send_hook, messages, raw_message, bot
    )
    token = _NOW.set(now)
    try:
        await plugin.on_group_message(event)
    finally:
        _NOW.reset(token)
    return event


def raw_segment(segment_type, **data):
    return {"type": segment_type, "data": data}


def raw_event(*segments, self_id="bot"):
    return {"self_id": self_id, "message": list(segments)}


def encoded(value):
    if isinstance(value, str):
        value = value.encode()
    return base64.b64encode(value).decode()


def rendered_debugs():
    return "\n".join(
        str(args[0]) % tuple(args[1:]) for args in LOGGER.debugs
    )


def rendered_infos():
    return "\n".join(
        str(args[0]) % tuple(args[1:]) for args in LOGGER.infos
    )


class FakeDirectBot:
    def __init__(self, failures=0):
        self.failures = failures
        self.send_group_calls = []
        self.action_calls = []

    async def send_group_msg(self, **payload):
        self.send_group_calls.append(payload)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("mface send failed")

    async def call_action(self, action, **payload):
        self.action_calls.append((action, payload))


class FakeActionBot:
    def __init__(self):
        self.action_calls = []

    async def call_action(self, action, **payload):
        self.action_calls.append((action, payload))


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
        digest = "0123456789abcdef0123456789abcdef"
        cases = (
            (
                Image(
                    file="message-token",
                    url="https://example.test/a.png?fileid=stable-a",
                    path="/tmp/not-selected.png",
                ),
                "url",
                "https://example.test/a.png?fileid=stable-a",
            ),
            (
                Image(
                    file="message-token",
                    path=f"/tmp/{digest}.png",
                ),
                "path",
                f"/tmp/{digest}.png",
            ),
            (
                Image(
                    file="https://example.test/from-file.png?fileid=stable-b"
                ),
                "base64",
                "base64://"
                + encoded(
                    "https://example.test/from-file.png?fileid=stable-b"
                ),
            ),
            (
                Image(file=f"/tmp/{digest}.png"),
                "path",
                f"/tmp/{digest}.png",
            ),
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

    async def test_url_identity_uses_content_not_query_fields(self):
        plugin = make_plugin()
        same_content = encoded("same-url-content")
        different_content = encoded("different-url-content")
        first = Image(
            file="message-token-one",
            url=(
                "https://example.test/download?appid=1407&fileid=one&"
                f"rkey={'1' * 32}"
            ),
            base64_data=same_content,
        )
        triggering = Image(
            file="message-token-two",
            url=(
                "https://example.test/download?appid=1407&fileid=one&"
                f"rkey={'2' * 32}"
            ),
            base64_data=same_content,
        )
        different = Image(
            file="message-token-three",
            url=(
                "https://example.test/download?appid=1407&fileid=two&"
                f"rkey={'2' * 32}"
            ),
            base64_data=different_content,
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
                    base64_data=different_content,
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
            await deliver(
                plugin,
                "g",
                "u1",
                "",
                3,
                messages=[Image(file="x", base64_data=encoded("x"))],
            )
        )
        events.append(
            await deliver(
                plugin,
                "g",
                "u2",
                "",
                4,
                messages=[Image(file="x", base64_data=encoded("x"))],
            )
        )

        self.assertEqual(
            sent_values(*events),
            [("face", 1), ("image", f"base64://{encoded('x')}", None, None)],
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
        for component_factory in (
            lambda: Face(9),
            lambda: Image(
                file="opaque-image", base64_data=encoded("image-content")
            ),
        ):
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
            plugin,
            "g",
            "u3",
            "",
            105,
            messages=[Image(file="x", base64_data=encoded("x"))],
        )
        during_two = await deliver(
            plugin,
            "g",
            "u4",
            "",
            106,
            messages=[Image(file="x", base64_data=encoded("x"))],
        )
        after_one = await deliver(
            plugin,
            "g",
            "u3",
            "",
            111,
            messages=[Image(file="x", base64_data=encoded("x"))],
        )
        after_two = await deliver(
            plugin,
            "g",
            "u4",
            "",
            112,
            messages=[Image(file="x", base64_data=encoded("x"))],
        )

        self.assertEqual(
            sent_values(first, during_one, during_two, after_one, after_two),
            [("face", 1), ("image", f"base64://{encoded('x')}", None, None)],
        )

    async def test_unicode_emoji_remains_a_plain_message(self):
        plugin = make_plugin()
        await deliver(plugin, "g", "u1", "😀", 1)
        trigger = await deliver(plugin, "g", "u2", "😀", 2)

        self.assertEqual(sent_values(trigger), ["😀"])


class RawMediaBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.real_time = main.time
        main.time = _FakeTime
        LOGGER.debugs.clear()
        LOGGER.infos.clear()
        Image.conversion_failures.clear()
        Image.conversion_payloads.clear()

    async def asyncTearDown(self):
        Image.conversion_failures.clear()
        Image.conversion_payloads.clear()
        main.time = self.real_time

    @staticmethod
    def _face_image_raw(face_id, image_file, *, image_first=False):
        face = raw_segment("face", id=str(face_id))
        image = raw_segment("image", file=image_file)
        parts = (image, face) if image_first else (face, image)
        return raw_event(*parts)

    async def test_raw_face_image_chain_repeats_in_original_order(self):
        plugin = make_plugin()
        raw = self._face_image_raw(339, "raw-token")
        parsed_one = Image(file="parsed-token", base64_data=encoded("photo"))
        parsed_two = Image(file="parsed-token", base64_data=encoded("photo"))

        await deliver(
            plugin, "g", "u1", "", 1,
            messages=[Face(339), parsed_one], raw_message=raw,
        )
        trigger = await deliver(
            plugin, "g", "u2", "", 2,
            messages=[Face(339), parsed_two], raw_message=raw,
        )

        self.assertEqual(sent_values(trigger), [
            ("face", 339),
            ("image", f"base64://{encoded('photo')}", None, None),
        ])

    async def test_face_image_identity_includes_face_image_and_order(self):
        plugin = make_plugin()

        async def put(sender, now, face_id, image_file, image_first=False):
            raw = self._face_image_raw(
                face_id, image_file, image_first=image_first
            )
            image = Image(file=image_file, base64_data=encoded(image_file))
            parsed = [image, Face(face_id)] if image_first else [Face(face_id), image]
            return await deliver(
                plugin, "g", sender, "", now,
                messages=parsed, raw_message=raw,
            )

        events = [
            await put("u1", 1, 339, "image-a"),
            await put("u2", 2, 340, "image-a"),
            await put("u3", 3, 339, "image-b"),
            await put("u4", 4, 339, "image-b", True),
            await put("u5", 5, 339, "image-b", True),
        ]

        self.assertEqual(sent_values(*events[:-1]), [])
        self.assertEqual(sent_values(events[-1]), [
            ("image", f"base64://{encoded('image-b')}", None, None),
            ("face", 339),
        ])

    async def test_face_image_chain_repeats_once_then_after_new_segment(self):
        plugin = make_plugin()

        async def put(sender, now, face_id, image_file):
            raw = self._face_image_raw(face_id, image_file)
            return await deliver(
                plugin, "g", sender, "", now,
                messages=[
                    Face(face_id),
                    Image(file=image_file, base64_data=encoded(image_file)),
                ],
                raw_message=raw,
            )

        events = [
            await put("u1", 1, 339, "image-a"),
            await put("u2", 2, 339, "image-a"),
            await put("u3", 3, 339, "image-a"),
            await put("u4", 4, 340, "image-a"),
            await put("u5", 5, 339, "image-a"),
            await put("u6", 6, 339, "image-a"),
        ]

        self.assertEqual(len(sent_values(*events)), 4)
        self.assertEqual(len(events[1].sent), 2)
        self.assertEqual(events[2].sent, [])
        self.assertEqual(len(events[-1].sent), 2)

    async def test_face_image_send_failure_retries_complete_chain(self):
        plugin = make_plugin()
        raw = self._face_image_raw(339, "raw-token")

        await deliver(
            plugin, "g", "u1", "", 1,
            messages=[Face(339), Image(file="parsed", base64_data=encoded("p"))],
            raw_message=raw,
        )

        async def fail():
            raise RuntimeError("send failed")

        failed = await deliver(
            plugin, "g", "u2", "", 2, fail,
            messages=[Face(339), Image(file="parsed", base64_data=encoded("p"))],
            raw_message=raw,
        )
        retried = await deliver(
            plugin, "g", "u3", "", 3,
            messages=[Face(339), Image(file="parsed", base64_data=encoded("p"))],
            raw_message=raw,
        )

        self.assertEqual(failed.sent, [])
        self.assertEqual(len(retried.sent), 2)
        self.assertTrue(plugin._groups["g"].repeated_in_segment)

    async def test_raw_weak_file_does_not_mask_parsed_image_identity(self):
        digest = "0123456789abcdef0123456789abcdef"
        cases = (
            (lambda: Image(file=f"C:/cache/{digest}.png"), "path"),
            (lambda: Image(file="weak", path=f"C:/cache/{digest}.png"), "path"),
            (
                lambda: Image(
                    file="weak",
                    url="https://parsed.test/image?fileid=stable-image",
                ),
                "base64",
            ),
        )
        for factory, constructor in cases:
            with self.subTest(constructor=constructor, image=vars(factory())):
                plugin = make_plugin()

                async def put(sender, now, raw_token):
                    raw = self._face_image_raw(339, raw_token)
                    return await deliver(
                        plugin, "g", sender, "", now,
                        messages=[Face(339), factory()], raw_message=raw,
                    )

                await put("u1", 1, "raw-message-token-one")
                trigger = await put("u2", 2, "raw-message-token-two")

                self.assertEqual(len(trigger.sent), 2)
                self.assertEqual(trigger.sent[1].constructed_by, constructor)

    async def test_image_probe_falls_back_until_a_source_converts(self):
        plugin = make_plugin()
        bad_parsed_url = "https://parsed.test/image?rkey=secret"
        bad_raw_url = "https://raw.test/image?rkey=secret"
        good_path = "C:/cache/fallback-image.bin"
        Image.conversion_failures.update({bad_parsed_url, bad_raw_url})
        Image.conversion_payloads[good_path] = encoded("fallback-bytes")

        async def put(sender, now, raw_file):
            raw = raw_event(raw_segment(
                "image", file=raw_file, url=bad_raw_url
            ))
            parsed = Image(
                file="parsed-file", url=bad_parsed_url, path=good_path
            )
            return await deliver(
                plugin, "g", sender, "", now,
                messages=[parsed], raw_message=raw,
            )

        await put("u1", 1, "raw-one")
        trigger = await put("u2", 2, "raw-two")

        self.assertEqual(len(trigger.sent), 1)
        self.assertEqual(trigger.sent[0].constructed_by, "path")
        self.assertEqual(trigger.sent[0].converted_source, good_path)

    async def test_parsed_path_remains_a_probe_after_component_file_fails(self):
        plugin = make_plugin()
        weak_file = "message-level-weak-token"
        good_path = "C:/cache/real-image-path.bin"
        Image.conversion_failures.add(weak_file)
        Image.conversion_payloads[good_path] = encoded("real-path-bytes")

        async def put(sender, now):
            image = Image(file=weak_file, path=good_path)
            return await deliver(
                plugin, "g", sender, "", now, messages=[image]
            )

        await put("u1", 1)
        trigger = await put("u2", 2)

        self.assertEqual(len(trigger.sent), 1)
        self.assertEqual(trigger.sent[0].constructed_by, "path")
        self.assertEqual(trigger.sent[0].converted_source, good_path)

    async def test_strong_image_send_falls_back_across_outbound_sources(self):
        plugin = make_plugin()
        bad_url = "https://parsed.test/broken?fileid=stable-image"
        good_url = "https://raw.test/working?fileid=stable-image"
        Image.conversion_failures.add(bad_url)
        raw = raw_event(
            raw_segment("face", id="339"),
            raw_segment(
                "image",
                file="raw-token",
                url=good_url,
                file_unique="stable-file-unique",
            ),
        )

        async def put(sender, now):
            return await deliver(
                plugin,
                "g",
                sender,
                "",
                now,
                messages=[Face(339), Image(file="parsed-token", url=bad_url)],
                raw_message=raw,
            )

        await put("u1", 1)
        trigger = await put("u2", 2)

        self.assertEqual(len(trigger.send_attempts), 2)
        self.assertEqual(
            [attempt[1].converted_source for attempt in trigger.send_attempts],
            [bad_url, good_url],
        )
        self.assertEqual(sent_values(trigger), [
            ("face", 339),
            ("image", good_url, good_url, None),
        ])
        self.assertTrue(plugin._groups["g"].repeated_in_segment)
        self.assertEqual(plugin._groups["g"].last_repeat_time, 2)

    async def test_raw_fileid_and_token_hash_yield_to_content_identity(self):
        first_hash = "1" * 32
        second_hash = "2" * 32

        async def put(plugin, sender, now, raw_hash, fileid, content):
            url = f"https://cdn.test/image?fileid={fileid}&rkey=secret-{now}"
            raw = raw_event(raw_segment(
                "image", file=f"token/{raw_hash}.jpg", url=url
            ))
            return await deliver(
                plugin,
                "g",
                sender,
                "",
                now,
                messages=[
                    Image(
                        file="parsed-token",
                        url=url,
                        base64_data=encoded(content),
                    )
                ],
                raw_message=raw,
            )

        same = make_plugin()
        await put(same, "u1", 1, first_hash, "same-file", "same-content")
        trigger = await put(
            same, "u2", 2, second_hash, "other-fileid", "same-content"
        )
        self.assertEqual(len(trigger.sent), 1)

        different = make_plugin()
        await put(different, "u1", 1, first_hash, "same-file", "content-a")
        no_trigger = await put(
            different, "u2", 2, first_hash, "same-file", "content-b"
        )
        trigger_b = await put(
            different, "u3", 3, second_hash, "other-fileid", "content-b"
        )
        self.assertEqual(no_trigger.sent, [])
        self.assertEqual(len(trigger_b.sent), 1)

    async def test_info_diagnostics_whitelist_raw_and_parsed_type_summaries(self):
        plugin = make_plugin(threshold=7, media_diagnostics=True)
        invalid_chain = raw_event(
            raw_segment("face", id="339"),
            raw_segment(
                "image", file="secret-token", url="https://secret.test/a"
            ),
            raw_segment("text", text="secret-caption"),
        )
        unknown_chain = raw_event(
            raw_segment("image", file="secret-token-two"),
            raw_segment("private-segment", payload="secret-payload"),
        )

        await deliver(
            plugin, "secret-group", "secret-user", "", 1,
            messages=[], raw_message=invalid_chain,
        )
        await deliver(
            plugin, "secret-group", "secret-user", "", 2,
            messages=[], raw_message=unknown_chain,
        )
        state = plugin._get_group("stateful-group")
        state.threshold = 9
        await deliver(
            plugin,
            "stateful-group",
            "stateful-user",
            "",
            3,
            messages=[Image(file="parsed-only", base64_data=encoded("body"))],
        )

        info = rendered_infos()
        self.assertIn("raw_types=face,image,text", info)
        self.assertIn("raw_types=image,other", info)
        self.assertIn("parsed_types=none", info)
        self.assertIn("raw_types=none parsed_types=image", info)
        self.assertIn("threshold=7", info)
        self.assertIn("threshold=9", info)
        for secret in (
            "339", "secret-token", "https://secret.test", "secret-caption",
            "private-segment", "secret-payload", "secret-group",
            "secret-user", "stateful-group", "stateful-user", encoded("body"),
        ):
            self.assertNotIn(secret, info)

    async def test_media_diagnostics_promotes_redacted_trace_to_info(self):
        raw = raw_event(
            raw_segment("face", id="987654321"),
            raw_segment(
                "image",
                file="secret-token",
                url="https://secret.test/image?rkey=secret-rkey",
            ),
        )
        enabled = make_plugin(media_diagnostics=True)
        await deliver(
            enabled, "secret-group", "secret-user-one", "secret-body", 1,
            messages=[
                Face(987654321),
                Image(
                    file="parsed-secret",
                    base64_data=encoded("secret-image-bytes"),
                ),
            ],
            raw_message=raw,
        )

        async def fail():
            raise RuntimeError("secret-send-error")

        await deliver(
            enabled, "secret-group", "secret-user-two", "secret-body", 2,
            send_hook=fail,
            messages=[
                Face(987654321),
                Image(
                    file="parsed-secret",
                    base64_data=encoded("secret-image-bytes"),
                ),
            ],
            raw_message=raw,
        )

        info = rendered_infos()
        self.assertIn("[repeater.media]", info)
        self.assertIn("source=raw", info)
        self.assertIn("strategy=chain-", info)
        self.assertIn("fp=", info)
        self.assertIn("count=", info)
        self.assertIn("threshold=2", info)
        self.assertIn("result=send-error", info)
        self.assertIn("error=RuntimeError", info)
        for secret in (
            "987654321", "secret-token", "https://secret.test",
            "secret-rkey", "secret-group", "secret-user-one",
            "secret-user-two", "secret-body", "secret-send-error",
            encoded("secret-image-bytes"),
        ):
            self.assertNotIn(secret, info)

        LOGGER.infos.clear()
        disabled = make_plugin()
        await deliver(
            disabled, "g", "u", "", 2,
            messages=[Image(file="quiet", base64_data=encoded("quiet"))],
        )
        self.assertEqual(LOGGER.infos, [])

    async def test_raw_image_repeats_when_parsed_chain_is_empty(self):
        cases = (
            raw_event(
                raw_segment(
                    "image",
                    file="message-token",
                    url="https://example.test/raw.png?fileid=raw-stable",
                    file_unique="abc123",
                )
            ),
            SimpleNamespace(
                self_id="bot",
                message=[
                    SimpleNamespace(
                        type="image",
                        data=SimpleNamespace(
                            file="message-token",
                            url="https://example.test/raw.png?fileid=raw-stable",
                            file_unique="abc123",
                        ),
                    )
                ],
            ),
        )
        for raw in cases:
            with self.subTest(raw_type=type(raw).__name__):
                plugin = make_plugin()
                await deliver(
                    plugin, "g", "u1", "", 1, messages=[], raw_message=raw
                )
                trigger = await deliver(
                    plugin, "g", "u2", "", 2, messages=[], raw_message=raw
                )

                self.assertEqual(len(trigger.sent), 1)
                self.assertEqual(trigger.sent[0].constructed_by, "url")

    async def test_raw_image_allows_only_blank_text_reply_and_self_at_helpers(self):
        raw = raw_event(
            raw_segment("reply", id="100"),
            raw_segment("text", text=" \n"),
            raw_segment("at", qq="bot"),
            raw_segment(
                "image",
                file="opaque",
                url="https://example.test/helper.png?fileid=helper-stable",
                file_unique="helper-image",
            ),
        )
        parsed = [Reply("100"), Plain(" \n"), At("bot")]
        plugin = make_plugin()

        await deliver(
            plugin, "g", "u1", "", 1, messages=parsed, raw_message=raw
        )
        trigger = await deliver(
            plugin, "g", "u2", "", 2, messages=parsed, raw_message=raw
        )

        self.assertEqual(len(trigger.sent), 1)

    async def test_raw_media_rejects_caption_other_at_and_multiple_media(self):
        invalid_raw_messages = (
            raw_event(
                raw_segment("image", file="a", url="https://e.test/a.png"),
                raw_segment("text", text="caption-secret"),
            ),
            raw_event(
                raw_segment("image", file="a", url="https://e.test/a.png"),
                raw_segment("at", qq="someone-else"),
            ),
            raw_event(
                raw_segment("image", file="a", url="https://e.test/a.png"),
                raw_segment("mface", emoji_package_id="1", emoji_id="2"),
            ),
        )
        for raw in invalid_raw_messages:
            with self.subTest(raw=raw):
                plugin = make_plugin()
                event = await deliver(
                    plugin, "g", "u1", "", 1, messages=[], raw_message=raw
                )
                self.assertEqual(event.sent, [])
                self.assertEqual(plugin._groups, {})

    async def test_market_image_and_legacy_mface_share_package_emoji_identity(self):
        plugin = make_plugin()
        market_image = raw_event(
            raw_segment(
                "image",
                file="market-token-one",
                url="https://example.test/market.gif?rkey=one",
                emoji_package_id="88",
                emoji_id="99",
                key="rotating-key-one",
                summary="[商城表情]",
            )
        )
        legacy_mface = raw_event(
            raw_segment(
                "mface",
                emoji_package_id="88",
                emoji_id="99",
                key="rotating-key-two",
                summary="[商城表情]",
            )
        )
        bot = FakeDirectBot()

        await deliver(
            plugin,
            "g",
            "u1",
            "",
            1,
            messages=[Image(file="market-token-one")],
            raw_message=market_image,
            bot=bot,
        )
        trigger = await deliver(
            plugin,
            "g",
            "u2",
            "",
            2,
            messages=[],
            raw_message=legacy_mface,
            bot=bot,
        )

        self.assertEqual(trigger.sent, [])
        self.assertEqual(len(bot.send_group_calls), 1)

    async def test_market_emoji_id_is_stable_without_package_id(self):
        plugin = make_plugin()
        bot = FakeDirectBot()
        first = raw_event(
            raw_segment("mface", emoji_id="99", key="rotating-key-one")
        )
        second = raw_event(
            raw_segment("mface", emoji_id="99", key="rotating-key-two")
        )

        await deliver(
            plugin,
            "g",
            "u1",
            "",
            1,
            messages=[],
            raw_message=first,
            bot=bot,
        )
        await deliver(
            plugin,
            "g",
            "u2",
            "",
            2,
            messages=[],
            raw_message=second,
            bot=bot,
        )

        self.assertEqual(len(bot.send_group_calls), 1)

    async def test_legacy_mface_uses_whitelisted_direct_payload_then_call_action(self):
        raw = raw_event(
            raw_segment(
                "mface",
                emoji_package_id="10",
                emoji_id="20",
                key="private-key",
                summary="[动画表情]",
                url="https://example.test/fallback.gif?secret=yes",
                ignored="do-not-forward",
            )
        )
        bots = (FakeDirectBot(), FakeActionBot())
        for bot in bots:
            with self.subTest(bot=type(bot).__name__):
                plugin = make_plugin()
                await deliver(
                    plugin,
                    "g",
                    "u1",
                    "",
                    1,
                    messages=[],
                    raw_message=raw,
                    bot=bot,
                )
                await deliver(
                    plugin,
                    "g",
                    "u2",
                    "",
                    2,
                    messages=[],
                    raw_message=raw,
                    bot=bot,
                )

                if isinstance(bot, FakeDirectBot):
                    self.assertEqual(len(bot.send_group_calls), 1)
                    payload = bot.send_group_calls[0]
                    self.assertEqual(bot.action_calls, [])
                else:
                    self.assertEqual(len(bot.action_calls), 1)
                    action, payload = bot.action_calls[0]
                    self.assertEqual(action, "send_group_msg")
                self.assertEqual(payload["group_id"], "g")
                self.assertEqual(payload["self_id"], "bot")
                self.assertEqual(payload["message"][0]["type"], "mface")
                self.assertEqual(
                    set(payload["message"][0]["data"]),
                    {"emoji_package_id", "emoji_id", "key", "summary"},
                )

    async def test_legacy_mface_without_direct_api_uses_gif_image(self):
        raw = raw_event(
            raw_segment(
                "mface",
                emoji_package_id="10",
                emoji_id="20",
                key="private-key",
                url="https://example.test/fallback.gif",
            )
        )
        plugin = make_plugin()

        await deliver(
            plugin,
            "g",
            "u1",
            "",
            1,
            messages=[],
            raw_message=raw,
            bot=object(),
        )
        trigger = await deliver(
            plugin,
            "g",
            "u2",
            "",
            2,
            messages=[],
            raw_message=raw,
            bot=object(),
        )

        self.assertEqual(len(trigger.sent), 1)
        self.assertEqual(trigger.sent[0].constructed_by, "url")

    async def test_mface_failed_send_retries_then_repeats_once_in_segment(self):
        raw = raw_event(
            raw_segment(
                "mface",
                emoji_package_id="10",
                emoji_id="20",
                key="private-key",
            )
        )
        plugin = make_plugin()
        bot = FakeDirectBot(failures=1)

        await deliver(
            plugin, "g", "u1", "", 1, messages=[], raw_message=raw, bot=bot
        )
        await deliver(
            plugin, "g", "u2", "", 2, messages=[], raw_message=raw, bot=bot
        )
        await deliver(
            plugin, "g", "u3", "", 3, messages=[], raw_message=raw, bot=bot
        )
        await deliver(
            plugin, "g", "u4", "", 4, messages=[], raw_message=raw, bot=bot
        )

        self.assertEqual(len(bot.send_group_calls), 2)
        self.assertEqual(bot.action_calls, [])
        self.assertIn("g", plugin._groups)
        self.assertTrue(plugin._groups["g"].repeated_in_segment)

    async def test_opaque_images_use_base64_content_digest(self):
        plugin = make_plugin()
        first = Image(
            file="opaque-token-one", base64_data=encoded("same-content")
        )
        triggering = Image(
            file="opaque-token-two", base64_data=encoded("same-content")
        )

        await deliver(plugin, "g", "u1", "", 1, messages=[first])
        trigger = await deliver(
            plugin, "g", "u2", "", 2, messages=[triggering]
        )

        self.assertEqual(len(trigger.sent), 1)
        self.assertEqual(trigger.sent[0].constructed_by, "base64")

        other = make_plugin()
        different_one = Image(
            file="opaque-a", base64_data=encoded("content-a")
        )
        different_two = Image(
            file="opaque-b", base64_data=encoded("content-b")
        )
        await deliver(other, "g", "u1", "", 1, messages=[different_one])
        no_trigger = await deliver(
            other, "g", "u2", "", 2, messages=[different_two]
        )
        self.assertEqual(no_trigger.sent, [])
        self.assertNotEqual(
            other._groups["g"].current_fingerprint,
            "image:value:opaque-b",
        )

    async def test_dynamic_urls_with_same_bytes_share_content_identity(self):
        plugin = make_plugin()
        content = encoded(b"same-real-image-bytes")
        first = Image(
            file="token-one",
            url="https://cdn-one.test/download?rkey=one",
            base64_data=content,
        )
        second = Image(
            file="token-two",
            url="https://cdn-two.test/other?rkey=two",
            base64_data=content,
        )

        await deliver(plugin, "g", "u1", "", 1, messages=[first])
        trigger = await deliver(plugin, "g", "u2", "", 2, messages=[second])

        self.assertEqual(len(trigger.sent), 1)
        self.assertEqual(trigger.sent[0].base64_data, content)

    async def test_parsed_file_identity_ignores_rotating_raw_fileid(self):
        plugin = make_plugin()

        async def put(sender, now, fileid):
            raw = raw_event(raw_segment(
                "image",
                file=f"raw-token-{now}",
                url=f"https://raw.test/image?fileid={fileid}&rkey=secret-{now}",
            ))
            return await deliver(
                plugin,
                "g",
                sender,
                "",
                now,
                messages=[Image(file="stable-picture.png")],
                raw_message=raw,
            )

        await put("u1", 1, "rotating-one")
        trigger = await put("u2", 2, "rotating-two")

        self.assertEqual(len(trigger.sent), 1)
        self.assertIn("parsed-file", plugin._groups["g"].current_fingerprint)

    async def test_short_parsed_file_and_path_outrank_raw_urls(self):
        cases = (
            (lambda: Image(file="stable-relative.png"), "parsed-file"),
            (
                lambda: Image(file=None, path="C:/pictures/stable-local.png"),
                "parsed-path",
            ),
        )
        for factory, strategy in cases:
            with self.subTest(strategy=strategy):
                plugin = make_plugin()

                async def put(sender, now):
                    raw = raw_event(raw_segment(
                        "image",
                        file=f"raw-{now}",
                        url=(
                            "https://raw.test/image?"
                            f"fileid=rotating-{now}&rkey=secret-{now}"
                        ),
                    ))
                    return await deliver(
                        plugin,
                        "g",
                        sender,
                        "",
                        now,
                        messages=[factory()],
                        raw_message=raw,
                    )

                await put("u1", 1)
                trigger = await put("u2", 2)
                self.assertEqual(len(trigger.sent), 1)
                self.assertIn(strategy, plugin._groups["g"].current_fingerprint)

    async def test_temporary_parsed_tokens_use_content_bytes_for_identity(self):
        token_one = "temporary-message-token-" + "A" * 400
        token_two = "temporary-message-token-" + "B" * 400
        same_content = encoded("same-real-picture")
        same = make_plugin()

        await deliver(
            same,
            "g",
            "u1",
            "",
            1,
            messages=[Image(file=token_one, base64_data=same_content)],
        )
        trigger = await deliver(
            same,
            "g",
            "u2",
            "",
            2,
            messages=[Image(file=token_two, base64_data=same_content)],
        )
        self.assertEqual(len(trigger.sent), 1)
        self.assertIn("image:content:", same._groups["g"].current_fingerprint)

        different = make_plugin()
        await deliver(
            different,
            "g",
            "u1",
            "",
            1,
            messages=[Image(file=token_one, base64_data=encoded("picture-a"))],
        )
        no_trigger = await deliver(
            different,
            "g",
            "u2",
            "",
            2,
            messages=[Image(file=token_two, base64_data=encoded("picture-b"))],
        )
        self.assertEqual(no_trigger.sent, [])

    async def test_failed_conversion_accepts_only_confirmed_stable_identity(self):
        stable = make_plugin(threshold=3)
        stable_file = "confirmed-stable.png"
        Image.conversion_failures.add(stable_file)
        await deliver(
            stable,
            "stable-group",
            "u1",
            "",
            1,
            messages=[Image(file=stable_file)],
        )
        self.assertIn(
            "parsed-file", stable._groups["stable-group"].current_fingerprint
        )

        weak = make_plugin(threshold=3, media_diagnostics=True)
        weak_token = "temporary-opaque-token-" + "X" * 400
        Image.conversion_failures.add(weak_token)
        await deliver(
            weak,
            "weak-group",
            "u1",
            "",
            1,
            messages=[Image(file=weak_token)],
        )
        self.assertIsNone(weak._groups["weak-group"].current_fingerprint)
        diagnostics = rendered_infos()
        self.assertIn("reason=probe-failed", diagnostics)
        self.assertNotIn(weak_token, diagnostics)

    async def test_base64_forms_hash_decoded_bytes_and_send_canonical_bare(self):
        plugin = make_plugin(threshold=3)
        raw_bytes = b"\x00same image bytes\xff"
        canonical = encoded(raw_bytes)
        forms = (
            f" {canonical[:8]} \n{canonical[8:]} ",
            f"base64://{canonical}",
            f"data:image/png;base64,{canonical[:5]} \r\n{canonical[5:]}",
        )
        events = []
        for index, form in enumerate(forms):
            events.append(
                await deliver(
                    plugin,
                    "g",
                    f"u{index}",
                    "",
                    index,
                    messages=[
                        Image(file=f"opaque-{index}", base64_data=form)
                    ],
                )
            )

        self.assertEqual(len(events[-1].sent), 1)
        output = events[-1].sent[0]
        self.assertEqual(output.base64_data, canonical)
        self.assertEqual(output.file, f"base64://{canonical}")
        expected = hashlib.sha256(raw_bytes).hexdigest()
        self.assertEqual(
            plugin._groups["g"].current_fingerprint,
            f"image:content:{expected}",
        )

        different = make_plugin()
        await deliver(
            different,
            "g",
            "u1",
            "",
            1,
            messages=[Image(file="opaque-a", base64_data=encoded(b"a"))],
        )
        no_trigger = await deliver(
            different,
            "g",
            "u2",
            "",
            2,
            messages=[
                Image(
                    file="opaque-b",
                    base64_data=f"data:image/gif;base64,{encoded(b'b')}",
                )
            ],
        )
        self.assertEqual(no_trigger.sent, [])

    async def test_skip_diagnostics_cover_raw_and_parsed_reasons_redacted(self):
        invalid = (
            raw_event(
                raw_segment("image", file="secret-token-one"),
                raw_segment("mface", key="secret-key-one"),
            ),
            raw_event(
                raw_segment(
                    "image",
                    url="https://secret.test/a?rkey=secret-rkey",
                ),
                raw_segment("text", text="secret-body"),
            ),
            raw_event(
                raw_segment("image", file="secret-token-two"),
                raw_segment("at", qq="secret-other-user"),
            ),
            raw_event(raw_segment("mface", summary="secret-summary")),
            raw_event(raw_segment("image")),
        )
        for index, raw in enumerate(invalid):
            await deliver(
                make_plugin(),
                "g",
                f"u{index}",
                "",
                index,
                messages=[],
                raw_message=raw,
            )
        await deliver(
            make_plugin(), "g", "parsed", "", 10, messages=[Image()]
        )

        diagnostics = rendered_debugs()
        for reason in (
            "multiple-media",
            "nonblank-text",
            "other-at",
            "missing-identity",
            "missing-source",
        ):
            self.assertIn(f"reason={reason}", diagnostics)
        self.assertIn("source=raw", diagnostics)
        self.assertIn("source=parsed", diagnostics)
        for secret in (
            "secret-token",
            "https://secret.test",
            "secret-rkey",
            "secret-key",
            "secret-body",
            "secret-summary",
            "secret-other-user",
        ):
            self.assertNotIn(secret, diagnostics)

    async def test_stable_parsed_path_is_rebuilt_for_send(self):
        plugin = make_plugin()
        Image.conversion_failures.update(
            {"message-token-one", "message-token-two"}
        )
        first = Image(file="message-token-one", path="/tmp/no-hash.png")
        triggering = Image(
            file="message-token-two",
            path="/tmp/no-hash.png",
        )

        await deliver(plugin, "g", "u1", "", 1, messages=[first])
        trigger = await deliver(
            plugin, "g", "u2", "", 2, messages=[triggering]
        )

        self.assertEqual(len(trigger.sent), 1)
        self.assertEqual(trigger.sent[0].constructed_by, "path")
        self.assertEqual(trigger.sent[0].converted_source, "/tmp/no-hash.png")

    async def test_media_digest_cache_is_bounded_and_expires(self):
        plugin = make_plugin(threshold=50)
        cache_content = encoded("cache-content")
        first = Image(file="opaque-cache", base64_data=cache_content)
        cached = Image(file="opaque-cache", base64_data=cache_content)
        expired = Image(file="opaque-cache", base64_data=cache_content)

        await deliver(plugin, "g", "u1", "", 0, messages=[first])
        await deliver(plugin, "g", "u2", "", 1, messages=[cached])
        await deliver(plugin, "g", "u3", "", 301, messages=[expired])

        self.assertEqual(first.convert_calls, 1)
        self.assertEqual(cached.convert_calls, 0)
        self.assertEqual(expired.convert_calls, 1)

        bounded = make_plugin(threshold=50)
        for index in range(130):
            await deliver(
                bounded,
                "g",
                f"u{index}",
                "",
                index,
                messages=[
                    Image(
                        file=f"opaque-{index}",
                        base64_data=encoded(f"content-{index}"),
                    )
                ],
            )
        self.assertLessEqual(len(bounded._media_digest_cache), 128)

    async def test_different_url_queries_share_equal_content_identity(self):
        plugin = make_plugin()
        first = Image(
            file="opaque-one",
            url="https://e.test/d?appid=1&fileid=2&rkey=secret-one",
            base64_data=encoded("same-image-content"),
        )
        second = Image(
            file="opaque-two",
            url="https://e.test/d?rkey=secret-two&fileid=2&appid=1",
            base64_data=encoded("same-image-content"),
        )

        await deliver(plugin, "g", "u1", "", 1, messages=[first])
        trigger = await deliver(plugin, "g", "u2", "", 2, messages=[second])

        self.assertEqual(len(trigger.sent), 1)

    async def test_media_debug_diagnostics_are_redacted(self):
        raw = raw_event(
            raw_segment(
                "image",
                file="complete-private-token",
                url=(
                    "https://example.test/private.gif?"
                    "fileid=private-fileid&rkey=private-rkey"
                ),
                file_unique="private-file-unique",
                key="private-market-key",
            )
        )
        plugin = make_plugin()

        await deliver(
            plugin, "g", "u1", "private-body", 1, messages=[], raw_message=raw
        )

        rendered = []
        for args in LOGGER.debugs:
            rendered.append(str(args[0]) % tuple(args[1:]))
        diagnostics = "\n".join(rendered)
        self.assertIn("[repeater.media]", diagnostics)
        self.assertIn("source=raw", diagnostics)
        self.assertNotIn("complete-private-token", diagnostics)
        self.assertNotIn("https://example.test", diagnostics)
        self.assertNotIn("private-rkey", diagnostics)
        self.assertNotIn("private-file-unique", diagnostics)
        self.assertNotIn("private-market-key", diagnostics)
        self.assertNotIn("private-body", diagnostics)


if __name__ == "__main__":
    unittest.main()
