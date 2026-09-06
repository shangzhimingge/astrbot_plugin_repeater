"""Minimal AstrBot modules needed to import the plugin in unit tests."""

from __future__ import annotations

import base64
import sys
import types


class _Logger:
    def __init__(self):
        self.warnings: list[tuple[object, ...]] = []
        self.debugs: list[tuple[object, ...]] = []
        self.infos: list[tuple[object, ...]] = []

    def info(self, *args, **kwargs):
        self.infos.append(args)

    def warning(self, *args, **kwargs):
        self.warnings.append(args)

    def debug(self, *args, **kwargs):
        self.debugs.append(args)


class _CommandGroup:
    def command(self, _name):
        return lambda func: func


class _Filter:
    class EventMessageType:
        GROUP_MESSAGE = "group"

    class PlatformAdapterType:
        AIOCQHTTP = "aiocqhttp"

    class PermissionType:
        ADMIN = "admin"

    def command_group(self, _name):
        def decorate(func):
            func.command = _CommandGroup().command
            return func

        return decorate

    def event_message_type(self, _kind):
        return lambda func: func

    def platform_adapter_type(self, _kind):
        return lambda func: func

    def permission_type(self, _kind):
        return lambda func: func


class Plain:
    def __init__(self, text):
        self.text = text


class Face:
    def __init__(self, id):
        self.id = id


class At:
    def __init__(self, qq):
        self.qq = qq


class Reply:
    def __init__(self, id):
        self.id = id


class Image:
    conversion_failures: set[str] = set()
    conversion_payloads: dict[str, str] = {}

    def __init__(
        self,
        file=None,
        url=None,
        path=None,
        file_unique=None,
        base64_data=None,
    ):
        self.file = file
        self.url = url
        self.path = path
        self.file_unique = file_unique
        self.base64_data = base64_data
        self.constructed_by = "direct"
        self.converted_source = None
        self.convert_calls = 0

    @classmethod
    def fromURL(cls, url):
        image = cls(file=url, url=url)
        image.constructed_by = "url"
        return image

    @classmethod
    def fromFileSystem(cls, path):
        image = cls(file=path, path=path)
        image.constructed_by = "path"
        return image

    @classmethod
    def fromBase64(cls, base64_data):
        image = cls(file=f"base64://{base64_data}", base64_data=base64_data)
        image.constructed_by = "base64"
        return image

    async def convert_to_base64(self):
        """Record the source the aiocqhttp adapter would try to convert."""
        self.convert_calls += 1
        self.converted_source = self.url or self.path or self.file
        if self.converted_source in self.conversion_failures:
            raise RuntimeError("image probe failed")
        if self.converted_source in self.conversion_payloads:
            return self.conversion_payloads[self.converted_source]
        if self.base64_data is not None:
            return self.base64_data
        if self.converted_source is None:
            return None
        return base64.b64encode(str(self.converted_source).encode()).decode()


class MessageChain(list):
    pass


class Star:
    def __init__(self, context):
        self.context = context


def install():
    existing = sys.modules.get("astrbot.api")
    if existing is not None and hasattr(existing, "logger"):
        return existing.logger

    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    event = types.ModuleType("astrbot.api.event")
    components = types.ModuleType("astrbot.api.message_components")
    star = types.ModuleType("astrbot.api.star")

    api.AstrBotConfig = dict
    api.logger = _Logger()
    event.AstrMessageEvent = object
    event.MessageChain = MessageChain
    event.filter = _Filter()
    components.Plain = Plain
    components.Face = Face
    components.At = At
    components.Reply = Reply
    components.Image = Image
    star.Context = object
    star.Star = Star

    astrbot.api = api
    api.event = event
    api.message_components = components
    api.star = star
    sys.modules.update(
        {
            "astrbot": astrbot,
            "astrbot.api": api,
            "astrbot.api.event": event,
            "astrbot.api.message_components": components,
            "astrbot.api.star": star,
        }
    )
    return api.logger
