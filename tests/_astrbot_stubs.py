"""Minimal AstrBot modules needed to import the plugin in unit tests."""

from __future__ import annotations

import sys
import types


class _Logger:
    def __init__(self):
        self.warnings: list[tuple[object, ...]] = []

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        self.warnings.append(args)


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


class Image:
    def __init__(self, file=None, url=None, path=None):
        self.file = file
        self.url = url
        self.path = path


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
