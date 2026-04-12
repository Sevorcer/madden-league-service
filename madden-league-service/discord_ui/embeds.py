from __future__ import annotations

import discord


def ok_embed(title: str, description: str) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=0x57F287)


def error_embed(message: str) -> discord.Embed:
    return discord.Embed(title="Error", description=message, color=0xED4245)


def info_embed(title: str, description: str) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=0x5865F2)
