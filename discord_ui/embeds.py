from __future__ import annotations

import discord


def ok_embed(title: str, description: str) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=0x2ECC71)


def error_embed(message: str) -> discord.Embed:
    return discord.Embed(title="Error", description=message, color=0xE74C3C)


def info_embed(title: str, description: str) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=0x3498DB)
