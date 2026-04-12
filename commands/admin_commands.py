from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

from config import Settings
from database import Database
from discord_ui.embeds import error_embed, info_embed, ok_embed


def register_admin_commands(bot: commands.Bot, settings: Settings, db: Database) -> None:
    @bot.tree.command(name="ping", description="Check whether the bot is online.")
    async def ping(interaction: discord.Interaction) -> None:
        await interaction.response.send_message(embed=ok_embed("Pong", "Madden League Service is online."))

    @bot.tree.command(name="healthcheck", description="Check bot and database health.")
    async def healthcheck(interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        try:
            result = db.healthcheck()
            guild_scope = ", ".join(str(gid) for gid in settings.guild_ids) if settings.guild_ids else "global"
            description = (
                f"Database connected successfully.\n"
                f"Guild sync scope: `{guild_scope}`\n"
                f"Server time: `{result.get('server_time')}`"
            )
            await interaction.followup.send(embed=info_embed("Healthcheck OK", description))
        except Exception as exc:
            await interaction.followup.send(embed=error_embed(f"Healthcheck failed: {exc}"))
