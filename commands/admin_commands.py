from __future__ import annotations

import discord
from discord.ext import commands

from database import Database


def register_admin_commands(bot: commands.Bot, db: Database) -> None:
    @bot.tree.command(name="ping", description="Check if the bot is online.")
    async def ping(interaction: discord.Interaction) -> None:
        await interaction.response.send_message("Pong!")

    @bot.tree.command(name="healthcheck", description="Check bot and database health.")
    async def healthcheck(interaction: discord.Interaction) -> None:
        try:
            result = db.healthcheck()
            server_time = result.get("server_time", "unknown")
            await interaction.response.send_message(
                f"✅ Bot is online. Database connected. Server time: {server_time}"
            )
        except Exception as exc:
            await interaction.response.send_message(
                f"❌ Database healthcheck failed: {exc}"
            )
