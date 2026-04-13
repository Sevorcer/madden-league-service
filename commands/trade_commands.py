from __future__ import annotations

import discord
from discord.ext import commands

from database import Database


def register_trade_commands(bot: commands.Bot, db: Database) -> None:
    async def coming_soon(interaction: discord.Interaction, feature: str) -> None:
        await interaction.response.send_message(
            f"**{feature}** is scaffolded in V1 but not wired yet in this command pack."
        )

    @bot.tree.command(name="submittrade", description="Submit a trade for committee review.")
    async def submittrade(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Submit trade")

    @bot.tree.command(name="tradestatus", description="Check the status of a submitted trade.")
    async def tradestatus(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Trade status")

    @bot.tree.command(name="forcetradeapprove", description="Force approve a trade.")
    async def forcetradeapprove(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Force trade approve")

    @bot.tree.command(name="forcetradedeny", description="Force deny a trade.")
    async def forcetradedeny(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Force trade deny")
