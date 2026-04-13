from __future__ import annotations

import discord
from discord.ext import commands

from database import Database


def register_ops_commands(bot: commands.Bot, db: Database) -> None:
    async def coming_soon(interaction: discord.Interaction, feature: str) -> None:
        await interaction.response.send_message(
            f"**{feature}** is scaffolded in V1 but not wired yet in this command pack."
        )

    @bot.tree.command(name="createweekchannels", description="Create weekly game channels.")
    async def createweekchannels(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Create week channels")

    @bot.tree.command(name="createbounty", description="Create a new bounty.")
    async def createbounty(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Create bounty")

    @bot.tree.command(name="bounties", description="List active bounties.")
    async def bounties(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Bounties")

    @bot.tree.command(name="claimbounty", description="Claim a bounty.")
    async def claimbounty(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Claim bounty")

    @bot.tree.command(name="updatebounty", description="Update bounty reward.")
    async def updatebounty(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Update bounty")
