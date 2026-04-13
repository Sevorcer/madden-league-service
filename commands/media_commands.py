from __future__ import annotations

import discord
from discord.ext import commands

from database import Database


def register_media_commands(bot: commands.Bot, db: Database) -> None:
    async def coming_soon(interaction: discord.Interaction, feature: str) -> None:
        await interaction.response.send_message(
            f"**{feature}** is scaffolded in V1 but not wired yet in this command pack."
        )

    @bot.tree.command(name="powerrankings", description="Show current power rankings.")
    async def powerrankings(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Power rankings")

    @bot.tree.command(name="weeklynews", description="Generate weekly league news.")
    async def weeklynews(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Weekly news")

    @bot.tree.command(name="matchuppreview", description="Generate a matchup preview.")
    async def matchuppreview(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Matchup preview")

    @bot.tree.command(name="gamerecap", description="Generate a game recap.")
    async def gamerecap(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Game recap")

    @bot.tree.command(name="weeklyrivalries", description="Show weekly rivalry games.")
    async def weeklyrivalries(interaction: discord.Interaction) -> None:
        await coming_soon(interaction, "Weekly rivalries")
