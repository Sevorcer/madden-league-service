from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

from database import Database
from config import settings
from core.players_service import get_player_profile

db = Database(settings)


def register_league_commands(bot: commands.Bot) -> None:
    @bot.tree.command(name="player", description="Look up a player by name.")
    @app_commands.describe(name="The player name to search for")
    async def player(interaction: discord.Interaction, name: str) -> None:
        await interaction.response.defer(thinking=True)

        try:
            result = get_player_profile(db, name)

            if result["status"] == "not_found":
                await interaction.followup.send(
                    f"No player found matching **{name}**."
                )
                return

            if result["status"] == "multiple":
                matches = result["matches"][:10]
                lines = []
                for idx, row in enumerate(matches, start=1):
                    lines.append(
                        f"{idx}. {row['full_name']} — {row['position']} | "
                        f"{row['team_name']} | {row['overall']} OVR | {row['dev_trait']}"
                    )

                embed = discord.Embed(
                    title=f"Multiple players found for: {name}",
                    description="\n".join(lines),
                    color=0xF1C40F,
                )
                embed.set_footer(text="Try a more specific player name.")
                await interaction.followup.send(embed=embed)
                return

            player_row = result["player"]
            embed = discord.Embed(
                title=player_row["full_name"],
                description=(
                    f"**Position:** {player_row['position']}\n"
                    f"**Team:** {player_row['team_name']}\n"
                    f"**Overall:** {player_row['overall']}\n"
                    f"**Dev Trait:** {player_row['dev_trait']}\n"
                    f"**Age:** {player_row['age']}"
                ),
                color=0x3498DB,
            )
            await interaction.followup.send(embed=embed)

        except Exception as exc:
            await interaction.followup.send(
                f"Error loading player data: {exc}"
            )