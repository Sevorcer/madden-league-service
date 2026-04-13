from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

from core.leaders_service import get_leaders_payload
from core.players_service import get_player_search_payload
from core.standings_service import get_standings_payload
from core.teams_service import get_team_payload, get_team_roster_payload
from database import Database

ROSTER_PAGE_SIZE = 12


def register_league_commands(bot: commands.Bot, db: Database) -> None:
    @bot.tree.command(name="player", description="Look up a player by name.")
    @app_commands.describe(name="The player name to search for")
    async def player(interaction: discord.Interaction, name: str) -> None:
        await interaction.response.defer(thinking=True)

        try:
            result = get_player_search_payload(db, name)

            if result["status"] == "not_found":
                await interaction.followup.send(
                    f"No player found matching **{name}**."
                )
                return

            if result["status"] == "multiple":
                matches = result["results"][:10]
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
            embed.add_field(
                name="Quick Stats",
                value=(
                    f"**SPD:** {player_row['speed']} | "
                    f"**STR:** {player_row['strength']} | "
                    f"**AWR:** {player_row['awareness']} | "
                    f"**COD:** {player_row['change_of_direction']}"
                ),
                inline=False,
            )
            await interaction.followup.send(embed=embed)

        except Exception as exc:
            await interaction.followup.send(
                f"Error loading player data: {exc}"
            )

    @bot.tree.command(name="team", description="Look up a team by name.")
    @app_commands.describe(name="The team name to search for")
    async def team(interaction: discord.Interaction, name: str) -> None:
        await interaction.response.defer(thinking=True)

        try:
            result = get_team_payload(db, name)

            if result["status"] == "not_found":
                await interaction.followup.send(
                    f"No team found matching **{name}**."
                )
                return

            row = result["team"]
            record = f"{row['wins']}-{row['losses']}-{row['ties']}"
            embed = discord.Embed(
                title=row["team_name"],
                description=(
                    f"**Conference:** {row['conference_name']}\n"
                    f"**Division:** {row['division_name']}\n"
                    f"**OVR:** {row['team_ovr']}\n"
                    f"**Record:** {record}\n"
                    f"**Points For:** {row['pts_for']}\n"
                    f"**Points Against:** {row['pts_against']}\n"
                    f"**Turnover Diff:** {row['turnover_diff']}"
                ),
                color=0x5865F2,
            )
            await interaction.followup.send(embed=embed)

        except Exception as exc:
            await interaction.followup.send(
                f"Error loading team data: {exc}"
            )

    @bot.tree.command(name="roster", description="Show a team roster.")
    @app_commands.describe(team_name="The team to show", page="Roster page number")
    async def roster(interaction: discord.Interaction, team_name: str, page: int = 1) -> None:
        await interaction.response.defer(thinking=True)

        try:
            result = get_team_roster_payload(db, team_name)

            if result["status"] == "not_found":
                await interaction.followup.send(
                    f"No team found matching **{team_name}**."
                )
                return

            roster_rows = result["roster"]
            if not roster_rows:
                await interaction.followup.send(
                    f"No roster data found for **{team_name}**."
                )
                return

            total_pages = max(1, (len(roster_rows) + ROSTER_PAGE_SIZE - 1) // ROSTER_PAGE_SIZE)
            page = max(1, min(page, total_pages))
            start_idx = (page - 1) * ROSTER_PAGE_SIZE
            end_idx = start_idx + ROSTER_PAGE_SIZE
            page_rows = roster_rows[start_idx:end_idx]

            lines = []
            for row in page_rows:
                lines.append(
                    f"**{row['full_name']}** — {row['position']} | "
                    f"{row['overall']} OVR | {row['dev_trait']}"
                )

            embed = discord.Embed(
                title=f"{result['team']['team_name']} Roster",
                description="\n".join(lines),
                color=0x2ECC71,
            )
            embed.set_footer(text=f"Page {page}/{total_pages}")
            await interaction.followup.send(embed=embed)

        except Exception as exc:
            await interaction.followup.send(
                f"Error loading roster: {exc}"
            )

    @bot.tree.command(name="standings", description="Show current league standings.")
    @app_commands.describe(
        conference="Optional conference filter, like AFC or NFC",
        division="Optional division filter",
    )
    async def standings(
        interaction: discord.Interaction,
        conference: str = "",
        division: str = "",
    ) -> None:
        await interaction.response.defer(thinking=True)

        try:
            payload = get_standings_payload(
                db,
                conference_filter=conference,
                division_filter=division,
            )
            rows = payload["rows"]

            if not rows:
                await interaction.followup.send("No standings data found.")
                return

            lines = []
            for idx, row in enumerate(rows[:20], start=1):
                record = f"{row['wins']}-{row['losses']}-{row['ties']}"
                lines.append(
                    f"{idx}. **{row['team_name']}** — {record} | "
                    f"PF {row['pts_for']} | PA {row['pts_against']} | TO {row['turnover_diff']}"
                )

            title = "League Standings"
            if conference:
                title += f" — {conference}"
            if division:
                title += f" — {division}"

            embed = discord.Embed(
                title=title,
                description="\n".join(lines),
                color=0x9B59B6,
            )
            await interaction.followup.send(embed=embed)

        except Exception as exc:
            await interaction.followup.send(
                f"Error loading standings: {exc}"
            )

    @bot.tree.command(name="leaders", description="Show league stat leaders.")
    @app_commands.describe(
        category="passing, rushing, receiving, sacks, or interceptions",
        limit="How many leaders to show",
    )
    async def leaders(interaction: discord.Interaction, category: str, limit: int = 10) -> None:
        await interaction.response.defer(thinking=True)

        try:
            payload = get_leaders_payload(db, category, limit=max(1, min(limit, 20)))
            rows = payload["rows"]

            if not rows:
                await interaction.followup.send(
                    f"No leader data found for **{category}**."
                )
                return

            lines = []
            for idx, row in enumerate(rows, start=1):
                lines.append(
                    f"{idx}. **{row['player_name']}** ({row['team_name']}) — {row['stat_value']}"
                )

            embed = discord.Embed(
                title=f"{category.title()} Leaders",
                description="\n".join(lines),
                color=0xE67E22,
            )
            await interaction.followup.send(embed=embed)

        except Exception as exc:
            await interaction.followup.send(
                f"Error loading leaders: {exc}"
            )