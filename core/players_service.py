from __future__ import annotations

from typing import Any, Optional

from core.madden_queries import (
    get_player_by_roster_id,
    search_players,
    safe_int,
    safe_text,
)
from database import Database


def build_player_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "roster_id": safe_int(row.get("roster_id")),
        "full_name": safe_text(row.get("full_name"), "Unknown Player"),
        "position": safe_text(row.get("position"), "N/A"),
        "age": safe_int(row.get("age")),
        "team_name": safe_text(row.get("team_name"), "Free Agent"),
        "overall": safe_int(row.get("resolved_overall")),
        "dev_trait": safe_text(row.get("resolved_dev_trait"), "Unknown"),
    }


def format_player_search_lines(rows: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for idx, row in enumerate(rows, start=1):
        player = build_player_summary(row)
        lines.append(
            f"{idx}. {player['full_name']} — {player['position']} | "
            f"{player['team_name']} | {player['overall']} OVR | {player['dev_trait']}"
        )
    return lines


def find_players(db: Database, query: str, limit: int = 10) -> list[dict[str, Any]]:
    if not safe_text(query):
        return []
    rows = search_players(db, query, limit=limit)
    return [build_player_summary(row) for row in rows]


def get_player_profile(db: Database, query: str) -> dict[str, Any]:
    rows = search_players(db, query, limit=10)

    if not rows:
        return {
            "status": "not_found",
            "query": query,
            "matches": [],
            "player": None,
        }

    if len(rows) == 1:
        player = build_player_summary(rows[0])
        return {
            "status": "single",
            "query": query,
            "matches": [],
            "player": player,
        }

    exact_matches = [
        row for row in rows
        if safe_text(row.get("full_name")).lower() == safe_text(query).lower()
    ]
    if len(exact_matches) == 1:
        player = build_player_summary(exact_matches[0])
        return {
            "status": "single",
            "query": query,
            "matches": [],
            "player": player,
        }

    return {
        "status": "multiple",
        "query": query,
        "matches": [build_player_summary(row) for row in rows],
        "player": None,
    }


def get_player_profile_by_roster_id(db: Database, roster_id: int) -> Optional[dict[str, Any]]:
    row = get_player_by_roster_id(db, roster_id)
    if not row:
        return None
    return build_player_summary(row)