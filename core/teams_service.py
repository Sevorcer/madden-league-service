from __future__ import annotations

from core.madden_queries import (
    fetch_team_roster_rows,
    fetch_team_standing,
    resolve_dev_trait_label,
    resolve_display_overall,
    resolve_team_row,
    safe_float,
    safe_int,
    safe_text,
)
from database import Database


def get_team_payload(db: Database, team_name: str) -> dict[str, object]:
    team = resolve_team_row(db, team_name)
    if not team:
        return {"status": "not_found", "team": None}

    standing = fetch_team_standing(db, safe_int(team.get("team_id"))) or {}
    return {
        "status": "ok",
        "team": {
            "team_id": safe_int(team.get("team_id")),
            "team_name": safe_text(team.get("team_name"), "Unknown Team"),
            "team_ovr": safe_int(team.get("team_ovr")),
            "conference_name": safe_text(team.get("conference_name"), "N/A"),
            "division_name": safe_text(team.get("division_name"), "N/A"),
            "wins": safe_int(standing.get("wins")),
            "losses": safe_int(standing.get("losses")),
            "ties": safe_int(standing.get("ties")),
            "win_pct": safe_float(standing.get("win_pct")),
            "seed": safe_int(standing.get("seed")),
            "pts_for": safe_int(standing.get("pts_for")),
            "pts_against": safe_int(standing.get("pts_against")),
            "turnover_diff": safe_int(standing.get("turnover_diff")),
        },
    }


def get_team_roster_payload(db: Database, team_name: str) -> dict[str, object]:
    team = resolve_team_row(db, team_name)
    if not team:
        return {"status": "not_found", "team": None, "roster": []}

    roster_rows = fetch_team_roster_rows(db, safe_int(team.get("team_id")))
    roster = []
    for row in roster_rows:
        roster.append(
            {
                "full_name": safe_text(row.get("full_name"), "Unknown Player"),
                "position": safe_text(row.get("position"), "N/A"),
                "age": safe_int(row.get("age")),
                "overall": resolve_display_overall(row),
                "dev_trait": resolve_dev_trait_label(row),
            }
        )

    return {
        "status": "ok",
        "team": {
            "team_id": safe_int(team.get("team_id")),
            "team_name": safe_text(team.get("team_name"), "Unknown Team"),
        },
        "roster": roster,
    }