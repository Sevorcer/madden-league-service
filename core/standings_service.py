from __future__ import annotations

from core.madden_queries import fetch_standings_rows, safe_float, safe_int, safe_text
from database import Database


def get_standings_payload(
    db: Database,
    conference_filter: str = "",
    division_filter: str = "",
) -> dict:
    rows = fetch_standings_rows(db)
    conference_filter = safe_text(conference_filter).lower()
    division_filter = safe_text(division_filter).lower()

    filtered = []
    for row in rows:
        conference_name = safe_text(row.get("conference_name"))
        division_name = safe_text(row.get("division_name"))
        if conference_filter and conference_filter not in conference_name.lower():
            continue
        if division_filter and division_filter not in division_name.lower():
            continue
        filtered.append(
            {
                "team_name": safe_text(row.get("team_name"), "Unknown Team"),
                "conference_name": conference_name,
                "division_name": division_name,
                "team_ovr": safe_int(row.get("team_ovr")),
                "wins": safe_int(row.get("wins")),
                "losses": safe_int(row.get("losses")),
                "ties": safe_int(row.get("ties")),
                "win_pct": safe_float(row.get("win_pct")),
                "seed": safe_int(row.get("seed")),
                "pts_for": safe_int(row.get("pts_for")),
                "pts_against": safe_int(row.get("pts_against")),
                "turnover_diff": safe_int(row.get("turnover_diff")),
            }
        )

    return {
        "rows": filtered,
        "conference_filter": conference_filter,
        "division_filter": division_filter,
    }
