from __future__ import annotations

from core.madden_queries import fetch_stat_leaders, safe_int, safe_text
from database import Database


def get_leaders_payload(db: Database, category: str, limit: int = 10) -> dict:
    rows = fetch_stat_leaders(db, category, limit=limit)
    return {
        "category": category,
        "rows": [
            {
                "player_name": safe_text(row.get("player_name"), "Unknown"),
                "team_name": safe_text(row.get("team_name"), "Unknown Team"),
                "stat_value": safe_int(row.get("stat_value")),
            }
            for row in rows
        ],
    }
