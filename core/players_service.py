from __future__ import annotations

from core.madden_queries import (
    fetch_player_by_roster_id,
    fetch_player_search_results,
    resolve_dev_trait_label,
    resolve_display_overall,
    safe_int,
    safe_text,
)
from database import Database


def build_player_card_data(row: dict[str, object]) -> dict[str, object]:
    return {
        "roster_id": safe_int(row.get("roster_id")),
        "full_name": safe_text(row.get("full_name"), "Unknown Player"),
        "team_name": safe_text(row.get("team_name"), "Free Agent"),
        "position": safe_text(row.get("position"), "N/A"),
        "age": safe_int(row.get("age")),
        "overall": resolve_display_overall(row),
        "dev_trait": resolve_dev_trait_label(row),
        "speed": safe_int(row.get("speed")),
        "strength": safe_int(row.get("strength")),
        "awareness": safe_int(row.get("awareness")),
        "change_of_direction": safe_int(row.get("change_of_direction")),
    }


def get_player_search_payload(db: Database, name: str) -> dict[str, object]:
    results = fetch_player_search_results(db, name, limit=10)

    if not results:
        return {"status": "not_found", "results": [], "player": None}

    first = results[0]
    exact_match = safe_text(first.get("full_name")).lower() == name.strip().lower()

    if exact_match or len(results) == 1:
        return {
            "status": "single",
            "results": [],
            "player": build_player_card_data(first),
        }

    return {
        "status": "multiple",
        "results": [build_player_card_data(row) for row in results[:10]],
        "player": None,
    }


def get_player_by_roster_payload(db: Database, roster_id: int) -> dict[str, object] | None:
    row = fetch_player_by_roster_id(db, roster_id)
    if not row:
        return None
    return build_player_card_data(row)