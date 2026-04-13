from __future__ import annotations

from typing import Any, Optional

from database import Database

DEV_TRAIT_LABELS = {
    0: "Normal",
    1: "Star",
    2: "Superstar",
    3: "X-Factor",
}

POSITION_SORT_ORDER = {
    "QB": 1,
    "HB": 2,
    "FB": 3,
    "WR": 4,
    "TE": 5,
    "LT": 6,
    "LG": 7,
    "C": 8,
    "RG": 9,
    "RT": 10,
    "LEDGE": 11,
    "REDGE": 12,
    "LE": 13,
    "RE": 14,
    "DT": 15,
    "LOLB": 16,
    "MLB": 17,
    "ROLB": 18,
    "SAM": 19,
    "MIKE": 20,
    "WILL": 21,
    "CB": 22,
    "FS": 23,
    "SS": 24,
    "K": 25,
    "P": 26,
    "LS": 27,
}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def dev_trait_to_label(raw_value: Any, fallback_label: str = "") -> str:
    raw_int = safe_int(raw_value, -1)
    if raw_int in DEV_TRAIT_LABELS:
        return DEV_TRAIT_LABELS[raw_int]
    fallback = safe_text(fallback_label)
    return fallback or "Unknown"


def resolve_display_overall(row: dict[str, Any]) -> int:
    candidates = [
        row.get("overall_rating"),
        row.get("player_best_ovr"),
        row.get("team_ovr"),
    ]
    for candidate in candidates:
        value = safe_int(candidate, 0)
        if value > 0:
            return value
    return 0


def fetch_player_search_results(db: Database, name: str, limit: int = 10) -> list[dict[str, Any]]:
    search = f"%{safe_text(name).lower()}%"
    rows = db.fetch_all(
        """
        SELECT
            p.roster_id,
            p.full_name,
            p.position,
            p.age,
            p.overall_rating,
            p.player_best_ovr,
            p.team_id,
            t.team_name,
            t.team_ovr
        FROM players p
        LEFT JOIN teams t
            ON t.team_id = p.team_id
        WHERE LOWER(COALESCE(p.full_name, '')) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(p.full_name, '')) = LOWER(%s) THEN 0
                WHEN LOWER(COALESCE(p.full_name, '')) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            COALESCE(NULLIF(p.overall_rating, 0), p.player_best_ovr, 0) DESC,
            p.full_name ASC
        LIMIT %s
        """,
        (search, safe_text(name), f"{safe_text(name)}%", limit),
    )
    for row in rows:
        row["resolved_dev_trait_label"] = row.get("dev_trait_label")
    return rows


def fetch_player_by_roster_id(db: Database, roster_id: int) -> Optional[dict[str, Any]]:
    row = db.fetch_one(
        """
        SELECT
            p.roster_id,
            p.full_name,
            p.position,
            p.age,
            p.overall_rating,
            p.player_best_ovr,
            p.team_id,
            t.team_name,
            t.team_ovr
        FROM players p
        LEFT JOIN teams t
            ON t.team_id = p.team_id
        WHERE p.roster_id = %s
        LIMIT 1
        """,
        (roster_id,),
    )
    if row:
        row["resolved_dev_trait_label"] = row.get("dev_trait_label")
    return row


def fetch_standings_rows(db: Database) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            t.team_name,
            t.conference_name,
            t.division_name,
            t.team_ovr,
            s.wins,
            s.losses,
            s.ties,
            s.win_pct,
            s.seed,
            s.pts_for,
            s.pts_against,
            s.turnover_diff
        FROM standings s
        JOIN teams t ON t.team_id = s.team_id
        ORDER BY s.wins DESC, s.win_pct DESC, s.pts_for DESC, t.team_name ASC
        """
    )


def resolve_team_row(db: Database, team_name: str) -> Optional[dict[str, Any]]:
    search = f"%{safe_text(team_name).lower()}%"
    return db.fetch_one(
        """
        SELECT
            t.team_id,
            t.team_name,
            t.team_abbrev,
            t.team_ovr,
            t.conference_name,
            t.division_name
        FROM teams t
        WHERE LOWER(COALESCE(t.team_name, '')) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(t.team_name, '')) = LOWER(%s) THEN 0
                WHEN LOWER(COALESCE(t.team_name, '')) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            t.team_name ASC
        LIMIT 1
        """,
        (search, safe_text(team_name), f"{safe_text(team_name)}%"),
    )


def fetch_team_standing(db: Database, team_id: int) -> Optional[dict[str, Any]]:
    return db.fetch_one(
        """
        SELECT
            wins,
            losses,
            ties,
            win_pct,
            seed,
            pts_for,
            pts_against,
            turnover_diff
        FROM standings
        WHERE team_id = %s
        LIMIT 1
        """,
        (team_id,),
    )


def fetch_team_roster_rows(db: Database, team_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        """
        SELECT
            p.roster_id,
            p.full_name,
            p.position,
            p.age,
            p.overall_rating,
            p.player_best_ovr,
            p.team_id,
            t.team_name,
            t.team_ovr
        FROM players p
        JOIN teams t ON t.team_id = p.team_id
        WHERE p.team_id = %s
        ORDER BY
            COALESCE(NULLIF(p.overall_rating, 0), p.player_best_ovr, 0) DESC,
            p.full_name ASC
        """,
        (team_id,),
    )
    rows.sort(
        key=lambda row: (
            POSITION_SORT_ORDER.get(safe_text(row.get("position")).upper(), 999),
            -resolve_display_overall(row),
            safe_text(row.get("full_name")).lower(),
        )
    )
    return rows


def fetch_passing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pps.roster_id,
            COALESCE(MAX(p.full_name), MAX(pps.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pps.pass_yds, 0)) AS stat_value
        FROM player_passing_stats pps
        LEFT JOIN players p ON p.roster_id = pps.roster_id
        LEFT JOIN teams t ON t.team_id = COALESCE(p.team_id, pps.team_id)
        GROUP BY pps.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_rushing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            prs.roster_id,
            COALESCE(MAX(p.full_name), MAX(prs.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rush_yds, 0)) AS stat_value
        FROM player_rushing_stats prs
        LEFT JOIN players p ON p.roster_id = prs.roster_id
        LEFT JOIN teams t ON t.team_id = COALESCE(p.team_id, prs.team_id)
        GROUP BY prs.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_receiving_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            prs.roster_id,
            COALESCE(MAX(p.full_name), MAX(prs.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rec_yds, 0)) AS stat_value
        FROM player_receiving_stats prs
        LEFT JOIN players p ON p.roster_id = prs.roster_id
        LEFT JOIN teams t ON t.team_id = COALESCE(p.team_id, prs.team_id)
        GROUP BY prs.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_sack_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pds.roster_id,
            COALESCE(MAX(p.full_name), MAX(pds.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.def_sacks, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p ON p.roster_id = pds.roster_id
        LEFT JOIN teams t ON t.team_id = COALESCE(p.team_id, pds.team_id)
        GROUP BY pds.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_interception_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pds.roster_id,
            COALESCE(MAX(p.full_name), MAX(pds.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.def_ints, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p ON p.roster_id = pds.roster_id
        LEFT JOIN teams t ON t.team_id = COALESCE(p.team_id, pds.team_id)
        GROUP BY pds.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_stat_leaders(db: Database, category: str, limit: int = 10) -> list[dict[str, Any]]:
    normalized = safe_text(category).lower()
    if normalized in {"passing", "pass", "passing yards", "pass yards"}:
        return fetch_passing_leaders(db, limit)
    if normalized in {"rushing", "rush", "rushing yards", "rush yards"}:
        return fetch_rushing_leaders(db, limit)
    if normalized in {"receiving", "rec", "receiving yards"}:
        return fetch_receiving_leaders(db, limit)
    if normalized in {"sacks", "sack"}:
        return fetch_sack_leaders(db, limit)
    if normalized in {"interceptions", "interception", "ints", "int"}:
        return fetch_interception_leaders(db, limit)
    raise ValueError(f"Unsupported leader category: {category}")
