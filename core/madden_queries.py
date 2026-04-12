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


def normalize_search_text(value: str) -> str:
    return " ".join(value.lower().strip().split())


def resolve_player_overall(row: dict[str, Any]) -> int:
    overall_candidates = [
        row.get("overall_rating"),
        row.get("player_best_ovr"),
        row.get("team_ovr"),
    ]
    for candidate in overall_candidates:
        value = safe_int(candidate, 0)
        if value > 0:
            return value
    return 0


def resolve_dev_trait(row: dict[str, Any]) -> str:
    raw = safe_int(row.get("dev_trait"), -1)
    if raw in DEV_TRAIT_LABELS:
        return DEV_TRAIT_LABELS[raw]

    text_candidates = [
        row.get("dev_trait_label"),
        row.get("development_trait"),
    ]
    for candidate in text_candidates:
        text = safe_text(candidate)
        if text:
            return text

    return "Unknown"


def search_players(db: Database, query: str, limit: int = 10) -> list[dict[str, Any]]:
    search_value = f"%{normalize_search_text(query)}%"
    rows = db.fetch_all(
        """
        SELECT
            p.roster_id,
            p.full_name,
            p.position,
            p.age,
            p.dev_trait,
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
        (search_value, query.strip(), f"{query.strip()}%", limit),
    )

    for row in rows:
        row["resolved_overall"] = resolve_player_overall(row)
        row["resolved_dev_trait"] = resolve_dev_trait(row)

    return rows


def get_player_by_roster_id(db: Database, roster_id: int) -> Optional[dict[str, Any]]:
    row = db.fetch_one(
        """
        SELECT
            p.roster_id,
            p.full_name,
            p.position,
            p.age,
            p.dev_trait,
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
        row["resolved_overall"] = resolve_player_overall(row)
        row["resolved_dev_trait"] = resolve_dev_trait(row)
    return row


def get_team_summary(db: Database, team_name: str) -> Optional[dict[str, Any]]:
    search_value = f"%{normalize_search_text(team_name)}%"
    return db.fetch_one(
        """
        SELECT
            t.team_id,
            t.team_name,
            t.team_abbrev,
            t.team_ovr,
            t.conference_name,
            t.division_name,
            COALESCE(s.wins, 0) AS wins,
            COALESCE(s.losses, 0) AS losses,
            COALESCE(s.ties, 0) AS ties,
            COALESCE(s.win_pct, 0) AS win_pct,
            COALESCE(s.seed, 0) AS seed,
            COALESCE(s.pts_for, 0) AS pts_for,
            COALESCE(s.pts_against, 0) AS pts_against,
            COALESCE(s.turnover_diff, 0) AS turnover_diff
        FROM teams t
        LEFT JOIN standings s
            ON s.team_id = t.team_id
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
        (search_value, team_name.strip(), f"{team_name.strip()}%"),
    )


def get_team_roster(db: Database, team_name: str) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        """
        SELECT
            p.roster_id,
            p.full_name,
            p.position,
            p.age,
            p.dev_trait,
            p.overall_rating,
            p.player_best_ovr,
            t.team_name,
            t.team_ovr
        FROM players p
        JOIN teams t
            ON t.team_id = p.team_id
        WHERE LOWER(COALESCE(t.team_name, '')) LIKE %s
        ORDER BY
            COALESCE(NULLIF(p.overall_rating, 0), p.player_best_ovr, 0) DESC,
            p.full_name ASC
        """,
        (f"%{normalize_search_text(team_name)}%",),
    )

    for row in rows:
        row["resolved_overall"] = resolve_player_overall(row)
        row["resolved_dev_trait"] = resolve_dev_trait(row)
        row["position_sort"] = POSITION_SORT_ORDER.get(safe_text(row.get("position")).upper(), 999)

    rows.sort(
        key=lambda row: (
            row.get("position_sort", 999),
            -safe_int(row.get("resolved_overall")),
            safe_text(row.get("full_name")).lower(),
        )
    )
    return rows


def get_standings(db: Database) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            t.team_id,
            t.team_name,
            t.team_abbrev,
            t.team_ovr,
            t.conference_name,
            t.division_name,
            COALESCE(s.wins, 0) AS wins,
            COALESCE(s.losses, 0) AS losses,
            COALESCE(s.ties, 0) AS ties,
            COALESCE(s.win_pct, 0) AS win_pct,
            COALESCE(s.seed, 0) AS seed,
            COALESCE(s.pts_for, 0) AS pts_for,
            COALESCE(s.pts_against, 0) AS pts_against,
            COALESCE(s.turnover_diff, 0) AS turnover_diff
        FROM standings s
        JOIN teams t
            ON t.team_id = s.team_id
        ORDER BY
            COALESCE(s.wins, 0) DESC,
            COALESCE(s.win_pct, 0) DESC,
            COALESCE(s.pts_for, 0) DESC,
            t.team_name ASC
        """
    )


def get_passing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pps.roster_id,
            COALESCE(MAX(p.full_name), MAX(pps.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pps.pass_yds, 0)) AS stat_value
        FROM player_passing_stats pps
        LEFT JOIN players p
            ON p.roster_id = pps.roster_id
        LEFT JOIN teams t
            ON t.team_id = COALESCE(p.team_id, pps.team_id)
        GROUP BY pps.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def get_rushing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            prs.roster_id,
            COALESCE(MAX(p.full_name), MAX(prs.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rush_yds, 0)) AS stat_value
        FROM player_rushing_stats prs
        LEFT JOIN players p
            ON p.roster_id = prs.roster_id
        LEFT JOIN teams t
            ON t.team_id = COALESCE(p.team_id, prs.team_id)
        GROUP BY prs.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def get_receiving_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            prs.roster_id,
            COALESCE(MAX(p.full_name), MAX(prs.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rec_yds, 0)) AS stat_value
        FROM player_receiving_stats prs
        LEFT JOIN players p
            ON p.roster_id = prs.roster_id
        LEFT JOIN teams t
            ON t.team_id = COALESCE(p.team_id, prs.team_id)
        GROUP BY prs.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def get_sack_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pds.roster_id,
            COALESCE(MAX(p.full_name), MAX(pds.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.def_sacks, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p
            ON p.roster_id = pds.roster_id
        LEFT JOIN teams t
            ON t.team_id = COALESCE(p.team_id, pds.team_id)
        GROUP BY pds.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def get_interception_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pds.roster_id,
            COALESCE(MAX(p.full_name), MAX(pds.full_name), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.def_ints, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p
            ON p.roster_id = pds.roster_id
        LEFT JOIN teams t
            ON t.team_id = COALESCE(p.team_id, pds.team_id)
        GROUP BY pds.roster_id
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def get_stat_leaders(db: Database, category: str, limit: int = 10) -> list[dict[str, Any]]:
    normalized = normalize_search_text(category)

    if normalized in {"passing", "pass", "pass yards", "passing yards"}:
        return get_passing_leaders(db, limit)
    if normalized in {"rushing", "rush", "rush yards", "rushing yards"}:
        return get_rushing_leaders(db, limit)
    if normalized in {"receiving", "receive", "rec", "receiving yards"}:
        return get_receiving_leaders(db, limit)
    if normalized in {"sacks", "sack"}:
        return get_sack_leaders(db, limit)
    if normalized in {"interceptions", "interception", "ints", "int"}:
        return get_interception_leaders(db, limit)

    raise ValueError(f"Unsupported leader category: {category}")