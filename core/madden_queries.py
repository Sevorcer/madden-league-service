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
    "LEDGE": 12,
    "REDGE": 13,
    "LE": 14,
    "RE": 15,
    "DT": 16,
    "LOLB": 17,
    "MLB": 18,
    "ROLB": 19,
    "CB": 20,
    "FS": 21,
    "SS": 22,
    "K": 23,
    "P": 24,
    "LS": 25,
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
    return " ".join(safe_text(value).lower().split())


def resolve_display_overall(row: dict[str, Any]) -> int:
    candidates = [
        row.get("overall_rating"),
        row.get("player_best_ovr"),
        row.get("playerBestOvr"),
        row.get("player_scheme_ovr"),
        row.get("playerSchemeOvr"),
        row.get("team_ovr"),
        row.get("teamSchemeOvr"),
    ]
    for candidate in candidates:
        value = safe_int(candidate, 0)
        if value > 0:
            return value
    return 0


def resolve_dev_trait_label(row: dict[str, Any]) -> str:
    raw = row.get("dev_trait")
    raw_int = safe_int(raw, -1)
    if raw_int in DEV_TRAIT_LABELS:
        return DEV_TRAIT_LABELS[raw_int]

    for key in ["dev_trait_label", "development_trait", "trait_name"]:
        value = safe_text(row.get(key))
        if value:
            return value

    return "Unknown"


def fetch_player_search_results(db: Database, name: str, limit: int = 10) -> list[dict[str, Any]]:
    search = f"%{normalize_search_text(name)}%"
    return db.fetch_all(
        """
        SELECT
            p.rosterId AS roster_id,
            CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, '')) AS full_name,
            p.position,
            p.age,
            p.devTrait AS dev_trait,
            p.overallRating AS overall_rating,
            p.playerBestOvr AS player_best_ovr,
            p.playerSchemeOvr AS player_scheme_ovr,
            p.speedRating AS speed,
            p.strengthRating AS strength,
            p.awareRating AS awareness,
            p.changeOfDirectionRating AS change_of_direction,
            p.teamId AS team_id,
            t.displayName AS team_name,
            t.teamOvr AS team_ovr
        FROM players p
        LEFT JOIN teams t
            ON t.teamId = p.teamId
        WHERE LOWER(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))) = LOWER(%s) THEN 0
                WHEN LOWER(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            COALESCE(NULLIF(p.overallRating, 0), p.playerBestOvr, p.playerSchemeOvr, 0) DESC,
            CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, '')) ASC
        LIMIT %s
        """,
        (search, safe_text(name), f"{safe_text(name)}%", limit),
    )


def fetch_player_by_roster_id(db: Database, roster_id: int) -> Optional[dict[str, Any]]:
    return db.fetch_one(
        """
        SELECT
            p.rosterId AS roster_id,
            CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, '')) AS full_name,
            p.position,
            p.age,
            p.devTrait AS dev_trait,
            p.overallRating AS overall_rating,
            p.playerBestOvr AS player_best_ovr,
            p.playerSchemeOvr AS player_scheme_ovr,
            p.speedRating AS speed,
            p.strengthRating AS strength,
            p.awareRating AS awareness,
            p.changeOfDirectionRating AS change_of_direction,
            p.teamId AS team_id,
            t.displayName AS team_name,
            t.teamOvr AS team_ovr
        FROM players p
        LEFT JOIN teams t
            ON t.teamId = p.teamId
        WHERE p.rosterId = %s
        LIMIT 1
        """,
        (roster_id,),
    )


def resolve_team_row(db: Database, team_name: str) -> Optional[dict[str, Any]]:
    search = f"%{normalize_search_text(team_name)}%"
    return db.fetch_one(
        """
        SELECT
            t.teamId AS team_id,
            t.displayName AS team_name,
            t.teamOvr AS team_ovr,
            t.conferenceName AS conference_name,
            t.divisionName AS division_name
        FROM teams t
        WHERE LOWER(COALESCE(t.displayName, '')) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(t.displayName, '')) = LOWER(%s) THEN 0
                WHEN LOWER(COALESCE(t.displayName, '')) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            t.displayName ASC
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
            winPct AS win_pct,
            seed,
            ptsFor AS pts_for,
            ptsAgainst AS pts_against,
            turnoverDiff AS turnover_diff
        FROM standings
        WHERE teamId = %s
        LIMIT 1
        """,
        (team_id,),
    )


def fetch_team_roster_rows(db: Database, team_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        """
        SELECT
            p.rosterId AS roster_id,
            CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, '')) AS full_name,
            p.position,
            p.age,
            p.devTrait AS dev_trait,
            p.overallRating AS overall_rating,
            p.playerBestOvr AS player_best_ovr,
            p.playerSchemeOvr AS player_scheme_ovr,
            p.speedRating AS speed,
            p.strengthRating AS strength,
            p.awareRating AS awareness,
            p.changeOfDirectionRating AS change_of_direction,
            p.teamId AS team_id,
            t.displayName AS team_name,
            t.teamOvr AS team_ovr
        FROM players p
        JOIN teams t
            ON t.teamId = p.teamId
        WHERE p.teamId = %s
        ORDER BY
            COALESCE(NULLIF(p.overallRating, 0), p.playerBestOvr, p.playerSchemeOvr, 0) DESC,
            CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, '')) ASC
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


def fetch_standings_rows(db: Database) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            t.displayName AS team_name,
            t.conferenceName AS conference_name,
            t.divisionName AS division_name,
            t.teamOvr AS team_ovr,
            s.wins,
            s.losses,
            s.ties,
            s.winPct AS win_pct,
            s.seed,
            s.ptsFor AS pts_for,
            s.ptsAgainst AS pts_against,
            s.turnoverDiff AS turnover_diff
        FROM standings s
        JOIN teams t
            ON t.teamId = s.teamId
        ORDER BY s.wins DESC, s.winPct DESC, s.ptsFor DESC, t.displayName ASC
        """
    )


def fetch_passing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pps.rosterId AS roster_id,
            COALESCE(MAX(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(pps.passYds, 0)) AS stat_value
        FROM player_passing_stats pps
        LEFT JOIN players p
            ON p.rosterId = pps.rosterId
        LEFT JOIN teams t
            ON t.teamId = COALESCE(p.teamId, pps.teamId)
        GROUP BY pps.rosterId
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_rushing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            prs.rosterId AS roster_id,
            COALESCE(MAX(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rushYds, 0)) AS stat_value
        FROM player_rushing_stats prs
        LEFT JOIN players p
            ON p.rosterId = prs.rosterId
        LEFT JOIN teams t
            ON t.teamId = COALESCE(p.teamId, prs.teamId)
        GROUP BY prs.rosterId
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_receiving_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            prs.rosterId AS roster_id,
            COALESCE(MAX(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.recYds, 0)) AS stat_value
        FROM player_receiving_stats prs
        LEFT JOIN players p
            ON p.rosterId = prs.rosterId
        LEFT JOIN teams t
            ON t.teamId = COALESCE(p.teamId, prs.teamId)
        GROUP BY prs.rosterId
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_sack_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pds.rosterId AS roster_id,
            COALESCE(MAX(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.defSacks, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p
            ON p.rosterId = pds.rosterId
        LEFT JOIN teams t
            ON t.teamId = COALESCE(p.teamId, pds.teamId)
        GROUP BY pds.rosterId
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_interception_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pds.rosterId AS roster_id,
            COALESCE(MAX(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.defInts, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p
            ON p.rosterId = pds.rosterId
        LEFT JOIN teams t
            ON t.teamId = COALESCE(p.teamId, pds.teamId)
        GROUP BY pds.rosterId
        ORDER BY stat_value DESC, player_name ASC
        LIMIT %s
        """,
        (limit,),
    )


def fetch_stat_leaders(db: Database, category: str, limit: int = 10) -> list[dict[str, Any]]:
    normalized = normalize_search_text(category)

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
