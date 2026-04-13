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
    "CB": 19,
    "FS": 20,
    "SS": 21,
    "K": 22,
    "P": 23,
    "LS": 24,
}

_SCHEMA_CACHE: dict[str, set[str]] = {}


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


def get_table_columns(db: Database, table_name: str) -> set[str]:
    if table_name in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[table_name]

    rows = db.fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    cols = {safe_text(row.get("column_name")) for row in rows if safe_text(row.get("column_name"))}
    _SCHEMA_CACHE[table_name] = cols
    return cols


def pick_existing_column(db: Database, table_name: str, candidates: list[str]) -> Optional[str]:
    cols = get_table_columns(db, table_name)
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def sql_select(alias: str, col: Optional[str], as_name: str) -> str:
    if col:
        return f'{alias}."{col}" AS {as_name}'
    return f"NULL AS {as_name}"


def quoted(alias: str, col: str) -> str:
    return f'{alias}."{col}"'


def resolve_display_overall(row: dict[str, Any]) -> int:
    candidates = [
        row.get("overall_rating"),
        row.get("player_best_ovr"),
        row.get("player_scheme_ovr"),
        row.get("team_ovr"),
    ]
    for candidate in candidates:
        value = safe_int(candidate, 0)
        if value > 0:
            return value
    return 0


def resolve_dev_trait_label(row: dict[str, Any]) -> str:
    raw_int = safe_int(row.get("dev_trait"), -1)
    if raw_int in DEV_TRAIT_LABELS:
        return DEV_TRAIT_LABELS[raw_int]

    for key in ["dev_trait_label", "development_trait", "trait_name"]:
        value = safe_text(row.get(key))
        if value:
            return value

    return "Unknown"


def _player_columns(db: Database) -> dict[str, Optional[str]]:
    return {
        "roster_id": pick_existing_column(db, "players", ["roster_id", "rosterId"]),
        "first_name": pick_existing_column(db, "players", ["first_name", "firstName"]),
        "last_name": pick_existing_column(db, "players", ["last_name", "lastName"]),
        "full_name": pick_existing_column(db, "players", ["full_name", "fullName"]),
        "position": pick_existing_column(db, "players", ["position"]),
        "age": pick_existing_column(db, "players", ["age"]),
        "dev_trait": pick_existing_column(db, "players", ["dev_trait", "devTrait"]),
        "overall_rating": pick_existing_column(db, "players", ["overall_rating", "overallRating"]),
        "player_best_ovr": pick_existing_column(db, "players", ["player_best_ovr", "playerBestOvr"]),
        "player_scheme_ovr": pick_existing_column(db, "players", ["player_scheme_ovr", "playerSchemeOvr"]),
        "speed": pick_existing_column(db, "players", ["speed", "speed_rating", "speedRating"]),
        "strength": pick_existing_column(db, "players", ["strength", "strength_rating", "strengthRating"]),
        "awareness": pick_existing_column(db, "players", ["awareness", "aware_rating", "awareRating", "awareness_rating"]),
        "change_of_direction": pick_existing_column(
            db,
            "players",
            [
                "change_of_direction",
                "change_of_direction_rating",
                "changeOfDirectionRating",
                "cod",
                "cod_rating",
            ],
        ),
        "team_id": pick_existing_column(db, "players", ["team_id", "teamId"]),
    }


def _team_columns(db: Database) -> dict[str, Optional[str]]:
    return {
        "team_id": pick_existing_column(db, "teams", ["team_id", "teamId"]),
        "team_name": pick_existing_column(db, "teams", ["team_name", "displayName", "display_name", "teamName"]),
        "team_ovr": pick_existing_column(db, "teams", ["team_ovr", "teamOvr"]),
        "conference_name": pick_existing_column(db, "teams", ["conference_name", "conferenceName"]),
        "division_name": pick_existing_column(db, "teams", ["division_name", "divisionName"]),
    }


def _full_name_expr(player_cols: dict[str, Optional[str]]) -> str:
    if player_cols["full_name"]:
        return quoted("p", player_cols["full_name"])
    first_expr = quoted("p", player_cols["first_name"]) if player_cols["first_name"] else "''"
    last_expr = quoted("p", player_cols["last_name"]) if player_cols["last_name"] else "''"
    return f"TRIM(CONCAT(COALESCE({first_expr}, ''), ' ', COALESCE({last_expr}, '')))"


def fetch_player_search_results(db: Database, name: str, limit: int = 10) -> list[dict[str, Any]]:
    pcols = _player_columns(db)
    tcols = _team_columns(db)
    full_name_expr = _full_name_expr(pcols)
    search = f"%{normalize_search_text(name)}%"

    join_clause = ""
    if pcols["team_id"] and tcols["team_id"]:
        join_clause = f'LEFT JOIN teams t ON {quoted("t", tcols["team_id"])} = {quoted("p", pcols["team_id"])}'

    overall_order_expr = "0"
    if pcols["overall_rating"]:
        overall_order_expr = f'COALESCE(NULLIF({quoted("p", pcols["overall_rating"])}, 0), 0)'
    elif pcols["player_best_ovr"]:
        overall_order_expr = f'COALESCE(NULLIF({quoted("p", pcols["player_best_ovr"])}, 0), 0)'

    return db.fetch_all(
        f"""
        SELECT
            {sql_select("p", pcols["roster_id"], "roster_id")},
            {full_name_expr} AS full_name,
            {sql_select("p", pcols["position"], "position")},
            {sql_select("p", pcols["age"], "age")},
            {sql_select("p", pcols["dev_trait"], "dev_trait")},
            {sql_select("p", pcols["overall_rating"], "overall_rating")},
            {sql_select("p", pcols["player_best_ovr"], "player_best_ovr")},
            {sql_select("p", pcols["player_scheme_ovr"], "player_scheme_ovr")},
            {sql_select("p", pcols["speed"], "speed")},
            {sql_select("p", pcols["strength"], "strength")},
            {sql_select("p", pcols["awareness"], "awareness")},
            {sql_select("p", pcols["change_of_direction"], "change_of_direction")},
            {sql_select("p", pcols["team_id"], "team_id")},
            {sql_select("t", tcols["team_name"], "team_name")},
            {sql_select("t", tcols["team_ovr"], "team_ovr")}
        FROM players p
        {join_clause}
        WHERE LOWER({full_name_expr}) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER({full_name_expr}) = LOWER(%s) THEN 0
                WHEN LOWER({full_name_expr}) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            {overall_order_expr} DESC,
            {full_name_expr} ASC
        LIMIT %s
        """,
        (search, safe_text(name), f"{safe_text(name)}%", limit),
    )


def fetch_player_by_roster_id(db: Database, roster_id: int) -> Optional[dict[str, Any]]:
    pcols = _player_columns(db)
    tcols = _team_columns(db)
    full_name_expr = _full_name_expr(pcols)

    if not pcols["roster_id"]:
        return None

    join_clause = ""
    if pcols["team_id"] and tcols["team_id"]:
        join_clause = f'LEFT JOIN teams t ON {quoted("t", tcols["team_id"])} = {quoted("p", pcols["team_id"])}'

    return db.fetch_one(
        f"""
        SELECT
            {sql_select("p", pcols["roster_id"], "roster_id")},
            {full_name_expr} AS full_name,
            {sql_select("p", pcols["position"], "position")},
            {sql_select("p", pcols["age"], "age")},
            {sql_select("p", pcols["dev_trait"], "dev_trait")},
            {sql_select("p", pcols["overall_rating"], "overall_rating")},
            {sql_select("p", pcols["player_best_ovr"], "player_best_ovr")},
            {sql_select("p", pcols["player_scheme_ovr"], "player_scheme_ovr")},
            {sql_select("p", pcols["speed"], "speed")},
            {sql_select("p", pcols["strength"], "strength")},
            {sql_select("p", pcols["awareness"], "awareness")},
            {sql_select("p", pcols["change_of_direction"], "change_of_direction")},
            {sql_select("p", pcols["team_id"], "team_id")},
            {sql_select("t", tcols["team_name"], "team_name")},
            {sql_select("t", tcols["team_ovr"], "team_ovr")}
        FROM players p
        {join_clause}
        WHERE {quoted("p", pcols["roster_id"])} = %s
        LIMIT 1
        """,
        (roster_id,),
    )


def resolve_team_row(db: Database, team_name: str) -> Optional[dict[str, Any]]:
    tcols = _team_columns(db)
    if not tcols["team_name"]:
        return None

    search = f"%{normalize_search_text(team_name)}%"
    return db.fetch_one(
        f"""
        SELECT
            {sql_select("t", tcols["team_id"], "team_id")},
            {sql_select("t", tcols["team_name"], "team_name")},
            {sql_select("t", tcols["team_ovr"], "team_ovr")},
            {sql_select("t", tcols["conference_name"], "conference_name")},
            {sql_select("t", tcols["division_name"], "division_name")}
        FROM teams t
        WHERE LOWER(COALESCE({quoted("t", tcols["team_name"])}, '')) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER(COALESCE({quoted("t", tcols["team_name"])}, '')) = LOWER(%s) THEN 0
                WHEN LOWER(COALESCE({quoted("t", tcols["team_name"])}, '')) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            {quoted("t", tcols["team_name"])} ASC
        LIMIT 1
        """,
        (search, safe_text(team_name), f"{safe_text(team_name)}%"),
    )


def fetch_team_standing(db: Database, team_id: int) -> Optional[dict[str, Any]]:
    scols = {
        "team_id": pick_existing_column(db, "standings", ["team_id", "teamId"]),
        "wins": pick_existing_column(db, "standings", ["wins"]),
        "losses": pick_existing_column(db, "standings", ["losses"]),
        "ties": pick_existing_column(db, "standings", ["ties"]),
        "win_pct": pick_existing_column(db, "standings", ["win_pct", "winPct"]),
        "seed": pick_existing_column(db, "standings", ["seed"]),
        "pts_for": pick_existing_column(db, "standings", ["pts_for", "ptsFor"]),
        "pts_against": pick_existing_column(db, "standings", ["pts_against", "ptsAgainst"]),
        "turnover_diff": pick_existing_column(db, "standings", ["turnover_diff", "turnoverDiff"]),
    }

    if not scols["team_id"]:
        return None

    return db.fetch_one(
        f"""
        SELECT
            {sql_select("s", scols["wins"], "wins")},
            {sql_select("s", scols["losses"], "losses")},
            {sql_select("s", scols["ties"], "ties")},
            {sql_select("s", scols["win_pct"], "win_pct")},
            {sql_select("s", scols["seed"], "seed")},
            {sql_select("s", scols["pts_for"], "pts_for")},
            {sql_select("s", scols["pts_against"], "pts_against")},
            {sql_select("s", scols["turnover_diff"], "turnover_diff")}
        FROM standings s
        WHERE {quoted("s", scols["team_id"])} = %s
        LIMIT 1
        """,
        (team_id,),
    )


def fetch_team_roster_rows(db: Database, team_id: int) -> list[dict[str, Any]]:
    pcols = _player_columns(db)
    tcols = _team_columns(db)
    full_name_expr = _full_name_expr(pcols)

    if not pcols["team_id"]:
        return []

    join_clause = ""
    if tcols["team_id"]:
        join_clause = f'LEFT JOIN teams t ON {quoted("t", tcols["team_id"])} = {quoted("p", pcols["team_id"])}'

    overall_order_expr = "0"
    if pcols["overall_rating"]:
        overall_order_expr = f'COALESCE(NULLIF({quoted("p", pcols["overall_rating"])}, 0), 0)'
    elif pcols["player_best_ovr"]:
        overall_order_expr = f'COALESCE(NULLIF({quoted("p", pcols["player_best_ovr"])}, 0), 0)'

    rows = db.fetch_all(
        f"""
        SELECT
            {sql_select("p", pcols["roster_id"], "roster_id")},
            {full_name_expr} AS full_name,
            {sql_select("p", pcols["position"], "position")},
            {sql_select("p", pcols["age"], "age")},
            {sql_select("p", pcols["dev_trait"], "dev_trait")},
            {sql_select("p", pcols["overall_rating"], "overall_rating")},
            {sql_select("p", pcols["player_best_ovr"], "player_best_ovr")},
            {sql_select("p", pcols["player_scheme_ovr"], "player_scheme_ovr")},
            {sql_select("p", pcols["speed"], "speed")},
            {sql_select("p", pcols["strength"], "strength")},
            {sql_select("p", pcols["awareness"], "awareness")},
            {sql_select("p", pcols["change_of_direction"], "change_of_direction")},
            {sql_select("p", pcols["team_id"], "team_id")},
            {sql_select("t", tcols["team_name"], "team_name")},
            {sql_select("t", tcols["team_ovr"], "team_ovr")}
        FROM players p
        {join_clause}
        WHERE {quoted("p", pcols["team_id"])} = %s
        ORDER BY
            {overall_order_expr} DESC,
            {full_name_expr} ASC
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
    tcols = _team_columns(db)
    scols = {
        "team_id": pick_existing_column(db, "standings", ["team_id", "teamId"]),
        "wins": pick_existing_column(db, "standings", ["wins"]),
        "losses": pick_existing_column(db, "standings", ["losses"]),
        "ties": pick_existing_column(db, "standings", ["ties"]),
        "win_pct": pick_existing_column(db, "standings", ["win_pct", "winPct"]),
        "seed": pick_existing_column(db, "standings", ["seed"]),
        "pts_for": pick_existing_column(db, "standings", ["pts_for", "ptsFor"]),
        "pts_against": pick_existing_column(db, "standings", ["pts_against", "ptsAgainst"]),
        "turnover_diff": pick_existing_column(db, "standings", ["turnover_diff", "turnoverDiff"]),
    }

    return db.fetch_all(
        f"""
        SELECT
            {sql_select("t", tcols["team_name"], "team_name")},
            {sql_select("t", tcols["conference_name"], "conference_name")},
            {sql_select("t", tcols["division_name"], "division_name")},
            {sql_select("t", tcols["team_ovr"], "team_ovr")},
            {sql_select("s", scols["wins"], "wins")},
            {sql_select("s", scols["losses"], "losses")},
            {sql_select("s", scols["ties"], "ties")},
            {sql_select("s", scols["win_pct"], "win_pct")},
            {sql_select("s", scols["seed"], "seed")},
            {sql_select("s", scols["pts_for"], "pts_for")},
            {sql_select("s", scols["pts_against"], "pts_against")},
            {sql_select("s", scols["turnover_diff"], "turnover_diff")}
        FROM standings s
        JOIN teams t ON {quoted("t", tcols["team_id"])} = {quoted("s", scols["team_id"])}
        ORDER BY
            COALESCE({quoted("s", scols["wins"])}, 0) DESC,
            COALESCE({quoted("s", scols["win_pct"])}, 0) DESC,
            COALESCE({quoted("s", scols["pts_for"])}, 0) DESC,
            {quoted("t", tcols["team_name"])} ASC
        """
    )


def fetch_passing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pps.roster_id,
            COALESCE(MAX(p.full_name), MAX(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(pps.pass_yds, pps.passYds, 0)) AS stat_value
        FROM player_passing_stats pps
        LEFT JOIN players p ON COALESCE(p.roster_id, p."rosterId") = COALESCE(pps.roster_id, pps."rosterId")
        LEFT JOIN teams t ON COALESCE(t.team_id, t."teamId") = COALESCE(p.team_id, p."teamId", pps.team_id, pps."teamId")
        GROUP BY COALESCE(pps.roster_id, pps."rosterId")
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
            COALESCE(MAX(p.full_name), MAX(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rush_yds, prs.rushYds, 0)) AS stat_value
        FROM player_rushing_stats prs
        LEFT JOIN players p ON COALESCE(p.roster_id, p."rosterId") = COALESCE(prs.roster_id, prs."rosterId")
        LEFT JOIN teams t ON COALESCE(t.team_id, t."teamId") = COALESCE(p.team_id, p."teamId", prs.team_id, prs."teamId")
        GROUP BY COALESCE(prs.roster_id, prs."rosterId")
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
            COALESCE(MAX(p.full_name), MAX(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rec_yds, prs.recYds, 0)) AS stat_value
        FROM player_receiving_stats prs
        LEFT JOIN players p ON COALESCE(p.roster_id, p."rosterId") = COALESCE(prs.roster_id, prs."rosterId")
        LEFT JOIN teams t ON COALESCE(t.team_id, t."teamId") = COALESCE(p.team_id, p."teamId", prs.team_id, prs."teamId")
        GROUP BY COALESCE(prs.roster_id, prs."rosterId")
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
            COALESCE(MAX(p.full_name), MAX(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.def_sacks, pds.defSacks, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p ON COALESCE(p.roster_id, p."rosterId") = COALESCE(pds.roster_id, pds."rosterId")
        LEFT JOIN teams t ON COALESCE(t.team_id, t."teamId") = COALESCE(p.team_id, p."teamId", pds.team_id, pds."teamId")
        GROUP BY COALESCE(pds.roster_id, pds."rosterId")
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
            COALESCE(MAX(p.full_name), MAX(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.team_name), MAX(t.displayName), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.def_ints, pds.defInts, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p ON COALESCE(p.roster_id, p."rosterId") = COALESCE(pds.roster_id, pds."rosterId")
        LEFT JOIN teams t ON COALESCE(t.team_id, t."teamId") = COALESCE(p.team_id, p."teamId", pds.team_id, pds."teamId")
        GROUP BY COALESCE(pds.roster_id, pds."rosterId")
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