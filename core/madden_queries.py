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


def _player_query_parts(db: Database) -> dict[str, str]:
    players = "players"
    teams = "teams"

    player_id_col = pick_existing_column(db, players, ["rosterId", "roster_id"])
    first_name_col = pick_existing_column(db, players, ["firstName", "first_name"])
    last_name_col = pick_existing_column(db, players, ["lastName", "last_name"])
    full_name_col = pick_existing_column(db, players, ["fullName", "full_name"])
    position_col = pick_existing_column(db, players, ["position"])
    age_col = pick_existing_column(db, players, ["age"])
    dev_trait_col = pick_existing_column(db, players, ["devTrait", "dev_trait"])
    overall_col = pick_existing_column(db, players, ["overallRating", "overall_rating"])
    best_ovr_col = pick_existing_column(db, players, ["playerBestOvr", "player_best_ovr"])
    scheme_ovr_col = pick_existing_column(db, players, ["playerSchemeOvr", "player_scheme_ovr"])
    speed_col = pick_existing_column(db, players, ["speedRating", "speed_rating", "speed"])
    strength_col = pick_existing_column(db, players, ["strengthRating", "strength_rating", "strength"])
    awareness_col = pick_existing_column(db, players, ["awareRating", "awareness_rating", "awareness"])
    cod_col = pick_existing_column(
        db,
        players,
        ["changeOfDirectionRating", "change_of_direction_rating", "change_of_direction", "cod_rating", "cod"],
    )
    player_team_col = pick_existing_column(db, players, ["teamId", "team_id"])

    team_id_col = pick_existing_column(db, teams, ["teamId", "team_id"])
    team_name_col = pick_existing_column(db, teams, ["displayName", "display_name", "teamName", "team_name"])
    team_ovr_col = pick_existing_column(db, teams, ["teamOvr", "team_ovr"])
    conference_col = pick_existing_column(db, teams, ["conferenceName", "conference_name"])
    division_col = pick_existing_column(db, teams, ["divisionName", "division_name"])

    if full_name_col:
        full_name_expr = quoted("p", full_name_col)
    else:
        first_expr = quoted("p", first_name_col) if first_name_col else "''"
        last_expr = quoted("p", last_name_col) if last_name_col else "''"
        full_name_expr = f"CONCAT(COALESCE({first_expr}, ''), ' ', COALESCE({last_expr}, ''))"

    join_clause = ""
    if player_team_col and team_id_col:
        join_clause = f"LEFT JOIN teams t ON {quoted('t', team_id_col)} = {quoted('p', player_team_col)}"

    return {
        "full_name_expr": full_name_expr,
        "join_clause": join_clause,
        "player_id_sel": sql_select("p", player_id_col, "roster_id"),
        "position_sel": sql_select("p", position_col, "position"),
        "age_sel": sql_select("p", age_col, "age"),
        "dev_trait_sel": sql_select("p", dev_trait_col, "dev_trait"),
        "overall_sel": sql_select("p", overall_col, "overall_rating"),
        "best_ovr_sel": sql_select("p", best_ovr_col, "player_best_ovr"),
        "scheme_ovr_sel": sql_select("p", scheme_ovr_col, "player_scheme_ovr"),
        "speed_sel": sql_select("p", speed_col, "speed"),
        "strength_sel": sql_select("p", strength_col, "strength"),
        "awareness_sel": sql_select("p", awareness_col, "awareness"),
        "cod_sel": sql_select("p", cod_col, "change_of_direction"),
        "team_id_sel": sql_select("p", player_team_col, "team_id"),
        "team_name_sel": sql_select("t", team_name_col, "team_name"),
        "team_ovr_sel": sql_select("t", team_ovr_col, "team_ovr"),
        "conference_sel": sql_select("t", conference_col, "conference_name"),
        "division_sel": sql_select("t", division_col, "division_name"),
        "player_id_col": player_id_col or "",
        "player_team_col": player_team_col or "",
        "team_id_col": team_id_col or "",
        "team_name_col": team_name_col or "",
        "team_ovr_col": team_ovr_col or "",
        "conference_col": conference_col or "",
        "division_col": division_col or "",
    }


def fetch_player_search_results(db: Database, name: str, limit: int = 10) -> list[dict[str, Any]]:
    parts = _player_query_parts(db)
    search = f"%{normalize_search_text(name)}%"

    return db.fetch_all(
        f"""
        SELECT
            {parts["player_id_sel"]},
            {parts["full_name_expr"]} AS full_name,
            {parts["position_sel"]},
            {parts["age_sel"]},
            {parts["dev_trait_sel"]},
            {parts["overall_sel"]},
            {parts["best_ovr_sel"]},
            {parts["scheme_ovr_sel"]},
            {parts["speed_sel"]},
            {parts["strength_sel"]},
            {parts["awareness_sel"]},
            {parts["cod_sel"]},
            {parts["team_id_sel"]},
            {parts["team_name_sel"]},
            {parts["team_ovr_sel"]}
        FROM players p
        {parts["join_clause"]}
        WHERE LOWER({parts["full_name_expr"]}) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER({parts["full_name_expr"]}) = LOWER(%s) THEN 0
                WHEN LOWER({parts["full_name_expr"]}) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            COALESCE(NULLIF({quoted("p", parts["player_id_col"] and pick_existing_column(db, "players", ["overallRating", "overall_rating"]) or "")}, 0), 0) DESC,
            {parts["full_name_expr"]} ASC
        LIMIT %s
        """,
        (search, safe_text(name), f"{safe_text(name)}%", limit),
    )


def fetch_player_by_roster_id(db: Database, roster_id: int) -> Optional[dict[str, Any]]:
    parts = _player_query_parts(db)
    if not parts["player_id_col"]:
        return None

    return db.fetch_one(
        f"""
        SELECT
            {parts["player_id_sel"]},
            {parts["full_name_expr"]} AS full_name,
            {parts["position_sel"]},
            {parts["age_sel"]},
            {parts["dev_trait_sel"]},
            {parts["overall_sel"]},
            {parts["best_ovr_sel"]},
            {parts["scheme_ovr_sel"]},
            {parts["speed_sel"]},
            {parts["strength_sel"]},
            {parts["awareness_sel"]},
            {parts["cod_sel"]},
            {parts["team_id_sel"]},
            {parts["team_name_sel"]},
            {parts["team_ovr_sel"]}
        FROM players p
        {parts["join_clause"]}
        WHERE {quoted("p", parts["player_id_col"])} = %s
        LIMIT 1
        """,
        (roster_id,),
    )


def resolve_team_row(db: Database, team_name: str) -> Optional[dict[str, Any]]:
    teams = "teams"
    team_id_col = pick_existing_column(db, teams, ["teamId", "team_id"])
    team_name_col = pick_existing_column(db, teams, ["displayName", "display_name", "teamName", "team_name"])
    team_ovr_col = pick_existing_column(db, teams, ["teamOvr", "team_ovr"])
    conference_col = pick_existing_column(db, teams, ["conferenceName", "conference_name"])
    division_col = pick_existing_column(db, teams, ["divisionName", "division_name"])

    if not team_name_col:
        return None

    search = f"%{normalize_search_text(team_name)}%"
    return db.fetch_one(
        f"""
        SELECT
            {sql_select("t", team_id_col, "team_id")},
            {sql_select("t", team_name_col, "team_name")},
            {sql_select("t", team_ovr_col, "team_ovr")},
            {sql_select("t", conference_col, "conference_name")},
            {sql_select("t", division_col, "division_name")}
        FROM teams t
        WHERE LOWER(COALESCE({quoted("t", team_name_col)}, '')) LIKE %s
        ORDER BY
            CASE
                WHEN LOWER(COALESCE({quoted("t", team_name_col)}, '')) = LOWER(%s) THEN 0
                WHEN LOWER(COALESCE({quoted("t", team_name_col)}, '')) LIKE LOWER(%s) THEN 1
                ELSE 2
            END,
            {quoted("t", team_name_col)} ASC
        LIMIT 1
        """,
        (search, safe_text(team_name), f"{safe_text(team_name)}%"),
    )


def fetch_team_standing(db: Database, team_id: int) -> Optional[dict[str, Any]]:
    standings = "standings"
    team_id_col = pick_existing_column(db, standings, ["teamId", "team_id"])
    win_pct_col = pick_existing_column(db, standings, ["winPct", "win_pct"])
    pts_for_col = pick_existing_column(db, standings, ["ptsFor", "pts_for"])
    pts_against_col = pick_existing_column(db, standings, ["ptsAgainst", "pts_against"])
    turnover_diff_col = pick_existing_column(db, standings, ["turnoverDiff", "turnover_diff"])
    wins_col = pick_existing_column(db, standings, ["wins"])
    losses_col = pick_existing_column(db, standings, ["losses"])
    ties_col = pick_existing_column(db, standings, ["ties"])
    seed_col = pick_existing_column(db, standings, ["seed"])

    if not team_id_col:
        return None

    return db.fetch_one(
        f"""
        SELECT
            {sql_select("s", wins_col, "wins")},
            {sql_select("s", losses_col, "losses")},
            {sql_select("s", ties_col, "ties")},
            {sql_select("s", win_pct_col, "win_pct")},
            {sql_select("s", seed_col, "seed")},
            {sql_select("s", pts_for_col, "pts_for")},
            {sql_select("s", pts_against_col, "pts_against")},
            {sql_select("s", turnover_diff_col, "turnover_diff")}
        FROM standings s
        WHERE {quoted("s", team_id_col)} = %s
        LIMIT 1
        """,
        (team_id,),
    )


def fetch_team_roster_rows(db: Database, team_id: int) -> list[dict[str, Any]]:
    parts = _player_query_parts(db)
    if not parts["player_team_col"]:
        return []

    rows = db.fetch_all(
        f"""
        SELECT
            {parts["player_id_sel"]},
            {parts["full_name_expr"]} AS full_name,
            {parts["position_sel"]},
            {parts["age_sel"]},
            {parts["dev_trait_sel"]},
            {parts["overall_sel"]},
            {parts["best_ovr_sel"]},
            {parts["scheme_ovr_sel"]},
            {parts["speed_sel"]},
            {parts["strength_sel"]},
            {parts["awareness_sel"]},
            {parts["cod_sel"]},
            {parts["team_id_sel"]},
            {parts["team_name_sel"]},
            {parts["team_ovr_sel"]}
        FROM players p
        {parts["join_clause"]}
        WHERE {quoted("p", parts["player_team_col"])} = %s
        ORDER BY
            COALESCE({quoted("p", pick_existing_column(db, "players", ["overallRating", "overall_rating"]) or parts["player_team_col"])}, 0) DESC,
            {parts["full_name_expr"]} ASC
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
    teams = "teams"
    standings = "standings"

    team_id_standings_col = pick_existing_column(db, standings, ["teamId", "team_id"])
    team_id_teams_col = pick_existing_column(db, teams, ["teamId", "team_id"])
    team_name_col = pick_existing_column(db, teams, ["displayName", "display_name", "teamName", "team_name"])
    team_ovr_col = pick_existing_column(db, teams, ["teamOvr", "team_ovr"])
    conference_col = pick_existing_column(db, teams, ["conferenceName", "conference_name"])
    division_col = pick_existing_column(db, teams, ["divisionName", "division_name"])
    win_pct_col = pick_existing_column(db, standings, ["winPct", "win_pct"])
    pts_for_col = pick_existing_column(db, standings, ["ptsFor", "pts_for"])
    pts_against_col = pick_existing_column(db, standings, ["ptsAgainst", "pts_against"])
    turnover_diff_col = pick_existing_column(db, standings, ["turnoverDiff", "turnover_diff"])
    wins_col = pick_existing_column(db, standings, ["wins"])
    losses_col = pick_existing_column(db, standings, ["losses"])
    ties_col = pick_existing_column(db, standings, ["ties"])
    seed_col = pick_existing_column(db, standings, ["seed"])

    return db.fetch_all(
        f"""
        SELECT
            {sql_select("t", team_name_col, "team_name")},
            {sql_select("t", conference_col, "conference_name")},
            {sql_select("t", division_col, "division_name")},
            {sql_select("t", team_ovr_col, "team_ovr")},
            {sql_select("s", wins_col, "wins")},
            {sql_select("s", losses_col, "losses")},
            {sql_select("s", ties_col, "ties")},
            {sql_select("s", win_pct_col, "win_pct")},
            {sql_select("s", seed_col, "seed")},
            {sql_select("s", pts_for_col, "pts_for")},
            {sql_select("s", pts_against_col, "pts_against")},
            {sql_select("s", turnover_diff_col, "turnover_diff")}
        FROM standings s
        JOIN teams t ON {quoted("t", team_id_teams_col)} = {quoted("s", team_id_standings_col)}
        ORDER BY
            COALESCE({quoted("s", wins_col)}, 0) DESC,
            COALESCE({quoted("s", win_pct_col)}, 0) DESC,
            COALESCE({quoted("s", pts_for_col)}, 0) DESC,
            {quoted("t", team_name_col)} ASC
        """
    )


def fetch_passing_leaders(db: Database, limit: int = 10) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            pps.rosterId AS roster_id,
            COALESCE(MAX(CONCAT(COALESCE(p.firstName, ''), ' ', COALESCE(p.lastName, ''))), 'Unknown') AS player_name,
            COALESCE(MAX(t.displayName), MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pps.passYds, pps.pass_yds, 0)) AS stat_value
        FROM player_passing_stats pps
        LEFT JOIN players p ON p.rosterId = pps.rosterId
        LEFT JOIN teams t ON t.team_id = COALESCE(p.teamId, p.team_id, pps.teamId, pps.team_id)
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
            COALESCE(MAX(t.displayName), MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.rushYds, prs.rush_yds, 0)) AS stat_value
        FROM player_rushing_stats prs
        LEFT JOIN players p ON p.rosterId = prs.rosterId
        LEFT JOIN teams t ON t.team_id = COALESCE(p.teamId, p.team_id, prs.teamId, prs.team_id)
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
            COALESCE(MAX(t.displayName), MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(prs.recYds, prs.rec_yds, 0)) AS stat_value
        FROM player_receiving_stats prs
        LEFT JOIN players p ON p.rosterId = prs.rosterId
        LEFT JOIN teams t ON t.team_id = COALESCE(p.teamId, p.team_id, prs.teamId, prs.team_id)
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
            COALESCE(MAX(t.displayName), MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.defSacks, pds.def_sacks, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p ON p.rosterId = pds.rosterId
        LEFT JOIN teams t ON t.team_id = COALESCE(p.teamId, p.team_id, pds.teamId, pds.team_id)
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
            COALESCE(MAX(t.displayName), MAX(t.team_name), 'Unknown Team') AS team_name,
            SUM(COALESCE(pds.defInts, pds.def_ints, 0)) AS stat_value
        FROM player_defense_stats pds
        LEFT JOIN players p ON p.rosterId = pds.rosterId
        LEFT JOIN teams t ON t.team_id = COALESCE(p.teamId, p.team_id, pds.teamId, pds.team_id)
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