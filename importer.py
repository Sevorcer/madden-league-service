
import json
import os
import re
from pathlib import Path

import psycopg

EXPORT_DIR = Path("received_exports")
DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    if DATABASE_URL:
        return psycopg.connect(DATABASE_URL)
    raise RuntimeError("DATABASE_URL is not set. Set it before running the importer.")


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_id BIGINT PRIMARY KEY,
                team_name TEXT,
                conference_name TEXT,
                division_name TEXT,
                team_ovr INTEGER
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS standings (
                team_id BIGINT PRIMARY KEY,
                wins INTEGER,
                losses INTEGER,
                ties INTEGER,
                win_pct DOUBLE PRECISION,
                seed INTEGER,
                pts_for INTEGER,
                pts_against INTEGER,
                turnover_diff INTEGER,
                off_total_yds_rank INTEGER,
                def_total_yds_rank INTEGER,
                FOREIGN KEY(team_id) REFERENCES teams(team_id)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                roster_id BIGINT PRIMARY KEY,
                team_id BIGINT,
                first_name TEXT,
                last_name TEXT,
                full_name TEXT,
                position TEXT,
                age INTEGER,
                overall_rating INTEGER,
                jersey_num INTEGER,
                years_pro INTEGER,
                height INTEGER,
                weight INTEGER,
                college TEXT,
                player_best_ovr INTEGER,
                contract_salary BIGINT,
                contract_years_left INTEGER,
                is_free_agent INTEGER,
                injury_rating INTEGER,
                speed_rating INTEGER,
                strength_rating INTEGER,
                awareness_rating INTEGER,
                throw_power_rating INTEGER,
                break_tackle_rating INTEGER,
                man_cover_rating INTEGER,
                zone_cover_rating INTEGER,
                catch_rating INTEGER,
                carrying_rating INTEGER,
                rookie_year INTEGER,
                FOREIGN KEY(team_id) REFERENCES teams(team_id)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS games (
                game_id BIGINT PRIMARY KEY,
                season_index INTEGER,
                stage_index INTEGER,
                week INTEGER,
                away_team_id BIGINT,
                home_team_id BIGINT,
                away_score INTEGER,
                home_score INTEGER,
                status INTEGER,
                is_game_of_the_week INTEGER DEFAULT 0,
                FOREIGN KEY(away_team_id) REFERENCES teams(team_id),
                FOREIGN KEY(home_team_id) REFERENCES teams(team_id)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS player_receiving_stats (
                season_index INTEGER NOT NULL DEFAULT 0,
                stage_index INTEGER NOT NULL DEFAULT 0,
                week INTEGER NOT NULL DEFAULT 0,
                roster_id BIGINT NOT NULL,
                team_id BIGINT,
                full_name TEXT,
                rec_catches INTEGER DEFAULT 0,
                rec_yds INTEGER DEFAULT 0,
                rec_tds INTEGER DEFAULT 0,
                rec_yac INTEGER DEFAULT 0,
                rec_drops INTEGER DEFAULT 0,
                rec_long INTEGER DEFAULT 0,
                PRIMARY KEY (season_index, stage_index, week, roster_id)
            )
            """)
        conn.commit()


def load_json_file(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_latest_file(keyword: str):
    matches = sorted(
        EXPORT_DIR.glob(f"*{keyword}*.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return matches[0] if matches else None


def find_all_files(keyword: str):
    return sorted(
        EXPORT_DIR.glob(f"*{keyword}*.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def _pick_first(dct, *keys, default=None):
    for key in keys:
        if key in dct and dct.get(key) is not None:
            return dct.get(key)
    return default


def _find_content_list(data: dict, candidates: list[str]):
    content = data.get("content", {})
    for key in candidates:
        value = content.get(key)
        if isinstance(value, list):
            return value
    for key, value in content.items():
        if isinstance(value, list) and any(token.lower() in key.lower() for token in ("receiv", "stand", "roster", "schedule")):
            return value
    return []


_STAGE_NAME_MAP = {
    "pre": 0,
    "preseason": 0,
    "reg": 1,
    "regular": 1,
    "wc": 2,
    "wildcard": 2,
    "wild_card": 2,
    "div": 3,
    "divisional": 3,
    "conf": 4,
    "conference": 4,
    "sb": 5,
    "superbowl": 5,
    "super_bowl": 5,
}


def infer_meta_from_filename(path: Path):
    name = path.stem.lower()
    season_index = 0
    stage_index = 0
    week = 0

    m = re.search(r"_week_([a-z]+)_(\d+)_", name)
    if m:
        stage_index = _STAGE_NAME_MAP.get(m.group(1), 0)
        week = max(int(m.group(2)) - 1, 0)

    # Optional season parsing if available in a filename later.
    sm = re.search(r"_season_(\d+)_", name)
    if sm:
        season_index = int(sm.group(1))

    return season_index, stage_index, week


def import_standings_file(path: Path):
    data = load_json_file(path)
    standings_list = _find_content_list(data, ["teamStandingInfoList", "standingsList"])

    if not standings_list:
        print(f"No standings data found in {path.name}")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            for team in standings_list:
                team_id = _pick_first(team, "teamId", "team_id")
                if team_id is None:
                    continue

                cur.execute("""
                    INSERT INTO teams (
                        team_id, team_name, conference_name, division_name, team_ovr
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (team_id) DO UPDATE SET
                        team_name = EXCLUDED.team_name,
                        conference_name = EXCLUDED.conference_name,
                        division_name = EXCLUDED.division_name,
                        team_ovr = EXCLUDED.team_ovr
                """, (
                    team_id,
                    _pick_first(team, "teamName", "team_name"),
                    _pick_first(team, "conferenceName", "conference_name"),
                    _pick_first(team, "divisionName", "division_name"),
                    _pick_first(team, "teamOvr", "team_ovr"),
                ))

                cur.execute("""
                    INSERT INTO standings (
                        team_id, wins, losses, ties, win_pct, seed,
                        pts_for, pts_against, turnover_diff,
                        off_total_yds_rank, def_total_yds_rank
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (team_id) DO UPDATE SET
                        wins = EXCLUDED.wins,
                        losses = EXCLUDED.losses,
                        ties = EXCLUDED.ties,
                        win_pct = EXCLUDED.win_pct,
                        seed = EXCLUDED.seed,
                        pts_for = EXCLUDED.pts_for,
                        pts_against = EXCLUDED.pts_against,
                        turnover_diff = EXCLUDED.turnover_diff,
                        off_total_yds_rank = EXCLUDED.off_total_yds_rank,
                        def_total_yds_rank = EXCLUDED.def_total_yds_rank
                """, (
                    team_id,
                    _pick_first(team, "totalWins", "wins", default=0),
                    _pick_first(team, "totalLosses", "losses", default=0),
                    _pick_first(team, "totalTies", "ties", default=0),
                    _pick_first(team, "winPct", "win_pct", default=0.0),
                    _pick_first(team, "seed", default=0),
                    _pick_first(team, "ptsFor", "pts_for", default=0),
                    _pick_first(team, "ptsAgainst", "pts_against", default=0),
                    _pick_first(team, "tODiff", "turnoverDiff", "turnover_diff", default=0),
                    _pick_first(team, "offTotalYdsRank", "off_total_yds_rank", default=0),
                    _pick_first(team, "defTotalYdsRank", "def_total_yds_rank", default=0),
                ))
        conn.commit()

    print(f"Imported standings from {path.name}")


def import_roster_file(path: Path):
    data = load_json_file(path)
    roster_list = _find_content_list(data, ["rosterInfoList", "rosterList", "playerList"])

    if not roster_list:
        print(f"No roster data found in {path.name}")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            for player in roster_list:
                roster_id = _pick_first(player, "rosterId", "roster_id")
                if roster_id is None:
                    continue

                first_name = _pick_first(player, "firstName", "first_name", default="") or ""
                last_name = _pick_first(player, "lastName", "last_name", default="") or ""
                full_name = _pick_first(player, "fullName", "full_name")
                if not full_name:
                    full_name = f"{first_name} {last_name}".strip()

                cur.execute("""
                    INSERT INTO players (
                        roster_id, team_id, first_name, last_name, full_name,
                        position, age, overall_rating, jersey_num, years_pro,
                        height, weight, college, player_best_ovr,
                        contract_salary, contract_years_left, is_free_agent,
                        injury_rating, speed_rating, strength_rating,
                        awareness_rating, throw_power_rating, break_tackle_rating,
                        man_cover_rating, zone_cover_rating, catch_rating,
                        carrying_rating, rookie_year
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (roster_id) DO UPDATE SET
                        team_id = EXCLUDED.team_id,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        full_name = EXCLUDED.full_name,
                        position = EXCLUDED.position,
                        age = EXCLUDED.age,
                        overall_rating = EXCLUDED.overall_rating,
                        jersey_num = EXCLUDED.jersey_num,
                        years_pro = EXCLUDED.years_pro,
                        height = EXCLUDED.height,
                        weight = EXCLUDED.weight,
                        college = EXCLUDED.college,
                        player_best_ovr = EXCLUDED.player_best_ovr,
                        contract_salary = EXCLUDED.contract_salary,
                        contract_years_left = EXCLUDED.contract_years_left,
                        is_free_agent = EXCLUDED.is_free_agent,
                        injury_rating = EXCLUDED.injury_rating,
                        speed_rating = EXCLUDED.speed_rating,
                        strength_rating = EXCLUDED.strength_rating,
                        awareness_rating = EXCLUDED.awareness_rating,
                        throw_power_rating = EXCLUDED.throw_power_rating,
                        break_tackle_rating = EXCLUDED.break_tackle_rating,
                        man_cover_rating = EXCLUDED.man_cover_rating,
                        zone_cover_rating = EXCLUDED.zone_cover_rating,
                        catch_rating = EXCLUDED.catch_rating,
                        carrying_rating = EXCLUDED.carrying_rating,
                        rookie_year = EXCLUDED.rookie_year
                """, (
                    roster_id,
                    _pick_first(player, "teamId", "team_id"),
                    first_name,
                    last_name,
                    full_name,
                    _pick_first(player, "position"),
                    _pick_first(player, "age"),
                    _pick_first(player, "overallRating", "overall_rating"),
                    _pick_first(player, "jerseyNum", "jersey_num"),
                    _pick_first(player, "yearsPro", "years_pro"),
                    _pick_first(player, "height"),
                    _pick_first(player, "weight"),
                    _pick_first(player, "college"),
                    _pick_first(player, "playerBestOvr", "player_best_ovr"),
                    _pick_first(player, "contractSalary", "contract_salary"),
                    _pick_first(player, "contractYearsLeft", "contract_years_left"),
                    1 if _pick_first(player, "isFreeAgent", "is_free_agent", default=False) else 0,
                    _pick_first(player, "injuryRating", "injury_rating"),
                    _pick_first(player, "speedRating", "speed_rating"),
                    _pick_first(player, "strengthRating", "strength_rating"),
                    _pick_first(player, "awarenessRating", "awareness_rating"),
                    _pick_first(player, "throwPowerRating", "throw_power_rating"),
                    _pick_first(player, "breakTackleRating", "break_tackle_rating"),
                    _pick_first(player, "manCoverRating", "man_cover_rating"),
                    _pick_first(player, "zoneCoverRating", "zone_cover_rating"),
                    _pick_first(player, "catchRating", "catch_rating"),
                    _pick_first(player, "carryingRating", "carrying_rating"),
                    _pick_first(player, "rookieYear", "rookie_year"),
                ))
        conn.commit()

    print(f"Imported roster from {path.name}")


def import_schedule_file(path: Path):
    data = load_json_file(path)
    schedule_list = _find_content_list(data, ["gameScheduleInfoList", "scheduleList"])

    if not schedule_list:
        print(f"No schedule data found in {path.name}")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            for game in schedule_list:
                game_id = _pick_first(game, "scheduleId", "gameId", "game_id")
                if game_id is None:
                    continue

                cur.execute("""
                    INSERT INTO games (
                        game_id,
                        season_index,
                        stage_index,
                        week,
                        away_team_id,
                        home_team_id,
                        away_score,
                        home_score,
                        status,
                        is_game_of_the_week
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id) DO UPDATE SET
                        season_index = EXCLUDED.season_index,
                        stage_index = EXCLUDED.stage_index,
                        week = EXCLUDED.week,
                        away_team_id = EXCLUDED.away_team_id,
                        home_team_id = EXCLUDED.home_team_id,
                        away_score = EXCLUDED.away_score,
                        home_score = EXCLUDED.home_score,
                        status = EXCLUDED.status,
                        is_game_of_the_week = EXCLUDED.is_game_of_the_week
                """, (
                    game_id,
                    _pick_first(game, "seasonIndex", "season_index", default=0),
                    _pick_first(game, "stageIndex", "stage_index", default=0),
                    _pick_first(game, "weekIndex", "week", default=0),
                    _pick_first(game, "awayTeamId", "away_team_id"),
                    _pick_first(game, "homeTeamId", "home_team_id"),
                    _pick_first(game, "awayScore", "away_score", default=0),
                    _pick_first(game, "homeScore", "home_score", default=0),
                    _pick_first(game, "status", default=0),
                    1 if _pick_first(game, "isGameOfTheWeek", "is_game_of_the_week", default=False) else 0,
                ))
        conn.commit()

    print(f"Imported schedule from {path.name}")


def import_receiving_file(path: Path):
    data = load_json_file(path)
    receiving_list = _find_content_list(
        data,
        [
            "playerReceivingStatInfoList",
            "playerReceivingStatsList",
            "receivingStatsList",
            "player_receiving_stats",
            "receivingInfoList",
        ],
    )

    if not receiving_list:
        print(f"No receiving data found in {path.name}")
        return

    season_index = _pick_first(data.get("content", {}), "seasonIndex", "season_index")
    stage_index = _pick_first(data.get("content", {}), "stageIndex", "stage_index")
    week = _pick_first(data.get("content", {}), "weekIndex", "week")
    if season_index is None or stage_index is None or week is None:
        inferred_season, inferred_stage, inferred_week = infer_meta_from_filename(path)
        season_index = inferred_season if season_index is None else season_index
        stage_index = inferred_stage if stage_index is None else stage_index
        week = inferred_week if week is None else week

    with get_conn() as conn:
        with conn.cursor() as cur:
            for row in receiving_list:
                roster_id = _pick_first(row, "rosterId", "roster_id")
                if roster_id is None:
                    continue

                first_name = _pick_first(row, "firstName", "first_name", default="") or ""
                last_name = _pick_first(row, "lastName", "last_name", default="") or ""
                full_name = _pick_first(row, "fullName", "full_name")
                if not full_name:
                    full_name = f"{first_name} {last_name}".strip() or None

                cur.execute("""
                    INSERT INTO player_receiving_stats (
                        season_index,
                        stage_index,
                        week,
                        roster_id,
                        team_id,
                        full_name,
                        rec_catches,
                        rec_yds,
                        rec_tds,
                        rec_yac,
                        rec_drops,
                        rec_long
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (season_index, stage_index, week, roster_id) DO UPDATE SET
                        team_id = EXCLUDED.team_id,
                        full_name = EXCLUDED.full_name,
                        rec_catches = EXCLUDED.rec_catches,
                        rec_yds = EXCLUDED.rec_yds,
                        rec_tds = EXCLUDED.rec_tds,
                        rec_yac = EXCLUDED.rec_yac,
                        rec_drops = EXCLUDED.rec_drops,
                        rec_long = EXCLUDED.rec_long
                """, (
                    int(season_index or 0),
                    int(stage_index or 0),
                    int(week or 0),
                    int(roster_id),
                    _pick_first(row, "teamId", "team_id"),
                    full_name,
                    _pick_first(row, "receptions", "catches", "recCatches", "rec_catches", default=0),
                    _pick_first(row, "recYds", "receivingYards", "yards", "rec_yds", default=0),
                    _pick_first(row, "recTDs", "receivingTDs", "touchdowns", "rec_tds", default=0),
                    _pick_first(row, "yac", "yardsAfterCatch", "recYAC", "rec_yac", default=0),
                    _pick_first(row, "drops", "recDrops", "rec_drops", default=0),
                    _pick_first(row, "long", "longest", "recLong", "rec_long", default=0),
                ))
        conn.commit()

    print(f"Imported receiving stats from {path.name}")


def main():
    init_db()

    standings_file = find_latest_file("standings")
    roster_files = find_all_files("roster")
    schedule_file = find_latest_file("schedule")
    receiving_files = find_all_files("receiving")

    if standings_file:
        try:
            import_standings_file(standings_file)
        except Exception as exc:
            print(f"Failed to import standings from {standings_file.name}: {exc}")
    else:
        print("No standings file found.")

    if roster_files:
        print(f"Found {len(roster_files)} roster files.")
        for roster_file in roster_files:
            try:
                import_roster_file(roster_file)
            except Exception as exc:
                print(f"Failed to import roster from {roster_file.name}: {exc}")
    else:
        print("No roster files found.")

    if schedule_file:
        try:
            import_schedule_file(schedule_file)
        except Exception as exc:
            print(f"Failed to import schedule from {schedule_file.name}: {exc}")
    else:
        print("No schedule file found.")

    if receiving_files:
        print(f"Found {len(receiving_files)} receiving files.")
        for receiving_file in receiving_files:
            try:
                import_receiving_file(receiving_file)
            except Exception as exc:
                print(f"Failed to import receiving stats from {receiving_file.name}: {exc}")
    else:
        print("No receiving files found.")

    print("Import complete.")


if __name__ == "__main__":
    main()
