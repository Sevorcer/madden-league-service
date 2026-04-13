import json
import os
from datetime import datetime
from pathlib import Path

import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

app = FastAPI()

SAVE_DIR = Path("received_exports")
SAVE_DIR.mkdir(exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg.connect(DATABASE_URL)


def get_first(payload: dict, *keys, default=0):
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return default


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
            CREATE TABLE IF NOT EXISTS player_passing_stats (
                stat_id BIGINT PRIMARY KEY,
                roster_id BIGINT,
                team_id BIGINT,
                schedule_id BIGINT,
                season_index INTEGER,
                stage_index INTEGER,
                week_index INTEGER,
                full_name TEXT,
                pass_att INTEGER,
                pass_comp INTEGER,
                pass_comp_pct DOUBLE PRECISION,
                pass_ints INTEGER,
                pass_longest INTEGER,
                passer_rating DOUBLE PRECISION,
                pass_sacks INTEGER,
                pass_tds INTEGER,
                pass_yds INTEGER
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS player_rushing_stats (
                stat_id BIGINT PRIMARY KEY,
                roster_id BIGINT,
                team_id BIGINT,
                schedule_id BIGINT,
                season_index INTEGER,
                stage_index INTEGER,
                week_index INTEGER,
                full_name TEXT,
                rush_att INTEGER,
                rush_broken_tackles INTEGER,
                rush_fum INTEGER,
                rush_longest INTEGER,
                rush_tds INTEGER,
                rush_yds_after_contact INTEGER,
                rush_yds INTEGER
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS player_defense_stats (
                stat_id BIGINT PRIMARY KEY,
                roster_id BIGINT,
                team_id BIGINT,
                schedule_id BIGINT,
                season_index INTEGER,
                stage_index INTEGER,
                week_index INTEGER,
                full_name TEXT,
                def_sacks DOUBLE PRECISION,
                def_ints INTEGER,
                tackles INTEGER,
                tackles_for_loss INTEGER
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS player_receiving_stats (
                stat_id BIGINT PRIMARY KEY,
                roster_id BIGINT,
                team_id BIGINT,
                schedule_id BIGINT,
                season_index INTEGER,
                stage_index INTEGER,
                week_index INTEGER,
                full_name TEXT,
                rec_catches INTEGER,
                rec_drops INTEGER,
                rec_longest INTEGER,
                rec_tds INTEGER,
                rec_yds_after_catch INTEGER,
                rec_yds INTEGER
            )
            """)
        conn.commit()


def import_standings_data(data: dict) -> dict:
    standings_list = data.get("content", {}).get("teamStandingInfoList", [])
    if not standings_list:
        return {"imported": False, "type": "standings", "reason": "No standings data found"}

    imported = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for team in standings_list:
                team_id = team.get("teamId")
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
                    team.get("teamName"),
                    team.get("conferenceName"),
                    team.get("divisionName"),
                    team.get("teamOvr"),
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
                    team.get("totalWins", 0),
                    team.get("totalLosses", 0),
                    team.get("totalTies", 0),
                    team.get("winPct", 0.0),
                    team.get("seed", 0),
                    team.get("ptsFor", 0),
                    team.get("ptsAgainst", 0),
                    team.get("tODiff", 0),
                    team.get("offTotalYdsRank", 0),
                    team.get("defTotalYdsRank", 0),
                ))
                imported += 1
        conn.commit()

    return {"imported": True, "type": "standings", "rows": imported}


def import_roster_data(data: dict) -> dict:
    roster_list = data.get("content", {}).get("rosterInfoList", [])
    if not roster_list:
        return {"imported": False, "type": "roster", "reason": "No roster data found"}

    imported = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for player in roster_list:
                roster_id = player.get("rosterId")
                if roster_id is None:
                    continue

                first_name = player.get("firstName", "")
                last_name = player.get("lastName", "")
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
                    player.get("teamId"),
                    first_name,
                    last_name,
                    full_name,
                    player.get("position"),
                    player.get("age"),
                    player.get("overallRating"),
                    player.get("jerseyNum"),
                    player.get("yearsPro"),
                    player.get("height"),
                    player.get("weight"),
                    player.get("college"),
                    player.get("playerBestOvr"),
                    player.get("contractSalary"),
                    player.get("contractYearsLeft"),
                    1 if player.get("isFreeAgent") else 0,
                    player.get("injuryRating"),
                    player.get("speedRating"),
                    player.get("strengthRating"),
                    player.get("awarenessRating"),
                    player.get("throwPowerRating"),
                    player.get("breakTackleRating"),
                    player.get("manCoverRating"),
                    player.get("zoneCoverRating"),
                    player.get("catchRating"),
                    player.get("carryingRating"),
                    player.get("rookieYear"),
                ))
                imported += 1
        conn.commit()

    return {"imported": True, "type": "roster", "rows": imported}


def import_schedule_data(data: dict) -> dict:
    schedule_list = data.get("content", {}).get("gameScheduleInfoList", [])
    if not schedule_list:
        return {"imported": False, "type": "schedule", "reason": "No schedule data found"}

    imported = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for game in schedule_list:
                game_id = game.get("scheduleId")
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
                    game.get("scheduleId"),
                    game.get("seasonIndex"),
                    game.get("stageIndex"),
                    game.get("weekIndex"),
                    game.get("awayTeamId"),
                    game.get("homeTeamId"),
                    game.get("awayScore", 0),
                    game.get("homeScore", 0),
                    game.get("status"),
                    1 if game.get("isGameOfTheWeek") else 0,
                ))
                imported += 1
        conn.commit()

    return {"imported": True, "type": "schedule", "rows": imported}


def import_passing_data(data: dict) -> dict:
    rows = data.get("content", {}).get("playerPassingStatInfoList", [])
    if not rows:
        return {"imported": False, "type": "passing", "reason": "No passing data found"}

    imported = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in rows:
                stat_id = item.get("statId")
                if stat_id is None:
                    continue

                cur.execute("""
                    INSERT INTO player_passing_stats (
                        stat_id, roster_id, team_id, schedule_id, season_index, stage_index, week_index,
                        full_name, pass_att, pass_comp, pass_comp_pct, pass_ints, pass_longest,
                        passer_rating, pass_sacks, pass_tds, pass_yds
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (stat_id) DO UPDATE SET
                        roster_id = EXCLUDED.roster_id,
                        team_id = EXCLUDED.team_id,
                        schedule_id = EXCLUDED.schedule_id,
                        season_index = EXCLUDED.season_index,
                        stage_index = EXCLUDED.stage_index,
                        week_index = EXCLUDED.week_index,
                        full_name = EXCLUDED.full_name,
                        pass_att = EXCLUDED.pass_att,
                        pass_comp = EXCLUDED.pass_comp,
                        pass_comp_pct = EXCLUDED.pass_comp_pct,
                        pass_ints = EXCLUDED.pass_ints,
                        pass_longest = EXCLUDED.pass_longest,
                        passer_rating = EXCLUDED.passer_rating,
                        pass_sacks = EXCLUDED.pass_sacks,
                        pass_tds = EXCLUDED.pass_tds,
                        pass_yds = EXCLUDED.pass_yds
                """, (
                    stat_id,
                    item.get("rosterId"),
                    item.get("teamId"),
                    item.get("scheduleId"),
                    item.get("seasonIndex"),
                    item.get("stageIndex"),
                    item.get("weekIndex"),
                    item.get("fullName"),
                    item.get("passAtt", 0),
                    item.get("passComp", 0),
                    item.get("passCompPct", 0.0),
                    item.get("passInts", 0),
                    item.get("passLongest", 0),
                    item.get("passerRating", 0.0),
                    item.get("passSacks", 0),
                    item.get("passTDs", 0),
                    item.get("passYds", 0),
                ))
                imported += 1
        conn.commit()

    return {"imported": True, "type": "passing", "rows": imported}


def import_rushing_data(data: dict) -> dict:
    rows = data.get("content", {}).get("playerRushingStatInfoList", [])
    if not rows:
        return {"imported": False, "type": "rushing", "reason": "No rushing data found"}

    imported = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in rows:
                stat_id = item.get("statId")
                if stat_id is None:
                    continue

                cur.execute("""
                    INSERT INTO player_rushing_stats (
                        stat_id, roster_id, team_id, schedule_id, season_index, stage_index, week_index,
                        full_name, rush_att, rush_broken_tackles, rush_fum, rush_longest,
                        rush_tds, rush_yds_after_contact, rush_yds
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (stat_id) DO UPDATE SET
                        roster_id = EXCLUDED.roster_id,
                        team_id = EXCLUDED.team_id,
                        schedule_id = EXCLUDED.schedule_id,
                        season_index = EXCLUDED.season_index,
                        stage_index = EXCLUDED.stage_index,
                        week_index = EXCLUDED.week_index,
                        full_name = EXCLUDED.full_name,
                        rush_att = EXCLUDED.rush_att,
                        rush_broken_tackles = EXCLUDED.rush_broken_tackles,
                        rush_fum = EXCLUDED.rush_fum,
                        rush_longest = EXCLUDED.rush_longest,
                        rush_tds = EXCLUDED.rush_tds,
                        rush_yds_after_contact = EXCLUDED.rush_yds_after_contact,
                        rush_yds = EXCLUDED.rush_yds
                """, (
                    stat_id,
                    item.get("rosterId"),
                    item.get("teamId"),
                    item.get("scheduleId"),
                    item.get("seasonIndex"),
                    item.get("stageIndex"),
                    item.get("weekIndex"),
                    item.get("fullName"),
                    item.get("rushAtt", 0),
                    item.get("rushBrokenTackles", 0),
                    item.get("rushFum", 0),
                    item.get("rushLongest", 0),
                    item.get("rushTDs", 0),
                    item.get("rushYdsAfterContact", 0),
                    item.get("rushYds", 0),
                ))
                imported += 1
        conn.commit()

    return {"imported": True, "type": "rushing", "rows": imported}


def import_defense_data(data: dict) -> dict:
    rows = data.get("content", {}).get("playerDefensiveStatInfoList", [])
    if not rows:
        return {"imported": False, "type": "defense", "reason": "No defense data found"}

    imported = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in rows:
                stat_id = item.get("statId")
                if stat_id is None:
                    continue

                cur.execute("""
                    INSERT INTO player_defense_stats (
                        stat_id, roster_id, team_id, schedule_id, season_index, stage_index, week_index,
                        full_name, def_sacks, def_ints, tackles, tackles_for_loss
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (stat_id) DO UPDATE SET
                        roster_id = EXCLUDED.roster_id,
                        team_id = EXCLUDED.team_id,
                        schedule_id = EXCLUDED.schedule_id,
                        season_index = EXCLUDED.season_index,
                        stage_index = EXCLUDED.stage_index,
                        week_index = EXCLUDED.week_index,
                        full_name = EXCLUDED.full_name,
                        def_sacks = EXCLUDED.def_sacks,
                        def_ints = EXCLUDED.def_ints,
                        tackles = EXCLUDED.tackles,
                        tackles_for_loss = EXCLUDED.tackles_for_loss
                """, (
                    stat_id,
                    item.get("rosterId"),
                    item.get("teamId"),
                    item.get("scheduleId"),
                    item.get("seasonIndex"),
                    item.get("stageIndex"),
                    item.get("weekIndex"),
                    item.get("fullName"),
                    float(get_first(item, "defSacks", "sacks", "sacksMade", default=0)),
                    int(get_first(item, "defInts", "ints", "interceptions", default=0)),
                    int(get_first(item, "defTotalTackles", "tackles", default=0)),
                    int(get_first(item, "tacklesForLoss", "defTfl", default=0)),
                ))
                imported += 1
        conn.commit()

    return {"imported": True, "type": "defense", "rows": imported}



def import_receiving_data(data: dict) -> dict:
    rows = data.get("content", {}).get("playerReceivingStatInfoList", [])
    if not rows:
        return {"imported": False, "type": "receiving", "reason": "No receiving data found"}

    imported = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in rows:
                stat_id = item.get("statId")
                if stat_id is None:
                    continue

                cur.execute("""
                    INSERT INTO player_receiving_stats (
                        stat_id, roster_id, team_id, schedule_id, season_index, stage_index, week_index,
                        full_name, rec_catches, rec_drops, rec_longest, rec_tds, rec_yds_after_catch, rec_yds
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (stat_id) DO UPDATE SET
                        roster_id = EXCLUDED.roster_id,
                        team_id = EXCLUDED.team_id,
                        schedule_id = EXCLUDED.schedule_id,
                        season_index = EXCLUDED.season_index,
                        stage_index = EXCLUDED.stage_index,
                        week_index = EXCLUDED.week_index,
                        full_name = EXCLUDED.full_name,
                        rec_catches = EXCLUDED.rec_catches,
                        rec_drops = EXCLUDED.rec_drops,
                        rec_longest = EXCLUDED.rec_longest,
                        rec_tds = EXCLUDED.rec_tds,
                        rec_yds_after_catch = EXCLUDED.rec_yds_after_catch,
                        rec_yds = EXCLUDED.rec_yds
                """, (
                    stat_id,
                    item.get("rosterId"),
                    item.get("teamId"),
                    item.get("scheduleId"),
                    item.get("seasonIndex"),
                    item.get("stageIndex"),
                    item.get("weekIndex"),
                    item.get("fullName"),
                    item.get("recCatches", 0),
                    item.get("recDrops", 0),
                    item.get("recLongest", 0),
                    item.get("recTDs", 0),
                    item.get("recYdsAfterCatch", 0),
                    item.get("recYds", 0),
                ))
                imported += 1
        conn.commit()

    return {"imported": True, "type": "receiving", "rows": imported}


def try_import_data(data: dict) -> dict:
    if not isinstance(data, dict):
        return {"imported": False, "reason": "Body was not JSON object"}

    content = data.get("content", data)
    if not isinstance(content, dict):
        return {"imported": False, "reason": "JSON missing content"}

    if "teamStandingInfoList" in content:
        return import_standings_data({"content": content})
    if "rosterInfoList" in content:
        return import_roster_data({"content": content})
    if "gameScheduleInfoList" in content:
        return import_schedule_data({"content": content})
    if "playerPassingStatInfoList" in content:
        return import_passing_data({"content": content})
    if "playerRushingStatInfoList" in content:
        return import_rushing_data({"content": content})
    if "playerDefensiveStatInfoList" in content:
        return import_defense_data({"content": content})
    if "playerReceivingStatInfoList" in content:
        return import_receiving_data({"content": content})

    return {"imported": False, "reason": "No recognized import payload found"}


def parse_file_json(path: Path):
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        return json.loads(text)
    except Exception:
        return None


def reimport_matching_files(kind: str):
    files = sorted(
        [f for f in SAVE_DIR.glob("*") if f.is_file()],
        key=lambda p: p.stat().st_mtime
    )

    matched = 0
    imported = 0
    skipped = 0
    failed = []

    for f in files:
        name = f.name.lower()

        if kind == "schedules":
            if "schedule" not in name and "schedules" not in name:
                continue
        elif kind == "stats":
            if not any(token in name for token in ["passing", "rushing", "defense", "receiving"]):
                continue
        elif kind == "all":
            pass
        else:
            continue

        matched += 1
        data = parse_file_json(f)
        if data is None:
            skipped += 1
            continue

        try:
            result = try_import_data(data)
            if result.get("imported"):
                imported += 1
            else:
                skipped += 1
        except Exception as exc:
            failed.append({"file": f.name, "error": str(exc)})

    return {
        "kind": kind,
        "matched_files": matched,
        "imported_files": imported,
        "skipped_files": skipped,
        "failed_files": failed,
    }


@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/")
async def home():
    return {"status": "working"}


async def save_request(request: Request, route_name: str):
    body = await request.body()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    safe_route = route_name.replace("/", "_")
    filename = SAVE_DIR / f"{safe_route}_{timestamp}.txt"

    with open(filename, "wb") as f:
        f.write(body if body else b"")

    import_result = None
    if body:
        try:
            parsed = json.loads(body.decode("utf-8", errors="replace"))
            import_result = try_import_data(parsed)
        except Exception as exc:
            import_result = {"imported": False, "reason": f"Import failed: {exc}"}

    return {
        "status": "received",
        "route": route_name,
        "saved_as": str(filename),
        "import_result": import_result,
    }


@app.get("/exports")
async def list_exports():
    files = sorted(
        [f for f in SAVE_DIR.glob("*") if f.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    return {
        "count": len(files),
        "files": [
            {
                "name": f.name,
                "size": f.stat().st_size,
                "modified": datetime.utcfromtimestamp(f.stat().st_mtime).isoformat() + "Z"
            }
            for f in files[:100]
        ]
    }


@app.get("/exports/latest")
async def latest_export():
    files = sorted(
        [f for f in SAVE_DIR.glob("*") if f.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if not files:
        raise HTTPException(status_code=404, detail="No exports found")

    f = files[0]
    content = f.read_text(encoding="utf-8", errors="replace")

    try:
        parsed = json.loads(content)
        return JSONResponse(content={"file": f.name, "content": parsed})
    except json.JSONDecodeError:
        return PlainTextResponse(f"FILE: {f.name}\n\n{content}")


@app.get("/exports/view", response_class=HTMLResponse)
async def view_exports():
    files = sorted(
        [f for f in SAVE_DIR.glob("*") if f.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    rows = []
    for f in files[:100]:
        rows.append(
            f"""
            <tr>
                <td><a href="/exports/{f.name}">{f.name}</a></td>
                <td>{f.stat().st_size}</td>
                <td>{datetime.utcfromtimestamp(f.stat().st_mtime).isoformat()}Z</td>
            </tr>
            """
        )

    html = f"""
    <html>
    <head>
        <title>Madden Exports</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #111;
                color: #eee;
                padding: 24px;
            }}
            a {{ color: #66b3ff; }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                border: 1px solid #333;
                padding: 10px;
                text-align: left;
            }}
            th {{
                background: #222;
            }}
            tr:nth-child(even) {{
                background: #181818;
            }}
            .links a {{
                margin-right: 16px;
            }}
        </style>
    </head>
    <body>
        <h1>Madden Export Receiver</h1>
        <div class="links">
            <a href="/exports">/exports</a>
            <a href="/exports/latest">/exports/latest</a>
            <a href="/reimport/schedules">/reimport/schedules</a>
            <a href="/reimport/stats">/reimport/stats</a>
            <a href="/reimport/all">/reimport/all</a>
        </div>
        <table>
            <thead>
                <tr>
                    <th>File</th>
                    <th>Size</th>
                    <th>Modified</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows) if rows else '<tr><td colspan="3">No exports found</td></tr>'}
            </tbody>
        </table>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/exports/{filename}")
async def get_export(filename: str):
    f = SAVE_DIR / filename
    if not f.exists() or not f.is_file():
        raise HTTPException(status_code=404, detail="Export not found")

    content = f.read_text(encoding="utf-8", errors="replace")

    try:
        parsed = json.loads(content)
        return JSONResponse(content={"file": f.name, "content": parsed})
    except json.JSONDecodeError:
        return PlainTextResponse(f"FILE: {f.name}\n\n{content}")


@app.get("/reimport/schedules")
async def reimport_schedules():
    return reimport_matching_files("schedules")


@app.get("/reimport/stats")
async def reimport_stats():
    return reimport_matching_files("stats")


@app.get("/reimport/all")
async def reimport_all():
    return reimport_matching_files("all")


@app.api_route("/xbsx/{league_id}/standings", methods=["GET", "POST"])
async def standings(league_id: str, request: Request):
    return await save_request(request, f"xbsx_{league_id}_standings")


@app.api_route("/xbsx/{league_id}/freeagents/roster", methods=["GET", "POST"])
async def freeagents_roster(league_id: str, request: Request):
    return await save_request(request, f"xbsx_{league_id}_freeagents_roster")


@app.api_route("/xbsx/{league_id}/week/{season_type}/{week}/team", methods=["GET", "POST"])
async def weekly_team(league_id: str, season_type: str, week: str, request: Request):
    return await save_request(request, f"xbsx_{league_id}_week_{season_type}_{week}_team")


@app.api_route("/xbsx/{league_id}/week/{season_type}/{week}/stats", methods=["GET", "POST"])
async def weekly_stats(league_id: str, season_type: str, week: str, request: Request):
    return await save_request(request, f"xbsx_{league_id}_week_{season_type}_{week}_stats")


@app.api_route("/xbsx/{league_id}/{full_path:path}", methods=["GET", "POST"])
async def catch_all_xbsx(league_id: str, full_path: str, request: Request):
    return await save_request(request, f"xbsx_{league_id}_{full_path}")
