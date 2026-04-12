from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    bot_token: str
    database_url: str
    guild_ids: list[int]
    openai_api_key: str
    openai_model: str
    auto_post_weekly_news: bool
    auto_post_matchup_previews: bool
    auto_post_game_recaps: bool
    trade_required_approvals: int
    trade_required_denials: int


def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_guild_ids(raw: str) -> list[int]:
    if not raw:
        return []
    ids: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        ids.append(int(item))
    return ids


def load_settings() -> Settings:
    return Settings(
        bot_token=os.getenv("DISCORD_BOT_TOKEN", ""),
        database_url=os.getenv("DATABASE_URL", ""),
        guild_ids=_parse_guild_ids(os.getenv("GUILD_IDS", "")),
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4-mini"),
        auto_post_weekly_news=_parse_bool(os.getenv("AUTO_POST_WEEKLY_NEWS"), True),
        auto_post_matchup_previews=_parse_bool(os.getenv("AUTO_POST_MATCHUP_PREVIEWS"), True),
        auto_post_game_recaps=_parse_bool(os.getenv("AUTO_POST_GAME_RECAPS"), False),
        trade_required_approvals=int(os.getenv("TRADE_REQUIRED_APPROVALS", "2")),
        trade_required_denials=int(os.getenv("TRADE_REQUIRED_DENIALS", "2")),
    )
