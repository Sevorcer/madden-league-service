from __future__ import annotations

import discord
from discord.ext import commands

from commands.admin_commands import register_admin_commands
from config import load_settings
from database import Database


settings = load_settings()

intents = discord.Intents.default()
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
db = Database(settings)


@bot.event
async def on_ready() -> None:
    print(f"Logged in as {bot.user} ({bot.user.id if bot.user else 'unknown'})")
    try:
        db.initialize_schema()
        if settings.guild_ids:
            for guild_id in settings.guild_ids:
                guild = discord.Object(id=guild_id)
                synced = await bot.tree.sync(guild=guild)
                print(f"Synced {len(synced)} commands to guild {guild_id}")
        else:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} global commands")
    except Exception as exc:
        print(f"Startup sync/init error: {exc}")


register_admin_commands(bot, settings, db)


if __name__ == "__main__":
    if not settings.bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set.")
    bot.run(settings.bot_token)
