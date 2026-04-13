import discord
from discord.ext import commands

from config import settings
from database import Database
from commands.admin_commands import register_admin_commands
from commands.league_commands import register_league_commands
from commands.media_commands import register_media_commands
from commands.trade_commands import register_trade_commands
from commands.ops_commands import register_ops_commands

intents = discord.Intents.default()
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
db = Database(settings)

register_admin_commands(bot, db)
register_league_commands(bot, db)
register_media_commands(bot, db)
register_trade_commands(bot, db)
register_ops_commands(bot, db)


@bot.event
async def on_ready() -> None:
    print(f"Logged in as {bot.user} ({bot.user.id})")

    try:
        db.initialize_schema()
        print("Database initialized successfully.")
    except Exception as exc:
        print(f"Database init error: {exc}")

    try:
        if settings.guild_ids:
            for guild_id in settings.guild_ids:
                guild = discord.Object(id=guild_id)
                bot.tree.copy_global_to(guild=guild)
                synced = await bot.tree.sync(guild=guild)
                print(f"Synced {len(synced)} commands to guild {guild_id}")
        else:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} global commands")
    except Exception as exc:
        print(f"Startup sync/init error: {exc}")


def main() -> None:
    if not settings.bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set.")
    bot.run(settings.bot_token)


if __name__ == "__main__":
    main()
