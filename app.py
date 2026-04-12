import discord
from discord.ext import commands

from config import settings
from database import Database

intents = discord.Intents.default()
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
db = Database(settings)


@bot.tree.command(name="ping", description="Check if the bot is online.")
async def ping(interaction: discord.Interaction) -> None:
    await interaction.response.send_message("Pong!")


@bot.tree.command(name="healthcheck", description="Check bot and database health.")
async def healthcheck(interaction: discord.Interaction) -> None:
    try:
        result = db.healthcheck()
        server_time = result.get("server_time", "unknown")
        await interaction.response.send_message(
            f"✅ Bot is online. Database connected. Server time: {server_time}"
        )
    except Exception as exc:
        await interaction.response.send_message(
            f"❌ Database healthcheck failed: {exc}"
        )


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
                synced = await bot.tree.sync(guild=guild)
                print(f"Synced {len(synced)} commands to guild {guild_id}")
        else:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} global commands")
    except Exception as exc:
        print(f"Startup sync/init error: {exc}")


def main() -> None:
    if not settings.discord_bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set.")
    bot.run(settings.discord_bot_token)


if __name__ == "__main__":
    main()