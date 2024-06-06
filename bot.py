import os
import json
import re
import requests
import logging
from dotenv import load_dotenv
import asyncpg
logging.getLogger('twitchio').setLevel(logging.CRITICAL)
from twitchio.ext import commands, eventsub
import twitchio
import asyncio
from typing import Union, Optional, get_args
from zoneinfo import ZoneInfo
import time
from datetime import datetime, timedelta
import calendar

load_dotenv()

logging.basicConfig(filename='bot.log', level=logging.INFO, format='%(message)s', encoding='utf-8')

berlin_zone = ZoneInfo("Europe/Berlin")

log_sites = [
  'https://logs.ivr.fi',
  'https://logs.2807.eu',
  'https://logsback.susgee.dev',
  'https://logs.spanix.team',
  'https://logs.nadeko.net',
  'https://log.spofoh.de'
]

frontend_url = 'https://logs.lucas19961.de'

def search_logs(channel_name):
    available_logs = []

    for site in log_sites:
        response = requests.get(f'{site}/channels')

        if response.status_code == 200:
            try:
                data = response.json()
                channels = [channel['name'] for channel in data['channels']]

                if channel_name.lower() in channels:
                    if 'logsback.susgee.dev' in site:
                        available_logs.append(f'{frontend_url}/?channel={channel_name}')
                    else:
                        available_logs.append(f'{site}/?channel={channel_name}')
            except ValueError:
                print(f'Warnung: Die Antwort von {site}/channels konnte nicht als JSON interpretiert werden.')
        else:
            print(f'Warnung: Anfrage an {site}/channels hat den Statuscode {response.status_code} zurückgegeben.')

    return available_logs

esbot = commands.Bot.from_client_credentials(client_id=os.getenv('Twitch_App_ID'),
                                         client_secret=os.getenv('Twitch_App_Token'))

# Erstellen Sie eine Instanz des EventSubClients
esclient = eventsub.EventSubClient(esbot,
                                   webhook_secret=os.getenv('webhook_secret_pw'),
                                   callback_route='https://eventsub.spofoh.de/callback')

class Bot(commands.Bot):

    def __init__(self):
        with open('blacklist.json') as f:
            data = json.load(f)
            self.blacklist = [word.lower() for word in data['blacklist']]
        if not os.path.exists('channels.json'):
            with open('channels.json', 'w') as f:
                json.dump([os.getenv('Not_leaveable')], f)
        with open('channels.json', 'r') as f:
            channels = json.load(f)
        super().__init__(token=os.getenv('Twitch_Generator_Token'), client_id=os.getenv('Twitch_Generator_ID'), prefix='+',
                         initial_channels=channels)
        self.live_channels_today = set()
        
    async def __ainit__(self) -> None:
        with open('channels.json', 'r') as f:
            channels = json.load(f)
        self.loop.create_task(esclient.listen(port=4000))

        broadcaster_id = await self.fetch_users(names=channels,  token = os.getenv('Twitch_Generator_Token'))
        for broad_id in broadcaster_id:
            try:
                await esclient.subscribe_channel_stream_start(broadcaster=broad_id.id)
            except twitchio.HTTPException:
                pass

    async def get_mods(self, channel):
        url = "https://gql.twitch.tv/gql"
        payload = "[{\"operationName\":\"Mods\",\"variables\":{\"login\":\"" + channel + "\"},\"extensions\":{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"cb912a7e0789e0f8a4c85c25041a08324475831024d03d624172b59498caf085\"}}}]"
        headers = {
            'client-id': 'kimne78kx3ncx6brgo4mv6wki5h1ko',
            'Content-Type': 'text/plain'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        data = json.loads(response.text)
        if 0 in data and 'data' in data[0] and 'user' in data[0]['data'] and 'mods' in data[0]['data']['user'] and 'edges' in data[0]['data']['user']['mods']:
            mods = [edge['node']['login'] for edge in data[0]['data']['user']['mods']['edges']] if data[0]['data']['user']['mods']['edges'] else []
        else:
            mods = []
        mods.append(channel)
        return mods

    async def create_database_tables(self):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'), loop=asyncio.get_event_loop())

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_monthly_stats (
                id SERIAL PRIMARY KEY,
                channel_name VARCHAR(255) NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                live_days INT DEFAULT 0,
                UNIQUE (channel_name, year, month)
            )
        """)

        await conn.close()

    async def update_live_days(self, channel_name):
        today = datetime.now().date()
        month = today.month
        year = today.year

        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'), loop=asyncio.get_event_loop())

        result = await conn.fetchrow(
    "SELECT id, live_days FROM channel_monthly_stats WHERE channel_name=$1 AND month=$2 AND year=$3",
    channel_name.lower(), month, year
)
        print(result)

        if result:
            new_live_days = result['live_days'] + 1
            await conn.execute(
                "UPDATE channel_monthly_stats SET live_days=$1 WHERE id=$2",
                new_live_days, result['id']
            )
        else:
            await conn.execute(
                "INSERT INTO channel_monthly_stats (channel_name, month, year, live_days) VALUES ($1, $2, $3, 1)",
                channel_name.lower(), month, year
            )


        await conn.close()

    @esbot.event()
    async def event_eventsub_notification_stream_start(event: eventsub.StreamOnlineData) -> None:
        print(f'Stream gestartet: {event.data.broadcaster.name}')
        esclient.delete_all_active_subscriptions()
        channel_name = event.data.broadcaster.name
        # Überprüfen, ob der Kanal bereits heute live war
        if channel_name not in bot.live_channels_today:
            bot.live_channels_today.add(channel_name)
            print(bot.live_channels_today)
            await bot.update_live_days(channel_name)

    async def event_ready(self):
        print(f'Ready | {self.nick}')

    async def event_message(self, message):
        if message.echo:
            return
        await self.handle_commands(message)
    
    @commands.command(name='status')
    async def status(self, ctx):
        if ctx.author.name.lower() == os.getenv('Bot_Admin'):
            subscriptions = await esclient.get_subscriptions()
            for sub in subscriptions:
                broadcaster_id = sub.condition.get('from_broadcaster_user_id')
                if broadcaster_id:
                    print(f"Abonnement ID: {sub.id}, Kanal: {broadcaster_id}, Typ: {sub.type}")
                else:
                    print(f"Abonnement ID: {sub.id} hat keine Broadcaster-ID. Typ: {sub.type}")

    @commands.command(name='join')
    @commands.cooldown(rate=1, per=5, bucket=commands.Bucket.channel)
    async def join(self, ctx, channel: str = None):
        if channel is None:
            channel = ctx.author.name.lower()
        mods = await self.get_mods(channel)
        if ctx.author.name.lower() == os.getenv('Bot_Admin') or ctx.author.name.lower() in mods:
            with open('channels.json', 'r') as f:
                channels = json.load(f)
            if channel not in channels:
                channels.append(channel.lower())
                with open('channels.json', 'w') as f:
                    json.dump(channels, f)
                await self.join_channels([channel])
                broadcaster_id = await self.fetch_users(names=[channel])
                await esclient.subscribe_channel_stream_start(broadcaster=broadcaster_id[0].id)
                await ctx.reply(f"/me Beigetreten zum Kanal: {channel}")
            else:
                await ctx.reply(f"/me Ich bin bereits dem Kanal {channel} beigetreten.")
        elif ctx.author.name.lower() not in mods and ctx.author.name.lower() != os.getenv('Bot_Admin'):
            await ctx.send("Nur der Streamer und die Moderatoren können den Bot einem Kanal hinzufügen.")
            return

    @commands.command(name='leave')
    @commands.cooldown(rate=1, per=5, bucket=commands.Bucket.channel)
    async def leave(self, ctx, channel: str = None):
        if channel is None:
            channel = ctx.author.name.lower()
        if channel.lower() == os.getenv('Not_leaveable'):
            await ctx.reply(f"/me Der Bot kann den Kanal {channel.lower()} nicht verlassen.")
            return
        mods = await self.get_mods(channel)
        if ctx.author.name.lower() == os.getenv('Bot_Admin'):
            with open('channels.json', 'r') as f:
                channels = json.load(f)
            if channel in channels:
                channels.remove(channel.lower())
                await ctx.reply(f"/me Verlassen des Kanals: {channel}")
                with open('channels.json', 'w') as f:
                    json.dump(channels, f)
                await self.part_channels([channel])
                broadcaster_id = await self.fetch_users(names=[channel])
                subscriptions = await esclient.get_subscriptions(user_id=broadcaster_id[0].id)
                for subscription in subscriptions:
                    await esclient.delete_subscription(subscription_id=subscription.id)
            else:
                await ctx.reply(f"/me Ich bin in dem Channel nicht.")
        elif ctx.author.name.lower() not in mods and ctx.author.name.lower() != os.getenv('Bot_Admin'):
            await ctx.reply("/me Nur der Streamer und die Moderatoren können den Bot entfernen.")
            return
        elif ctx.channel.name == ctx.author.name.lower() and ctx.author.name.lower() in mods:
            with open('channels.json', 'r') as f:
                channels = json.load(f)
            if channel in channels:
                channels.remove(channel.lower())
                await ctx.reply(f"/me Verlassen des Kanals: {channel}")
                with open('channels.json', 'w') as f:
                    json.dump(channels, f)
                await self.part_channels([channel])
                broadcaster_id = await self.fetch_users(names=[channel])
                subscriptions = await esclient.get_subscriptions(user_id=broadcaster_id[0].id)
                for subscription in subscriptions:
                    await esclient.delete_subscription(subscription_id=subscription.id)
        else:
            await ctx.reply('/me Du kannst den Bot nur in deinem Channel entfernen')

    @commands.command(name='mostplayed')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def mostplayed(self, ctx, streamer_name: Optional[str] = None, num_games: int = 5):
        if num_games > 10:
            num_games = 10
        elif num_games < 1:
            num_games = 1

        if streamer_name is None:
            streamer_name = ctx.channel.name

        url = f"https://sullygnome.com/api/standardsearch/{streamer_name}/false/true/false/false"
        response = requests.get(url)
        data = response.json()
        if not data:
            await ctx.reply("/me ⚠️Der gesuchte Streamer wurde nicht gefunden!⚠️")
            return
        
        streamer_id = data[0]['value']
        safe_streamer_name = data[0]['displaytext']

        url = f"https://sullygnome.com/api/tables/channeltables/games/365/{streamer_id}/%20/1/2/desc/0/100"
        response = requests.get(url)
        data = response.json()
        
        if not data['data']:
            await ctx.reply(f"/me ⚠️{safe_streamer_name} hat noch kein Spiel gespielt oder wird noch nicht getrackt.⚠️")
            return

        num_games = min(num_games, len(data['data']))
        messages = []
        current_message = f"{safe_streamer_name}: "
        for i in range(num_games):
            game = data['data'][i]
            game_name = game['gamesplayed'].split('|')[0]
            if '.' in game_name:
                game_name = game_name.replace('.', '(.)')
            stream_time = round(game['streamtime'] / 60, 1)
            total_stream_time = game['channelstreamtime'] / 60
            percentage = round((stream_time / total_stream_time) * 100, 1)

            if stream_time.is_integer():
                stream_time = int(stream_time)
            if percentage.is_integer():
                percentage = int(percentage)

            new_line = f"{i+1}. {stream_time} Stunden ({percentage}%) {game_name}"
            if len(current_message + new_line + " | ") > 500:
                messages.append(current_message.rstrip(" | "))
                current_message = new_line + " | "
            else:
                current_message += new_line + " | "

        messages.append(current_message.rstrip(" | "))

        for message in messages:
            await ctx.reply('/me ' + message)
            await asyncio.sleep(0.5)

    @commands.command(name='bayrisch')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def bayrisch(self, ctx, *, message=None):
        if message is None:
            await ctx.reply("/me Bitte gib eine Nachricht ein, die übersetzt werden soll.")
            return
        else:
            url = "https://translator-ai.onrender.com/"
            payload = json.dumps({
                "prompt": f"Übersetze \"{message}\" aus Deutsch in den deutschen Dialekt bairisch."
            })
            headers = {
                'content-type': 'application/json',
                'origin': 'https://de.cdn.mr-dialect.com',
                'referer': 'https://de.cdn.mr-dialect.com/'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            response_json = json.loads(response.text)
            translated_message = response_json['bot'].strip('"')

            cleaned_message = re.sub(r'\W+', '', translated_message.lower())
            if any(badword in cleaned_message for badword in self.blacklist):
                await ctx.reply("Blacklist-Wort in der Nachricht enthalten.")
                logging.info(f'\nFrage: {datetime.now().strftime("%d.%m.%Y %H:%M:%S")} {ctx.author.name}: "{message}"')
                logging.info(f'Antwort: {translated_message}')
            else:
                await ctx.reply('/me ' + translated_message)

    @commands.command(name='ösi')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def oesi(self, ctx, *, message=None):
        if message is None:
            await ctx.reply("/me Bitte gib eine Nachricht ein, die übersetzt werden soll.")
            return
        else:
            url = "https://translator-ai.onrender.com/"
            payload = json.dumps({
                "prompt": f"Übersetze \"{message}\" aus Deutsch in den deutschen Dialekt oesterreichisch."
            })
            headers = {
                'content-type': 'application/json',
                'origin': 'https://de.cdn.mr-dialect.com',
                'referer': 'https://de.cdn.mr-dialect.com/'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            response_json = json.loads(response.text)
            translated_message = response_json['bot'].strip('"')

            # Überprüfen Sie, ob die Antwort ein verbotenes Wort enthält
            cleaned_message = re.sub(r'\W+', '', translated_message.lower())
            if any(badword in cleaned_message for badword in self.blacklist):
                await ctx.reply("Blacklist-Wort in der Nachricht enthalten.")
                logging.info(f'\nFrage: {datetime.now().strftime("%d.%m.%Y %H:%M:%S")} {ctx.author.name}: "{message}"')
                logging.info(f'Antwort: {translated_message}')
            else:
                await ctx.reply('/me ' + translated_message)

    @commands.command(name='freegames')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def freegames(self, ctx):
        url = "https://store-site-backend-static-ipv4.ak.epicgames.com/freeGamesPromotions?locale=en-US&country=DE&allowCountries=DE"
        headers = {}
        response = requests.request("GET", url, headers=headers)
        data = json.loads(response.text)
        free_games = []
        for element in data['data']['Catalog']['searchStore']['elements']:
            if element['status'] == 'ACTIVE' and element['offerType'] == 'OTHERS' and any(category['path'] == 'freegames' or category['path'] == 'games' for category in element['categories']):
                effective_date = datetime.strptime(element['effectiveDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
                if effective_date < datetime.utcnow():
                    free_games.append(element['title'])
        await ctx.reply(f"/me Die momentanen Free Games auf Epic: {', '.join(free_games)}")

    @commands.command(name='commands')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def list_commands(self, ctx):
        hidden_commands = ['commands', 'status']
        available_commands = [command for command in self.commands.keys()]
        for hidden_command in hidden_commands:
            if hidden_command in available_commands:
                available_commands.remove(hidden_command)
        await ctx.reply(f"/me Die verfügbaren Befehle sind: {', '.join(available_commands)}")

    @commands.command(name='searchlogs', aliases=['srlogs', 'slogs', 'searchlog', 'slog'])
    @commands.cooldown(rate=1, per=10, bucket=commands.Bucket.channel)
    async def searchlogs(self, ctx, channel_name=None):
        if channel_name is None:
            channel_name = ctx.channel.name

        cleaned_channel_name = re.sub(r'\W+', '', channel_name.lower())
        if any(badword in cleaned_channel_name for badword in self.blacklist):
            await ctx.reply("/me Blacklist-Wort im Kanalnamen enthalten.")
            return

        logs = search_logs(channel_name)

        if logs:
            await ctx.reply(f'/me Die Logs vom Channel: {channel_name} sind auf den folgenden Seiten verfügbar: {" ".join(logs)}')
        else:
            await ctx.reply(f'/me Keine Logs gefunden für den Channel: {channel_name}')

    @commands.command(name='offdays')
    @commands.cooldown(rate=1, per=10, bucket=commands.Bucket.channel)
    async def offdays_command(self, ctx, month: Optional[str], year: Optional[str], channel_name: Optional[str]):
        month_mapping = {
        'januar': 1, 'februar': 2, 'märz': 3, 'april': 4, 'mai': 5, 'juni': 6,
        'juli': 7, 'august': 8, 'september': 9, 'oktober': 10, 'november': 11, 'dezember': 12
        }

        if month is None:
            month = datetime.now().month
        elif month.lower() in month_mapping:
            month = int(month_mapping[month.lower()])
        if year is None:
            year = datetime.now().year
        if channel_name is None:
            channel_name = ctx.channel.name

        month = int(month)
        year = int(year)

        days_in_month = calendar.monthrange(int(year), int(month))[1]

        if month == datetime.now().month and year == datetime.now().year:
            days_in_month = datetime.now().day

        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        result = await conn.fetchrow(
            "SELECT live_days FROM channel_monthly_stats WHERE LOWER(channel_name) = LOWER($1) AND month=$2 AND year=$3",
            channel_name.lower(), month, year
        )
        await conn.close()

        if result is None:
            await ctx.reply('/me Keine Daten zu diesem Zeitpunkt oder der Streamer wird nicht getracked.')
            return
        else:
            live_days = result['live_days']

        offdays = days_in_month - live_days

        if month == datetime.now().month and year == datetime.now().year:
            await ctx.reply(f"/me Offdays für diesen Monat im Channel {channel_name}: {offdays} ({live_days}/{days_in_month})")
        else:
            month_names = ["Januar", "Februar", "März", "April", "Mai", "Juni",
                        "Juli", "August", "September", "Oktober", "November", "Dezember"]
            month_name = month_names[month - 1]
            await ctx.reply(f"/me Offdays für {month_name} im Channel {channel_name}: {offdays} ({live_days}/{days_in_month})")

    @commands.command(name='restreams')
    @commands.cooldown(rate=1, per=5, bucket=commands.Bucket.channel)
    async def restreams(self, ctx, streamer_name: str, *time_parts):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'))
        if time_parts:
            mods = await self.get_mods(os.getenv('Bot_Admin'))
            print(mods)
            if ctx.author.name not in mods:
                await ctx.reply('/me Nur Moderatoren können die Zeit hinzufügen.')
                return
            print(time_parts)
            time = " ".join(time_parts)
            parts = time.lower().split()
            hours = 0
            minutes = 0
            seconds = 0
            for part in parts:
                if "h" in part:
                    print("nur h detected")
                    hours = part.split("h")[0]
                elif "m" in part:
                    print("nur m detected")
                    minutes = part.split("m")[0]
                elif "s" in part:
                    print("nur s detected")
                    seconds = part.split("s")[0]
                time_in_seconds = int(minutes) * 60 + int(hours) * 3600 + int(seconds)
                print(time_in_seconds)
            print(streamer_name)
            streamer_twitch_id = await self.fetch_users(names=[streamer_name])
            if not streamer_twitch_id:
                await ctx.reply('/me Kein Kanal gefunden mit diesem Namen')
                return
            print(streamer_twitch_id[0].id)
            await conn.execute('''
                INSERT INTO twitch_channels(channel_id, watch_time) VALUES($1, $2)
                ON CONFLICT (channel_id) DO UPDATE SET watch_time = twitch_channels.watch_time + $2
            ''', streamer_twitch_id[0].id, time_in_seconds)
            await conn.close()
            await ctx.reply(f'/me Zeit wurde hinzugefügt.')
        else:
            streamer_twitch_id = await self.fetch_users(names=[streamer_name])
            if not streamer_twitch_id:
                await ctx.reply('/me Kein Kanal gefunden mit diesem Namen.')
                return
            seconds = await conn.fetchval('SELECT watch_time FROM twitch_channels WHERE channel_id = $1', streamer_twitch_id[0].id)
            if not seconds:
                await ctx.reply('/me Keine Informationen zu diesem Benutzer.')
                return
            await conn.close()
            hours, remainder = divmod(seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            time_str = ""
            parts = []
            if hours > 0:
                if hours == 1:
                    parts.append(f"{hours} Stunde")
                else:
                    parts.append(f"{hours} Stunden")
            if minutes > 0:
                if minutes == 1:
                    parts.append(f"{minutes} Minute")
                else:
                    parts.append(f"{minutes} Minuten")
            if seconds > 0:
                if seconds == 1:
                    parts.append(f"{seconds} Sekunde")
                else:
                    parts.append(f"{seconds} Sekunden")
            if len(parts) > 1:
                last_part = parts.pop()
                time_str = ", ".join(parts) + " und " + last_part
            else:
                time_str = parts[0] if parts else ""
            await ctx.reply(f'/me {os.getenv('Bot_Admin')} hat {streamer_twitch_id[0].display_name} schon: {time_str} restreamt.')

bot = Bot()
bot.loop.run_until_complete(bot.__ainit__())
bot.loop.run_until_complete(bot.create_database_tables())
async def schedule_daily_reset():
    while True:
        now = datetime.now(berlin_zone)
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_until_midnight = (midnight - now).total_seconds()
        print(seconds_until_midnight)
        await asyncio.sleep(seconds_until_midnight)
        bot.live_channels_today.clear()

bot.loop.create_task(schedule_daily_reset())
bot.run()