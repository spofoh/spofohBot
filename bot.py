import logging
import os
import json
import re
import requests
from dotenv import load_dotenv
import asyncpg
from twitchio.ext import commands, eventsub
import twitchio
import asyncio
from typing import Optional
import aiohttp
from zoneinfo import ZoneInfo
import time
from datetime import datetime, timedelta, timezone
import calendar
from collections import defaultdict


load_dotenv()

logging.getLogger('twitchio').setLevel(logging.CRITICAL)
logging.basicConfig(filename='bot.log', level=logging.INFO, format='%(message)s', encoding='utf-8')

berlin_zone = ZoneInfo("Europe/Berlin")

log_sites = [
  'https://logs.ivr.fi',
  'https://logs.2807.eu',
  'https://logs.susgee.dev',
  'https://logs.spanix.team',
  'https://logs.nadeko.net',
  'https://log.spofoh.de'
]

esbot = commands.Bot.from_client_credentials(client_id=os.getenv('Twitch_App_ID'),
                                         client_secret=os.getenv('Twitch_App_Token'))


esclient = eventsub.EventSubClient(esbot,
                                   webhook_secret=os.getenv('webhook_secret_pw'),
                                   callback_route='https://eventsub.spofoh.de/callback')

class Bot(commands.Bot):

    def __init__(self):
        if not os.path.exists('channels.json'):
            with open('channels.json', 'w') as f:
                json.dump([os.getenv('Not_leaveable')], f)
        with open('channels.json', 'r') as f:
            channels = json.load(f)
        super().__init__(token=os.getenv('Twitch_Generator_Token'), client_id=os.getenv('Twitch_Generator_ID'), prefix='+',
                         initial_channels=channels)
        
    async def __ainit__(self) -> None:
        await esclient.delete_all_active_subscriptions()
        with open('channels.json', 'r') as f:
            channels = json.load(f)
        self.loop.create_task(esclient.listen(port=4000))

        broadcaster_id = await self.fetch_users(names=channels,  token = os.getenv('Twitch_Generator_Token'))
        for broad_id in broadcaster_id:
            try:
                await esclient.subscribe_channel_stream_start(broadcaster=broad_id.id)
            except twitchio.HTTPException:
                pass

    async def search_logs(self, channel_name, username=None):
        available_logs = []

        async with aiohttp.ClientSession() as session:
            tasks = []
            for site in log_sites:
                tasks.append(self.fetch_logs(session, site, channel_name, username))

            results = await asyncio.gather(*tasks)
            for result in results:
                if result:
                    available_logs.append(result)

        return available_logs
    
    async def fetch_logs(self, session, site, channel_name, username):
        try:
            async with session.get(f'{site}/channels') as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        channels = [channel['name'] for channel in data['channels']]

                        if channel_name.lower() in channels:
                            url = f'{site}/?channel={channel_name}'

                            if username:
                                url += f'&username={username}'
                        
                            return url
                    except aiohttp.ContentTypeError:
                        print(f'Warnung: Die Antwort von {site}/channels konnte nicht als JSON interpretiert werden.')
                else:
                    print(f'Warnung: Anfrage an {site}/channels hat den Statuscode {response.status} zurückgegeben.')
        except aiohttp.ClientError as e:
            print(f'Warnung: Anfrage an {site} fehlgeschlagen. Fehlermeldung: {str(e)}')

        return None

    async def get_mods(self, channel):
        url = "https://gql.twitch.tv/gql"
        payload = "[{\"operationName\":\"Mods\",\"variables\":{\"login\":\"" + channel + "\"},\"extensions\":{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"cb912a7e0789e0f8a4c85c25041a08324475831024d03d624172b59498caf085\"}}}]"
        headers = {
            'client-id': 'kimne78kx3ncx6brgo4mv6wki5h1ko',
            'Content-Type': 'text/plain'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        data = json.loads(response.text)
        if data and 'data' in data[0] and 'user' in data[0]['data'] and 'mods' in data[0]['data']['user'] and 'edges' in data[0]['data']['user']['mods']:
            mods = [edge['node']['login'] for edge in data[0]['data']['user']['mods']['edges']]
        else:
            mods = []
        mods.append(channel)
        return mods

    async def create_database_tables(self):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS twitch_channels (
                channel_id INTEGER PRIMARY KEY,
                watch_time INTEGER
            );
        ''')

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_offdays_stats (
                id SERIAL PRIMARY KEY,
                channel_id INT NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                live_days INT DEFAULT 0,
                UNIQUE (channel_id, year, month)
            )
        """)

        await conn.execute('''CREATE TABLE IF NOT EXISTS streaks (
                streamer_id INTEGER PRIMARY KEY,
                current_streak INTEGER,
                highest_streak INTEGER,
                last_live_date TEXT
            )''')
        
        await conn.execute('''CREATE TABLE IF NOT EXISTS live_channels_today (
                streamer_id INTEGER PRIMARY KEY,
                last_live_date TEXT
            )''')

        await conn.close()

    async def update_live_days(self, channel_name):
        today = datetime.now(berlin_zone).date()
        month = today.month
        year = today.year

        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        
        streamer_twitch_id = await self.fetch_users(names=[channel_name])

        result = await conn.fetchrow(
    "SELECT id, live_days FROM channel_offdays_stats WHERE channel_id=$1 AND month=$2 AND year=$3",
    streamer_twitch_id[0].id, month, year
)

        if result:
            new_live_days = result['live_days'] + 1
            await conn.execute(
                "UPDATE channel_offdays_stats SET live_days=$1 WHERE id=$2",
                new_live_days, result['id']
            )
        else:
            await conn.execute(
                "INSERT INTO channel_offdays_stats (channel_id, month, year, live_days) VALUES ($1, $2, $3, 1)",
                streamer_twitch_id[0].id, month, year
            )


        await conn.close()

    async def update_streak(self, streamer_name):
        today = datetime.now(berlin_zone).date()
            
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        
        streamer_twitch_id = await self.fetch_users(names=[streamer_name])
        streamer_id = streamer_twitch_id[0].id
        
        row = await conn.fetchrow("SELECT current_streak, highest_streak, last_live_date FROM streaks WHERE streamer_id = $1", (streamer_id))

        if row:
            current_streak, highest_streak, last_live_date = row
            last_live_date = datetime.strptime(last_live_date, '%Y-%m-%d').date()

            if last_live_date == today:
                return

            if last_live_date == today - timedelta(days=1):
                current_streak += 1
            else:
                current_streak = 1

            if current_streak > highest_streak:
                highest_streak = current_streak

            await conn.execute(f"""
                UPDATE streaks 
                SET current_streak={current_streak}, highest_streak={highest_streak}, last_live_date='{str(today)}' 
                WHERE streamer_id={streamer_id}
            """)
        else:
            await conn.execute(
                '''INSERT INTO streaks (streamer_id, current_streak, highest_streak, last_live_date) 
                   VALUES ($1, $2, $3, $4)''',
                streamer_id, 1, 1, str(today)
            )

        await conn.close()

    async def reset_streaks(self):
        today = datetime.now(berlin_zone).date()
        yesterday = today - timedelta(days=1)

        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                     user=os.getenv('db_user'), password=os.getenv('db_password'),
                                     database=os.getenv('db_database'), loop=asyncio.get_event_loop())

        rows = await conn.fetch("SELECT streamer_id, last_live_date FROM streaks")
        for row in rows:
            streamer_id, last_live_date = row
            last_live_date = datetime.strptime(last_live_date, '%Y-%m-%d').date()

            if last_live_date < yesterday:
                await conn.execute("""
                    UPDATE streaks 
                    SET current_streak=0 
                    WHERE streamer_id=$1
                """, streamer_id)

        await conn.close()

    @esbot.event()
    async def event_eventsub_notification_stream_start(event: eventsub.StreamOnlineData) -> None:
        print(f'Stream gestartet: {event.data.broadcaster.name}')
        channel_name = event.data.broadcaster.name
        await bot.update_streak(channel_name)

        today = datetime.now(berlin_zone).date()
        last_stream_date = await bot.get_last_stream_date(channel_name)
        
        if last_stream_date is None:
            await bot.create_new_streamer_entry(channel_name, today)
        else:
            if str(last_stream_date) == str(today):
                return
            else:
                await bot.update_last_stream_date(channel_name, today)
                await bot.update_live_days(channel_name)

    async def get_last_stream_date(self, channel_name):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                     user=os.getenv('db_user'), password=os.getenv('db_password'),
                                     database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        
        streamer_twitch_id = await self.fetch_users(names=[channel_name])
        streamer_id = streamer_twitch_id[0].id

        last_stream_date = await conn.fetchval(
            "SELECT last_live_date FROM live_channels_today WHERE streamer_id = $1", streamer_id
        )
        return last_stream_date
    
    async def create_new_streamer_entry(self, channel_name, today):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                     user=os.getenv('db_user'), password=os.getenv('db_password'),
                                     database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        
        streamer_twitch_id = await self.fetch_users(names=[channel_name])
        streamer_id = streamer_twitch_id[0].id

        await conn.execute(
            "INSERT INTO live_channels_today (streamer_id, last_live_date) VALUES ($1, $2)",
            streamer_id, str(today)
        )

    async def update_last_stream_date(self, channel_name, today):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                     user=os.getenv('db_user'), password=os.getenv('db_password'),
                                     database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        
        streamer_twitch_id = await self.fetch_users(names=[channel_name])
        streamer_id = streamer_twitch_id[0].id

        await conn.execute(
            "UPDATE live_channels_today SET last_live_date = $1 WHERE streamer_id = $2",
            str(today), streamer_id
        )

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
        if ctx.author.name.lower() == os.getenv('Bot_Admin'):
            with open('channels.json', 'r') as f:
                channels = json.load(f)
            if channel not in channels:
                channels.append(channel.lower())
                with open('channels.json', 'w') as f:
                    json.dump(channels, f)
                await self.join_channels([channel])
                broadcaster_id = await self.fetch_users(names=[channel])
                await esclient.subscribe_channel_stream_start(broadcaster=broadcaster_id[0].id)
                await ctx.reply(f"/me ✅ Beigetreten zum Kanal: {channel}")
            else:
                await ctx.reply(f"/me Ich bin bereits dem Kanal {channel} beigetreten.")
        elif ctx.author.name.lower() not in mods and ctx.author.name.lower() != os.getenv('Bot_Admin'):
            await ctx.reply("/me ⚠️ Nur der Streamer und die Moderatoren können den Bot einem Kanal hinzufügen. ⚠️")
            return
        elif ctx.author.name.lower() in mods:
            with open('channels.json', 'r') as f:
                channels = json.load(f)
            if channel not in channels:
                channels.append(channel.lower())
                with open('channels.json', 'w') as f:
                    json.dump(channels, f)
                await self.join_channels([channel])
                broadcaster_id = await self.fetch_users(names=[channel])
                await esclient.subscribe_channel_stream_start(broadcaster=broadcaster_id[0].id)
                await ctx.reply(f"/me ✅ Beigetreten zum Kanal: {channel}")

    @commands.command(name='leave')
    @commands.cooldown(rate=1, per=5, bucket=commands.Bucket.channel)
    async def leave(self, ctx, channel: str = None):
        if channel is None:
            channel = ctx.author.name.lower()
        if channel.lower() == os.getenv('Not_leaveable'):
            await ctx.reply(f"/me ⚠️ Der Bot kann den Kanal {channel.lower()} nicht verlassen. ⚠️")
            return
        mods = await self.get_mods(channel)
        if ctx.author.name.lower() == os.getenv('Bot_Admin'):
            with open('channels.json', 'r') as f:
                channels = json.load(f)
            if channel in channels:
                channels.remove(channel.lower())
                await ctx.reply(f"/me ❌ Verlassen des Kanals: {channel}")
                with open('channels.json', 'w') as f:
                    json.dump(channels, f)
                await self.part_channels([channel])
                broadcaster_id = await self.fetch_users(names=[channel])
                subscriptions = await esclient.get_subscriptions(user_id=broadcaster_id[0].id)
                for subscription in subscriptions:
                    await esclient.delete_subscription(subscription_id=subscription.id)
            else:
                await ctx.reply(f"/me ❌ Ich bin in dem Channel nicht.")
        elif ctx.author.name.lower() not in mods and ctx.author.name.lower() != os.getenv('Bot_Admin'):
            await ctx.reply("/me ⚠️ Nur der Streamer und die Moderatoren können den Bot entfernen. ⚠️")
            return
        elif ctx.author.name.lower() in mods:
            with open('channels.json', 'r') as f:
                channels = json.load(f)
            if channel in channels:
                channels.remove(channel.lower())
                await ctx.reply(f"/me ❌ Verlassen des Kanals: {channel}")
                with open('channels.json', 'w') as f:
                    json.dump(channels, f)
                await self.part_channels([channel])
                broadcaster_id = await self.fetch_users(names=[channel])
                subscriptions = await esclient.get_subscriptions(user_id=broadcaster_id[0].id)
                for subscription in subscriptions:
                    await esclient.delete_subscription(subscription_id=subscription.id)

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
            await ctx.reply("/me ⚠️ Der gesuchte Streamer wurde nicht gefunden! ⚠️")
            return
        
        streamer_id = data[0]['value']
        safe_streamer_name = data[0]['displaytext']

        url = f"https://sullygnome.com/api/tables/channeltables/games/365/{streamer_id}/%20/1/2/desc/0/100"
        response = requests.get(url)
        data = response.json()
        
        if not data['data']:
            await ctx.reply(f"/me ⚠️ {safe_streamer_name} hat noch kein Spiel gespielt oder wird noch nicht getrackt. ⚠️")
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
            await ctx.reply('/me ✅ ' + message)
            await asyncio.sleep(0.5)

    @commands.command(name='bayrisch')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def bayrisch(self, ctx, *, message=None):
        if message is None:
            await ctx.reply("/me ⚠️ Bitte gib eine Nachricht ein, die übersetzt werden soll. ⚠️")
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
            await ctx.reply('/me ✅ ' + translated_message)

    @commands.command(name='ösi')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def oesi(self, ctx, *, message=None):
        if message is None:
            await ctx.reply("/me ⚠️ Bitte gib eine Nachricht ein, die übersetzt werden soll. ⚠️")
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
            await ctx.reply('/me ✅ ' + translated_message)

    @commands.command(name='freegames')
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def freegames(self, ctx):
        url = "https://store-site-backend-static-ipv4.ak.epicgames.com/freeGamesPromotions?locale=en-US&country=DE&allowCountries=DE"
        headers = {}
        response = requests.request("GET", url, headers=headers)
        data = json.loads(response.text)
        free_games = []

        for element in data['data']['Catalog']['searchStore']['elements']:
            if element['status'] == 'ACTIVE' and element['offerType'] != 'ADD_ON' and any(category['path'] == 'freegames' or category['path'] == 'games' for category in element['categories']):
                promotion = element.get('promotions', None)
                if promotion:
                    for promotional_offer in promotion.get('promotionalOffers', []):
                        for offer in promotional_offer['promotionalOffers']:
                            start_date = datetime.strptime(offer['startDate'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                            end_date = datetime.strptime(offer['endDate'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                            current_time_utc = datetime.now(timezone.utc)
                            if start_date <= current_time_utc <= end_date:
                                free_games.append(element['title'])
                                break

                    for upcoming_offer in promotion.get('upcomingPromotionalOffers', []):
                        for offer in upcoming_offer['promotionalOffers']:
                            start_date = datetime.strptime(offer['startDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
                            end_date = datetime.strptime(offer['endDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
                            if start_date <= datetime.utcnow() <= end_date:
                                free_games.append(element['title'])
                                break

        if free_games:
            await ctx.reply(f"/me ✅ Die momentanen Free Games auf Epic: {', '.join(free_games)}")
        else:
            await ctx.reply("/me ❌ Es gibt momentan keine kostenlosen Spiele auf Epic.")

    @commands.command(name='commands', aliases=['help', 'cmd', 'cmds'])
    @commands.cooldown(rate=1, per=15, bucket=commands.Bucket.channel)
    async def list_commands(self, ctx):
        await ctx.reply(f"/me ✅ Die verfügbaren Befehle findet man hier: https://pastebin.com/raw/PsLL2pJv")

    @commands.command(name='logs', aliases=['log'])
    @commands.cooldown(rate=1, per=10, bucket=commands.Bucket.channel)
    async def searchlogs(self, ctx, channel_name=None, username=None):
        if channel_name is None:
            channel_name = ctx.channel.name

        logs = await self.search_logs(channel_name, username)

        if logs:
            await ctx.reply(f'/me ✅ Die Logs vom Channel: {channel_name} sind auf den folgenden Seiten verfügbar: {" ".join(logs)}')
        else:
            response = f'/me ⚠️ Keine Logs gefunden für den Channel. ⚠️ Benutzte logs instanzen: '
            response += ' | '.join(log_sites[1:]) if len(log_sites) > 1 else ''
            await ctx.reply(response)

    @commands.command(name='offdays', aliases=['offday'])
    @commands.cooldown(rate=1, per=10, bucket=commands.Bucket.channel)
    async def offdays_command(self, ctx, channel_name: Optional[str], month: Optional[str], year: Optional[str]):
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
        streamer_twitch_id = await self.fetch_users(names=[channel_name])
        if not streamer_twitch_id:
            await ctx.reply('/me ⚠️ Kein Kanal gefunden mit diesem Namen. ⚠️')
            return
        result = await conn.fetchrow(
            "SELECT live_days FROM channel_offdays_stats WHERE channel_id=$1 AND month=$2 AND year=$3",
            streamer_twitch_id[0].id, month, year
        )
        await conn.close()

        if result is None:
            await ctx.reply('/me ⚠️ Keine Daten zu diesem Zeitpunkt oder der Streamer wird nicht getracked. ⚠️')
            return
        else:
            live_days = result['live_days']

        offdays = days_in_month - live_days

        if month == datetime.now().month and year == datetime.now().year:
            await ctx.reply(f"/me ✅ Offdays für diesen Monat im Channel {streamer_twitch_id[0].display_name}: {offdays} ({live_days}/{days_in_month})")
        else:
            month_names = ["Januar", "Februar", "März", "April", "Mai", "Juni",
                        "Juli", "August", "September", "Oktober", "November", "Dezember"]
            month_name = month_names[month - 1]
            await ctx.reply(f"/me ✅ Offdays für {month_name} im Channel {streamer_twitch_id[0].display_name}: {offdays} offdays ({live_days}/{days_in_month})")

    @commands.command(name='restreams', aliases=['restream'])
    @commands.cooldown(rate=1, per=5, bucket=commands.Bucket.channel)
    async def restreams(self, ctx, streamer_name: str, *time_parts):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'))
        if time_parts:
            mods = await self.get_mods(os.getenv('Bot_Admin'))
            print(mods)
            if ctx.author.name not in mods:
                await ctx.reply('/me ❌ Nur Moderatoren können die Zeit hinzufügen.')
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
                await ctx.reply('/me ⚠️ Kein Kanal gefunden mit diesem Namen. ⚠️')
                return
            print(streamer_twitch_id[0].id)
            await conn.execute('''
                INSERT INTO twitch_channels(channel_id, watch_time) VALUES($1, $2)
                ON CONFLICT (channel_id) DO UPDATE SET watch_time = twitch_channels.watch_time + $2
            ''', streamer_twitch_id[0].id, time_in_seconds)
            await conn.close()
            await ctx.reply(f'/me ✅ Zeit wurde hinzugefügt.')
        else:
            streamer_twitch_id = await self.fetch_users(names=[streamer_name])
            if not streamer_twitch_id:
                await ctx.reply('/me ⚠️ Kein Kanal gefunden mit diesem Namen. ⚠️')
                return
            seconds = await conn.fetchval('SELECT watch_time FROM twitch_channels WHERE channel_id = $1', streamer_twitch_id[0].id)
            if not seconds:
                await ctx.reply('/me ⚠️ Keine Informationen zu diesem Benutzer. ⚠️')
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
            Bot_Admin = os.getenv('Bot_Admin')
            await ctx.reply(f'/me ✅ {Bot_Admin} hat {streamer_twitch_id[0].display_name} schon: {time_str} restreamt.')

    @commands.command(name='streak')
    @commands.cooldown(rate=1, per=5, bucket=commands.Bucket.channel)
    async def streak(self, ctx, channel_name: Optional[str]):
        conn = await asyncpg.connect(host=os.getenv('db_host_ip'), port=os.getenv('db_port'),
                                    user=os.getenv('db_user'), password=os.getenv('db_password'),
                                    database=os.getenv('db_database'), loop=asyncio.get_event_loop())
        
        if channel_name is None:
            channel_name = ctx.channel.name
        
        streamer_twitch_id = await self.fetch_users(names=[channel_name])

        row = await conn.fetchrow("SELECT current_streak, highest_streak FROM streaks WHERE streamer_id = $1", (streamer_twitch_id[0].id))

        if row:
            current_streak, highest_streak = row
            await ctx.reply(
    f"/me {streamer_twitch_id[0].name}'s aktuelle daily Streak: {current_streak} {'Tag' if current_streak == 1 else 'Tage'}, "
    f"höchste tracked daily Streak: {highest_streak} {'Tag' if highest_streak == 1 else 'Tage'}")
        else:
            await ctx.reply(f"/me ⚠️ Keine Daten für {streamer_twitch_id[0].name} verfügbar. ⚠️")

    @commands.command(name='update')
    @commands.cooldown(rate=1, per=5, bucket=commands.Bucket.channel)
    async def update_offdays(self, ctx, channel_name: str):
        if channel_name is None:
            channel_name = ctx.channel.name.lower()
        mods = await self.get_mods(channel_name)
        if ctx.author.name.lower() in mods or ctx.author.name.lower() == os.getenv('Bot_Admin'):
            url = f"https://sullygnome.com/api/standardsearch/{channel_name}/false/true/false/false"
            response = requests.get(url)
            data = response.json()

            if not data:
                await ctx.reply("/me ⚠️ Der Streamer wird nicht auf sullygnome getracked. ⚠️")
                return

            streamer_id = data[0]['value']

            all_streams = []
            offset = 0

            while True:
                streams_url = f"https://sullygnome.com/api/tables/channeltables/streams/365/{streamer_id}/%20/1/1/desc/{offset}/100"
                response = requests.get(streams_url)
                streams_data = response.json()

                if not streams_data['data']:
                    break

                all_streams.extend(streams_data['data'])

                if len(streams_data['data']) < 100:
                    break

                offset += 100

            live_days_per_month = defaultdict(int)

            for stream in all_streams:
                stream_date = datetime.strptime(stream['startDateTime'], "%Y-%m-%dT%H:%M:%SZ").date()
                year = stream_date.year
                month = stream_date.month
                live_days_per_month[(year, month)] += 1

            print(f"Off-days found: {live_days_per_month} | {channel_name}")

            await self.update_offdays_in_db(channel_name, live_days_per_month)
            await ctx.reply(f'/me ✅ Die Offdays wurden erfolgreich aktualisiert für den Channel: {channel_name}!')
        else:
            await ctx.reply("/me ❌ Nur der Streamer und die Moderatoren können diesen Command ausführen.")

    async def update_offdays_in_db(self, channel_name, live_days_per_month):
        conn = await asyncpg.connect(
            host=os.getenv('db_host_ip'), 
            port=os.getenv('db_port'),
            user=os.getenv('db_user'), 
            password=os.getenv('db_password'),
            database=os.getenv('db_database'), 
            loop=asyncio.get_event_loop()
        )

        streamer_twitch_id = await self.fetch_users(names=[channel_name])

        for (year, month), live_days in live_days_per_month.items():
            result = await conn.fetchrow(
                "SELECT id FROM channel_offdays_stats WHERE channel_id=$1 AND month=$2 AND year=$3",
                streamer_twitch_id[0].id, month, year
            )

            if result:
                await conn.execute(
                    "UPDATE channel_offdays_stats SET live_days=$1 WHERE id=$2",
                    live_days, result['id']
                )
            else:
                await conn.execute(
                    "INSERT INTO channel_offdays_stats (channel_id, month, year, live_days) VALUES ($1, $2, $3, $4)",
                    streamer_twitch_id[0].id, month, year, live_days
                )

        await conn.close()

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
        await bot.reset_streaks()

bot.loop.create_task(schedule_daily_reset())
bot.run()
