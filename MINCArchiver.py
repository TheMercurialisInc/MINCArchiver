import os
import json
import logging
import discord
from discord.ext import commands
import pandas as pd
import datetime
import asyncio

logging.basicConfig(filename='bot.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

TOKEN = 'DISCORD TOKEN HERE'
OUTPUT_DIR = r'FILE DESTINATION/LOCATION'

intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.guild_messages = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Load export state from file if it exists
export_state_file = 'export_state.json'
if os.path.exists(export_state_file):
    with open(export_state_file, 'r') as f:
        export_state = json.load(f)
else:
    export_state = {}

async def export_channel(ctx, channel_id: int):
    global export_state

    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            await ctx.send("Channel not found")
            logging.error("Channel not found")
            return

        last_message_id, last_attachment_id = export_state.get(channel_id, (None, None))

        await ctx.send(f"MINC Archiver received command to export {channel.name}")
        await ctx.send("Export has begun.")
        logging.info("Export has begun.")

        total_size = 0
        num_messages = 0
        num_attachments = 0
        async for message in channel.history(limit=None, after=last_message_id):
            num_messages += 1
            for attachment in message.attachments:
                num_attachments += 1
                total_size += attachment.size
            
            export_state[channel_id] = (message.id, last_attachment_id)
            logging.info(f"Exported message: {message.id}")

            if datetime.datetime.now().minute % 5 == 0:
                progress_percentage = (num_messages / channel.history(limit=None).flatten()) * 100
                await ctx.send(f"Progress: {progress_percentage:.2f}%")

            # Save export state periodically
            if num_messages % 100 == 0:
                await save_export_state()

        await save_export_state()

        total_size_mb = total_size / (1024 * 1024)
        confirmation_message = (
            f"Total size of attachments to be downloaded: {total_size_mb:.2f} MB\n"
            f"Number of messages: {num_messages}\n"
            f"Number of attachments: {num_attachments}\n"
            f"Do you want to continue? (yes/no)"
        )
        await ctx.send(confirmation_message)

        def check(msg):
            return msg.author == ctx.author and msg.channel == ctx.channel and msg.content.lower() in ['yes', 'no']

        try:
            response = await bot.wait_for('message', check=check, timeout=60)
            if response.content.lower() == 'yes':
                await ctx.send("Downloading attachments. This may take some time...")
            else:
                await ctx.send("Operation cancelled.")
                return
        except asyncio.TimeoutError:
            await ctx.send("Response timed out. Operation cancelled.")
            return

        channel_dir = os.path.join(OUTPUT_DIR, f'{channel.name}_{channel.id}')
        os.makedirs(channel_dir, exist_ok=True)

        message_data = []
        count = 0
        async for message in channel.history(limit=None):
            count += 1
            message_info = {
                'date/time': str(message.created_at),
                'name': str(message.author),
                'content': message.content,
                'attachment_urls': [attachment.url for attachment in message.attachments],
                'attachment_paths': []
            }
            
            for attachment in message.attachments:
                try:
                    year = str(message.created_at.year)
                    attachments_dir = os.path.join(channel_dir, "Attachments", year)
                    os.makedirs(attachments_dir, exist_ok=True)
                    attachment_filename = f"{datetime.datetime.fromtimestamp(message.created_at.timestamp(), datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}_{message.author.name}_{attachment.filename}"
                    attachment_path = os.path.join(attachments_dir, attachment_filename)
                    await attachment.save(attachment_path)
                    message_info['attachment_paths'].append(os.path.relpath(attachment_path, start=channel_dir))
                except Exception as e:
                    logging.error(f"Error saving attachment: {e}")

            message_data.append(message_info)
            progress_percentage = (count / num_messages) * 100
            await ctx.send(f"Progress: {progress_percentage:.2f}%")

        json_filename = os.path.join(channel_dir, 'messages.json')
        try:
            with open(json_filename, 'w') as f:
                json.dump(message_data, f, indent=4)
        except Exception as e:
            logging.error(f"Error exporting to JSON: {e}")

        excel_filename = os.path.join(channel_dir, 'messages.xlsx')
        try:
            df = pd.DataFrame(message_data)
            df.to_excel(excel_filename, index=False, columns=['date/time', 'name', 'content', 'attachment_urls', 'attachment_paths'])
        except Exception as e:
            logging.error(f"Error exporting to Excel: {e}")

        years = set(message_info['date/time'][:4] for message_info in message_data)
        for year in years:
            year_filename = os.path.join(channel_dir, f'messages_{year}.txt')
            try:
                with open(year_filename, 'w', encoding='utf-8') as f:
                    for message_info in message_data:
                        if message_info['date/time'].startswith(year):
                            f.write(f"Date/Time: {message_info['date/time']}\n")
                            f.write(f"Name: {message_info['name']}\n")
                            f.write(f"Content: {message_info['content']}\n")
                            f.write("Attachments:\n")
                            for attachment_url, attachment_path in zip(message_info['attachment_urls'], message_info['attachment_paths']):
                                f.write(f"    URL: {attachment_url}\n")
                                f.write(f"    Local Path: {attachment_path}\n")
                            f.write("\n\n")
                print(f"Messages for year {year} exported to text file: {year_filename}")
            except Exception as e:
                logging.error(f"Error exporting to text file: {e}")

        await ctx.send(f"Messages exported to JSON: {json_filename}, Excel: {excel_filename}, and text files for each year")

    except Exception as e:
        logging.error(f"Error during export: {str(e)}")
        await ctx.send("An error occurred during export. Please check the logs for more information.")

async def save_export_state():
    with open('export_state.json', 'w') as file:
        json.dump(export_state, file)

@bot.command()
async def export(ctx, channel_id: int):
    print("Export command invoked")
    logging.info("Export command invoked")
    await export_channel(ctx, channel_id)

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    logging.info(f'Logged in as {bot.user}')

bot.run(TOKEN)
