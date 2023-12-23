from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from datetime import datetime
import asyncio
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


schema = pa.schema([
    ('time', pa.timestamp('ns')),
    ('source', pa.string()),
    ('text', pa.string()),
    ('has_media', pa.string())
])

accumulation_interval_minutes = 1

#delete the existing session file
session_file = 'my_session.session'
if os.path.exists(session_file):
    os.remove(session_file)


api_id = 'API_id'
api_hash =  'API_hash'
channel_username = "Target"

def get_filename():
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"messages_{current_time}.parquet"

async def save_messages(messages, filename):
    data = {
        'time': [message.date for message in messages],
        'source': [channel_username] * len(messages),
        'text': [message.text for message in messages],
        'has_media': ['Yes' if message.media else 'No' for message in messages]
    }

    df = pd.DataFrame(data)

    # Если файл существует, загрузим его
    if os.path.exists(filename):
        table = pq.read_table(filename)
        # Добавим новые данные к существующим данным
        df = pd.concat([table.to_pandas(), df], ignore_index=True)

    # Запишем DataFrame в файл Parquet
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, filename)
    
async def main():
    client = TelegramClient('my_session', api_id, api_hash)
    await client.connect()

    try:
        await client.start()
    except SessionPasswordNeededError:
        client.run(await client.get_password())

    target_chanel_entity = await client.get_entity(channel_username)
    @client.on(events.NewMessage(chats=target_chanel_entity))
    async def my_handler(event):
        messages = [event.message]
        filename = os.path.join("messages", get_filename())
        await save_messages(messages, filename)

    await client.run_until_disconnected()    


if __name__ == "__main__":

    if not os.path.exists("messages"):
        os.makedirs("messages")   

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())