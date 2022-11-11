import os
import logging
import asyncpg
import asyncio
import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# ボットトークンとソケットモードハンドラーを使ってアプリを初期化します
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

logging.basicConfig(level=logging.INFO)

async def db_insert(channel_id,post_at_time,permalink,post_user_id):
    dsn = os.environ["PSQL_DSN"]
    conn = await asyncpg.connect(dsn)
    
    table = os.environ["PSQL_TABLE"]
    sql = "INSERT INTO :table (channel_id,post_at,permalink,post_user_id) VALUES($1,$2,$3,$4)"
    sql = sql.replace(":table",table)

    async with conn.transaction():
        await conn.execute(sql, channel_id,post_at_time,permalink,post_user_id)

    await conn.close()

@app.event({
    "type": "message",
    "subtype": (None,"thread_broadcast")
})
def handle_message_events(say, logger, context, message):
    #logger.debug(message)

    channel_id = message["channel"]
    post_at_ts = message["ts"]
    post_user_id = message["user"]

    res = app.client.chat_getPermalink(
            channel=channel_id,
            message_ts=post_at_ts
    )
    permalink = res["permalink"]
    post_at_unix = int(post_at_ts.split('.')[0])
    post_at_time = datetime.datetime.fromtimestamp(post_at_unix)
   
    app.client.chat_postMessage(
            channel=os.environ["TIMELINE_CHANNEL_ID"],
            text=permalink
    )

    asyncio.run(db_insert(channel_id,post_at_time,permalink,post_user_id))

@app.event("message")
def handle_message_events(body, logger):
        logger.debug(body)

# アプリを起動します
if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
