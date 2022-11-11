import os
import logging
import asyncpg
import asyncio
import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# app init
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# log level
# TODO: setting by config value
logging.basicConfig(level=logging.INFO)

# ---common function---

async def db_insert(channel_id,post_at_time,permalink,post_user_id):
    dsn = os.environ["PSQL_DSN"]
    conn = await asyncpg.connect(dsn)
    
    # Note: table is using by "production" and "develop" in my DB.
    # So this can switch two tables.
    table = os.environ["PSQL_TABLE"]
    sql = "INSERT INTO :table (channel_id,post_at,permalink,post_user_id) VALUES($1,$2,$3,$4)"
    sql = sql.replace(":table",table)

    # using transaction
    async with conn.transaction():
        await conn.execute(sql, channel_id,post_at_time,permalink,post_user_id)

    await conn.close()

# ---event function---

@app.event({
    "type": "message",
    "subtype": (None,"thread_broadcast")
})
def handle_message_events(say, logger, context, message):
    logger.debug(message)

    channel_id = message["channel"]
    post_at_ts = message["ts"]

    # In some cases, 'user' is null
    if 'user' in message["user"]:
        post_user_id = message["user"]
    else:
        post_user_id = ""

    # get permalink api
    res = app.client.chat_getPermalink(
            channel=channel_id,
            message_ts=post_at_ts
    )
    permalink = res["permalink"]

    # "ts" to unixtime. And unixtime to datetime.
    post_at_unix = int(post_at_ts.split('.')[0])
    post_at_time = datetime.datetime.fromtimestamp(post_at_unix)
   
    # post to timeline
    app.client.chat_postMessage(
            channel=os.environ["TIMELINE_CHANNEL_ID"],
            text=permalink
    )

    # insert to DB
    asyncio.run(db_insert(channel_id,post_at_time,permalink,post_user_id))

# other message
@app.event("message")
def handle_message_events(body, logger):
        logger.debug(body)

# app start
if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
