import os
import psycopg2
import requests
import json

def select_monthly_count():
    dsn = os.environ["PSQL_DSN"]
    table_name = os.environ["PSQL_TABLE"]
    sql = "select pl.channel_id, count(pl.channel_id) from %(psql_table)s pl where pl.post_at > now() +  '-1 month' and not exists(select * from exclusion_list el where el.channel_id = pl.channel_id) group by pl.channel_id order by count(pl.channel_id) desc limit 20"
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql % {'psql_table':table_name})
            results = cur.fetchall()
    return results

def slack_post_message(message):
    url = "https://slack.com/api/chat.postMessage"
    token = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["SLACK_GENERAL_CHANNEL_ID"]
    
    payload = {"token":token,
            "channel":channel,
            "text":message
    }

    requests.post(url,data=payload)

# --- Main ---

data = select_monthly_count()
timeline_channel = os.environ["TIMELINE_CHANNEL_ID"]

message = "1ヶ月の投稿数Top20 (in <#"
message += timeline_channel
message += "> )\n"

for row in data:
    message += " <#"
    message += row[0]
    message += "> ("
    message += str(row[1])
    message += " posts)\n"

slack_post_message(message)
