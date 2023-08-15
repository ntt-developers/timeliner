import os
import requests
import json
import psycopg2
import time

def select_uniq_cid():
    dsn = os.environ.get("PSQL_DSN")
    sql = "select distinct channel_id from postlog"

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

    array = []
    for res in results:
        array.append(res[0])

    return array

def slack_channel_join(channel):
    url = "https://slack.com/api/conversations.join"
    token = os.environ["SLACK_BOT_TOKEN_TL"]
    payload = {"token":token,
            "channel":channel
    }
    response = requests.post(url, data=payload)
    
    if response.status_code != 200:
        print("cid: " + channel)
        print(response.content)
        print()
    #print(response.status_code)
    #print(response.content)
# --- Main ---

cid_list = select_uniq_cid()
for cid in cid_list:
    slack_channel_join(cid)
    time.sleep(2)
