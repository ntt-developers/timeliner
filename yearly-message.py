import os
import psycopg2
import requests
import json
import time

def select_yearly_count():
    dsn = os.environ["PSQL_DSN"]
    sql = "select channel_id,count(pl.channel_id) from postlog pl where pl.post_at < '2024-01-01 00:00:00' and pl.post_at >= '2023-01-01 00:00:00' and not exists(select * from exclusion_list el where el.channel_id = pl.channel_id) group by pl.channel_id order by count(pl.channel_id) desc"
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()
    return results

def select_user_count(channel_id):
    dsn = os.environ["PSQL_DSN"]
    sql = "select pl.post_user_id, count(pl.post_user_id) from postlog pl where pl.channel_id = %s and pl.post_at < '2024-01-01 00:00:00' and pl.post_at >= '2023-01-01 00:00:00' group by pl.post_user_id order by count(pl.post_user_id) desc"
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql,(channel_id,))
            results = cur.fetchall()
    return results

def slack_post_message_main(message):
    url = "https://slack.com/api/chat.postMessage"
    token = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["POST_CHANNEL_ID"]
    
    payload = {"token":token,
            "channel":channel,
            "text":message
    }

    ret = requests.post(url,data=payload)
    ret_json = ret.json()
    return ret_json.get("message").get("ts")

def slack_post_message_thread(message,ts):
    url = "https://slack.com/api/chat.postMessage"
    token = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["POST_CHANNEL_ID"]

    payload = {"token":token,
            "channel":channel,
            "text":message,
            "thread_ts":ts
    }
    requests.post(url,data=payload)

def slack_get_user_info(user_id):
    url = "https://slack.com/api/users.profile.get"
    token = os.environ["SLACK_BOT_TOKEN"]

    head_token = "Bearer " + token
    headers = {"Authorization":head_token}

    payload = {
            "user":user_id
    }
    ret = requests.get(url,params=payload,headers=headers)
    return ret.json()

# --- Main ---

data = select_yearly_count()

fir_message = " :bamboo: あけましておめでとうございます :sunrise_over_mountains: \n 2023年、一年分のチャンネル別投稿数ランキングをお知らせします \n 長いのでスレッド形式で投稿します\n ※ Timelinerが参加しているチャンネルのみ集計されています \n 2024年もntt-developers slackをよろしくお願いします :bow: \n"

ts = slack_post_message_main(fir_message)

max_count = len(data)-1

for i in range(max_count):
    message = ""
    time.sleep(2)
    message += str(i+1)
    message += "位 "
    message += " <#"
    message += data[i][0]
    message += "> ("
    message += str(data[i][1])
    message += " posts)\n\n"

    user_data = select_user_count(data[i][0]) 

    message += "＜チャンネル内投稿者ランキング＞\n"
    max_count_j = min(5,len(user_data))
    for j in range(max_count_j):
        message += str(j+1)
        message += "："
        time.sleep(0.5)
        user_profile = slack_get_user_info(user_data[j][0]).get("profile")
        if user_profile is None:
            user_name = "[deactivated user]"
        else:
            user_name = user_profile.get("display_name")
        message += user_name
        message += " ("
        message += str(user_data[j][1])
        message += " posts)\n"
    message += "\n"
    
    slack_post_message_thread(message,ts)
time.sleep(0.5)
last_message = " このランキング投稿は今年初の試みなので、コメントあればお願いします :bow: \n ※ 全チャンネルのtop5出すのは長いかなと思いつつ、全部見たいかなと思って全部投稿してみました \n ※ 紅白見ながらやっつけでコード書いたのでバグったら:gomensoumen: \n "

slack_post_message_thread(last_message,ts)
