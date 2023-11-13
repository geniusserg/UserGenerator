import json
import time
import requests
from tqdm import tqdm
import pandas as pd

try:
    auth = json.load(open("access_token.json", "r"))
except BaseException:
    raise Exception("Load valid access_token.json file and start again")
access_token = auth["token"]

## check VK API #


def check_vk_api():
    params = {
        'user_ids': "1",
        'fields': "sex",
        'access_token': access_token,
        'v': '5.131'
    }
    try:
        response = requests.get(
            'https://api.vk.com/method/users.get',
            params=params, timeout=50)
    except BaseException as e:
        raise Exception(
            "Can not create VK API request. Check token and settings")
    data = response.json()
    if 'response' in data:
        print("VK API is available")
        return True
    else:
        raise Exception("Not valid response from VK API")


check_vk_api()

## Methods of grabbing info ##

def user_process(df):
    return df

def user_request(user_id):
    params = {
        'user_ids': user_id,
        'fields': '''sex,
                    bdate,
                    city,
                    home_town,
                    country,
                    activities,
                    books,
                    career,
                    connections,
                    counters,
                    education,
                    military,
                    followers_count,
                    friend_status,
                    schools,
                    verified,
                    games,
                    interests,
                    movies,
                    music,
                    occupation,
                    personal,
                    relation,
                    universities''',
        'access_token': access_token,
        'v': '5.131',
    }
    response = requests.get(
        'https://api.vk.com/method/users.get',
        params=params, timeout=50)
    data = response.json()
    if ('response' in data):
        data = data['response'][0]
    else:
        print("WARNING", data)
        data = {}
    return data


def post_process(post):
    if("owner_id" in post):
        if(post["owner_id"]!=post["from_id"]):
            post["owner"] = 0
        else:
            post["owner"] = 1
    
    # attachments
    if("attachments" in post):
        for attachment in post["attachments"]:
            if("type" in attachment):
                if (attachment["type"] == "video"):
                    video = attachment["video"]
                    if ("comments" in video):
                        post["video_comments"] = video["comments"]
                    if ("date" in video):
                        post["video_date"] = video["date"]
                    if ("duration" in video):
                        post["video_duration"] = video["duration"]
                    if ("description" in video):
                        post["video_description"] = video["duration"]
            if ("photo" in attachment):
                if("type" in attachment):
                    if (attachment["type"] == "photo"):
                        photo = attachment["photo"]
                        if ("text" in photo):
                            post["photo_text"] = photo["text"]
                        if ("date" in photo):
                            post["photo_date"] = photo["date"]
        del post["attachments"]
    del post["hash"]
    del post["from_id"]
    del post["owner_id"]
    del post["type"]
    del post["inner_type"]
    del post["short_text_rate"]
    del post["likes"]["can_like"]
    del post["likes"]["user_likes"]
    # del post["id"]

    return post


def get_items_of_page(group_id, count=10, offset=0):
    params = {
        'owner_id': '-' + group_id,
        'count': count,
        'offset': offset,
        'access_token': access_token,
        'v': '5.131',  # Версия VK API
    }
    response = requests.get(
        'https://api.vk.com/method/wall.get',
        params=params)
    data = response.json()
    like_data = []
    post_info = []
    print("Start parsing")
    if 'response' in data:
        posts = data['response']['items']
        for post in tqdm(posts):
            # try:
            post_processed = post_process(post=post)
            # except:
            #     print(f"Warning! cannot process item with id {str(post['id'])}")
            post_info.append({"post_id": "-"+str(group_id)+"_"+str(post["id"]), "post_info": post_processed})
            like_data.append({"post_id": str(post["id"]), "likes": likes_request(group_id, post["id"])})
            time.sleep(0.4)
    else:
        print("Произошла ошибка")

    result = {"likes":like_data, "posts": post_info}
    return result

def get_groups_user_follows(user_id):
    params = {
    'user_ids': user_id,
    
    'access_token': access_token,
    'v': '5.131',
    }
    response = requests.get(
        'https://api.vk.com/method/users.get',
        params=params, timeout=50)
    data = response.json()
    if ('response' in data):
        data = data['response'][0]
    else:
        print("WARNING", data)
        data = {}
    return data



def likes_request(group_id, post_id):
    params = {
        'type': 'post',
        'owner_id': '-' + group_id,
        'item_id': post_id,
        'filter': 'likes',
        'access_token': access_token,
        'v': '5.131',
    }
    response = requests.get(
        'https://api.vk.com/method/likes.getList',
        params=params)
    list_of_likes = []
    data = response.json()
    if 'response' in data:
        liked_users = data['response']['items']
        for user_id in liked_users:
            list_of_likes.append(user_id)
    else:
        raise Exception(data)
    return list_of_likes


# grab user profiles and their likes in current gr
def grabb_group_info(group_id="31480508"):
    iteraction_info = get_items_of_page(group_id=group_id, count=100)
    result_recsys = {"group": group_id, "users_likes": iteraction_info[0], "user_reposts":  iteraction_info[1]}
    result_tables = {"group": group_id, "user_info": iteraction_info[2]}
    with open("data/user_likes.json", "w") as f:
        json.dump(result_recsys, f)
    with open("data/user_info.json", "w") as f:
        json.dump(result_tables, f)
    return True

def preprocess_user():
    df = json.load(open("data/user_info.json","r",encoding='utf-8'))["user_info"]
    df = pd.json_normalize(df)
    df = df.drop(columns=["can_access_closed", 
                      "personal", 
                      "relation_partner.id", 
                      'relation_partner.first_name', 
                      'relation_partner.last_name'])
    df.loc[~pd.isna(df["relation"]) & (df["relation"]==0), "relation"] = None
    def bdate_year_parser(bday):
        if pd.isna(bday):
            return None
        bday = bday.split(".")
        if len(bday) == 3:
            return pd.to_datetime(".".join(bday), format="%d.%m.%Y")
        else:
            return None
    df["bdate"].apply(bdate_year_parser)
    now = pd.to_datetime('now')
    df['age'] = (now.year - df['bdate'].dt.year) - ((now.month - df['bdate'].dt.month) < 0)
    user_info_df = df
    data = json.load(open("user_info.json", "r"))["users_likes"]
    data_for_df = []
    for i in data:
        data_for_df.extend(list(zip([i]*len(data[i]), map(str, data[i]))))
    post_likes_df = pd.DataFrame(data_for_df, columns = ["post_id", "user_id"])
    return user_info_df, post_likes_df
