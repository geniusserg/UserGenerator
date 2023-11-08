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


def get_items_of_page(group_id, count=10):
    params = {
        'owner_id': '-' + group_id,
        'count': count,
        'access_token': access_token,
        'v': '5.131',  # Версия VK API
    }
    response = requests.get(
        'https://api.vk.com/method/wall.get',
        params=params)
    data = response.json()
    like_data = {}
    users_set = set()
    user_info = []
    if 'response' in data:
        posts = data['response']['items']
        print(posts[0]["id"])
        for post in tqdm(posts):
            user_likes = likes_request(group_id, post["id"])
            like_data[str(post["id"])] = user_likes
            users_set.update(user_likes)
            time.sleep(0.4)
        for user in tqdm(list(users_set)):
            user_info.append(user_request(user))
            time.sleep(0.4)
    else:
        print("Произошла ошибка")
    return like_data, list(users_set), user_info

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
def grabb_users_and_likes(group_id="94"):
    iteraction_info = get_items_of_page(group_id=group_id, count=100)
    result_recsys = {"group": group_id, "users_likes": iteraction_info[0]}
    result_tables = {"group": group_id, "user_info": iteraction_info[2]}
    with open("data/user_likes.json", "w") as f:
        json.dump(result_recsys, f)
    with open("data/user_info.json", "w") as f:
        json.dump(result_tables, f)
    return True

def preprocess_tabular_data():
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
