import json
import time
import requests
from tqdm import tqdm
import pandas as pd
from methods.pandas_process import posts_transfrom_json_to_pandas
from methods.pandas_process import likes_to_recsys_matrix
from methods.pandas_process import users_transfrom_json_to_pandas

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
    if ('response' in data and isinstance(data['response'], list) and len(data['response'])>0):
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
    return post


def get_items_of_page(group_id, count=10):
    offset = 0
    like_data = []
    post_info = []
    while count > 0:
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
        if 'response' in data:
            posts = data['response']['items']
            for post in tqdm(posts):
                try:
                    post_processed = post_process(post=post)
                except:
                    post_processed = {}
                    pass
                orig_post_id = post["id"]
                post_processed["id"] = str(group_id)+"_"+str(post_processed["id"])
                post_processed["post_id"] = str(post_processed["id"])
                post_processed["group_id"] = str(group_id)
                like_data.append({"post_id": post_processed["id"], "likes": likes_request(group_id, orig_post_id)})
                post_info.append(post_processed)
                time.sleep(0.4)
        else:
            print(f"{count}, {offset} iteration: Произошла ошибка")
        count -= 100; offset += 100
        time.sleep(10)
    result = {"likes":like_data, "posts": post_info}
    return result

## currently is not used
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

# select active users. columns=users, index=posts
def select_active_users(recsys_matrix, treshold=3):
    return recsys_matrix.loc[:, (~recsys_matrix.isna()).sum()>treshold].columns

# threshold for likes count -> 
def get_active_user_info(recsys_matrix, threshold = 3):
    active_users = recsys_matrix.loc[:, (~recsys_matrix.isna()).sum() >= threshold].columns
    users_info = []
    for j in tqdm(active_users):
        users_info.append(grabber.user_request(j))
        time.sleep(0.4)
    df = users_transfrom_json_to_pandas(users_info)
    return df

####
# Function which collect posts from group, 
# users who liked that posts and information about that users.
# Input arguments: group_id, count of posts, 
# likes caount (function will collect users who liked more than likes'-treshold times)
# Output: 3 csvs in data folder - posts.csv, like_matrix.csv, users.csv
####

def grabb_group_info(group_id, posts_count = 100):
    iteraction_info = get_items_of_page(group_id=group_id, count=posts_count)
    result_recsys = {"group": group_id, "likes": iteraction_info["likes"], "posts": iteraction_info["posts"]}
    with open("data/user_likes.json", "w") as f:
        json.dump(result_recsys, f) # save unprocessed result to json
    # post info to csv
    posts_info = posts_transfrom_json_to_pandas(f["posts"])
    posts_info.to_csv("data/posts.csv")
    #likes matrix
    recsys_matrix = likes_to_recsys_matrix(f["likes"])
    recsys_matrix.to_csv("data/like_matrix.csv")
    return result_recsys
