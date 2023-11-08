import json
import time
import requests
from tqdm import tqdm

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
                    country,
                    activities,
                    books,
                    career,
                    connections,
                    counters,
                    education,
                    exports,
                    followers_count,
                    friend_status,
                    has_photo,
                    has_mobile,
                    home_town,
                    schools,
                    verified,
                    games,
                    interests,
                    military,
                    movies,
                    music,
                    occupation,
                    personal,
                    relation,
                    relatives,
                    timezone,
                    tv,
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


def get_items_of_page(group_id, count=5):
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
        for post in tqdm(posts, leave=True):
            user_likes = likes_request(group_id, post["id"])
            like_data[str(post["id"])] = user_likes
            users_set.update(user_likes)
            time.sleep(0.1)
        for user in tqdm(list(users_set), leave=True):
            user_info.append(user_request(user))
            time.sleep(0.1)
    else:
        print("Произошла ошибка")
    return like_data, list(users_set), user_info


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


def grabb_users_and_likes(group_id="94"):
    iteraction_info = get_items_of_page(group_id=group_id)
    result_recsys = {"group": group_id, "users_likes": iteraction_info[0]}
    result_tables = {"group": group_id, "user_info": iteraction_info[2]}
    with open("data/user_likes.json", "w") as f:
        json.dump(result_recsys, f)
    with open("data/user_info.json", "w") as f:
        json.dump(result_tables, f)
    return True

