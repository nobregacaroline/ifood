from pyspark.sql import SparkSession
from datetime import datetime
import requests

# Inicializando sessão Spark
spark = SparkSession.builder.appName("Case - Ifood").getOrCreate()

GITHUB_TOKEN = 'token-teste'
HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}'
}


def get_response(url):
    return requests.get(url, headers=HEADERS).json()


def clean_data(user_data):
    user_data['company'] = user_data['company'].replace('@', '') if user_data['company'] else None
    user_data['created_at'] = datetime.strptime(user_data['created_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%d/%m/%Y')
    return user_data


def get_followers(username):
    followers = []
    page = 1
    while True:
        url = f'https://api.github.com/users/{username}/followers?page={page}'
        response = get_response(url)
        if not response:
            break
        followers.extend(response)
        page += 1
    return followers


def process(username):
    followers = get_followers(username)
    user_data_list = []

    for follower in followers:
        url = f'https://api.github.com/users/{follower['login']}'
        user_data = get_response(url)
        cleaned_data = clean_data(user_data)
        user_data_list.append((
            cleaned_data.get('name'),
            cleaned_data.get('company'),
            cleaned_data.get('blog'),
            cleaned_data.get('email'),
            cleaned_data.get('bio'),
            cleaned_data.get('public_repos'),
            cleaned_data.get('followers'),
            cleaned_data.get('following'),
            cleaned_data.get('created_at')
        ))

    return user_data_list


if __name__ == '__main__':
    # Processando dados da API Github
    user_data_list = process('elonmuskceo')

    df = spark.createDataFrame(user_data_list,
                               ["name", "company", "blog", "email", "bio", "public_repos", "followers", "following",
                                "created_at"])

    # Escrevendo dados em arquivo CSV
    df.repartition(1).write.csv('github_out.csv', mode='overwrite', header=True)
