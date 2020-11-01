#!/usr/bin/env python3
import requests
import json
import pandas as pd
import time
import random
import os

def do_request(url):
    HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
               'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
    }
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = json.loads(response.text)
#    news_data = data['data']
#    next_id = data['meta']['page']['minmaxPublishOn']['min']
    return data

def request_data(symbol, next_id=None):
    INITIAL_URL = 'https://seekingalpha.com/api/v3/symbols/{2}/news?cachebuster={0}&id={2}&include=author,primaryTickers,secondaryTickers,sentiments&isMounting=true&page[size]={1}'
    NEXT_URL = 'https://seekingalpha.com/api/v3/symbols/{2}/news?cachebuster={0}&filter[until]={3}&id={2}&include=author,primaryTickers,secondaryTickers,sentiments&isMounting=false&page[size]={1}'
    LAST_DATE = '2020-10-31'
    PAGE_SIZE = 20
    if next_id == None:
        url = INITIAL_URL.format(LAST_DATE, PAGE_SIZE, symbol)
    else:
        url = NEXT_URL.format(LAST_DATE, PAGE_SIZE, symbol, next_id)
    done = False
    while not done:
        try:
            time.sleep(random.uniform(1, 4))
            response = do_request(url)
            done = True
        except requests.HTTPError as err:
            print(f'HTTP error for symbol {symbol} and next_id {next_id}: {err}')
            print(url)
            print('Retrying')

    return response

def get_df_from_request(data):
    column_names = ['id', 'date', 'title', 'comments']
    parsed_data = {'id': [], 'date': [], 'title': [], 'comments':[]}
    news_data = data['data']
    for piece_of_news in news_data:
        parsed_data['id'].append(piece_of_news['id'])
        parsed_data['date'].append(piece_of_news['attributes']['publishOn'])
        parsed_data['title'].append(piece_of_news['attributes']['title'])
        parsed_data['comments'].append(piece_of_news['attributes']['commentCount'])
    news_df = pd.DataFrame(parsed_data, columns = column_names)
    news_df.set_index('id', inplace=True)
    return news_df

def next_id_from_request(data):
    return data['meta']['page']['minmaxPublishOn']['min']

def store_news(symbol, dir_name):
    response = request_data(symbol)
    news_df = get_df_from_request(response)

    next_id = next_id_from_request(response)
    response = request_data(symbol, next_id)
    news_df = news_df.append(get_df_from_request(response))
    next_id = next_id_from_request(response)
    news_df.to_csv(f'{dir_name}/{symbol}.csv', index=True)
    return news_df

symbol = 'aapl'
dir_name = time.strftime("%Y_%m_%d_%H_%M_%S")
os.mkdir(dir_name)
store_news(symbol, dir_name)

