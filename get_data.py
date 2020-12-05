#!/usr/bin/env python3
import requests
import json
import pandas as pd
import time
import random
import os
import argparse
from datetime import date
import json

INITIAL_URL = 'https://seekingalpha.com/api/v3/symbols/{2}/news?cachebuster={0}&id={2}&include=author,primaryTickers,secondaryTickers,sentiments&isMounting=true&page[size]={1}'
NEXT_URL = 'https://seekingalpha.com/api/v3/symbols/{2}/news?cachebuster={0}&filter[until]={3}&id={2}&include=author,primaryTickers,secondaryTickers,sentiments&isMounting=false&page[size]={1}'
LAST_DATE = date.today().strftime('%Y-%m-%d')
PAGE_SIZE = 25

def do_request(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = json.loads(response.text)
    return data

def request_data(symbol, http_headers, next_id=None):
    if next_id == None:
        url = INITIAL_URL.format(LAST_DATE, PAGE_SIZE, symbol)
    else:
        url = NEXT_URL.format(LAST_DATE, PAGE_SIZE, symbol, next_id)
    done = False
    while not done:
        try:
            ## Sleep added to be polite with web requests
            # and randomized to try to avoid web scraping detection
            time.sleep(random.uniform(1, 4))
            response = do_request(url, http_headers)
            done = True
        except requests.HTTPError as err:
            if err.response.status_code != 403:
                raise
            print(f'HTTP error for symbol {symbol} and next_id {next_id}: {err}')
            print(url)
            print('Retrying')

    return response

def get_headers(headers_file):
    headers = {}
    headers_data = json.load(headers_file)
    root_key = next(iter(headers_data))
    for item in headers_data[root_key]['headers']:
        headers[item['name']] = item['value']
    return headers

def get_df_from_request(data):
    column_names = ['id', 'date', 'title', 'comments']
    parsed_data = {'id': [], 'date': [], 'title': [], 'comments':[]}
    news_data = data['data']
    for piece_of_news in news_data:
        parsed_data['id'].append(piece_of_news['id'])
        parsed_data['date'].append(pd.to_datetime(piece_of_news['attributes']['publishOn'], utc=True))
        parsed_data['title'].append(piece_of_news['attributes']['title'])
        parsed_data['comments'].append(piece_of_news['attributes']['commentCount'])
    news_df = pd.DataFrame(parsed_data, columns = column_names)
    news_df.set_index('id', inplace=True)
    return news_df

def next_id_from_request(data):
    return data['meta']['page']['minmaxPublishOn']['min']

def get_news(symbol, oldest_news, http_headers):
    response = request_data(symbol, http_headers)
    news_df = get_df_from_request(response)
    print(f'{symbol}: Last date scraped: {news_df.iloc[-1]["date"]}')

    next_id = next_id_from_request(response)
    while (news_df.iloc[-1]['date'] > oldest_news):
        try:
            response = request_data(symbol, http_headers, next_id)
        except Exception as err:
            print(f'{symbol}: Error doing a request: {err}')
            break
        older_news_df = get_df_from_request(response)
        print(f'{symbol}: Number of new rows: {len(older_news_df.index)} until date scraped: {older_news_df.iloc[-1]["date"]}')
        if len(older_news_df.index) < 25:
            print(f'{symbol}: No more news found about this symbol')
            break
        news_df = news_df.append(older_news_df)
        next_id = next_id_from_request(response)
    return news_df

def store_symbols_news(symbols, oldest_news_date, headers_file, dir_name):
    http_headers = get_headers(headers_file)
    for symbol in args.symbols:
        symbol = symbol.lower()
        print(f'Starting with {symbol}')
        file_name = f'{dir_name}/{symbol}_news.csv'
        news_df = get_news(symbol.lower(), args.date, http_headers)
        print(f'{symbol}: Total size of data: {news_df.shape[0]}')
        news_df.to_csv(file_name, index=True)
        print(f'{symbol}: Data stored in {file_name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape seeking alpha news.')
    parser.add_argument(
        '-s','--symbols',
        nargs='+', required=True,
        help='List of symbols to scrap')
    parser.add_argument(
        '-d', '--date',
        type=lambda s: pd.to_datetime(s, utc=True), required=True,
        help='Search data from now until this date. Format: YYYY-MM-DD')
    parser.add_argument(
        '-a', '--headers',
        type=argparse.FileType('r'), required=True,
        help='File with the containing the HTTP headers to use. This header file content is exported from directly from Firefox.')
    args = parser.parse_args()

    dir_name = time.strftime("%Y_%m_%d_%H_%M_%S")
    os.mkdir(dir_name)

    store_symbols_news(args.symbols, args.date, args.headers, dir_name)

    print(f'Data of {args.symbols} have been saved in {dir_name}')
