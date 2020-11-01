#!/usr/bin/env python3
import requests
import json
import pandas as pd
import time
import random
import os
import argparse

def do_request(url):
    HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
               'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'en-GB,en-US;q=0.9,en:q=0.8',
               'sec-fetch-mode': 'navigate',
               'Cookie': 'machine_cookie=1703391517042; aasd=3%7C1604183496961; __aaxsc=2; session_id=489c1503-0c34-4902-8c63-a65d5c45956a; _gcl_au=1.1.978364666.1604183498; _pxvid=d786f211-1bc8-11eb-901b-0242ac12000d; _pxde=03bb33b4118990d5339992ead6add2c952ba2966dc02a4c8cd927d1f04ffb164:eyJ0aW1lc3RhbXAiOjE2MDQyMjc0NDUzMTcsImZfa2IiOjB9; prism_25946650=3b46e6a7-5f5d-4bc8-9d41-25a114f023c5; _px2=eyJ1IjoiMjdmNGIwMDAtMWMyZi0xMWViLWE2YjAtNmRlMTNlYzk0NzA2IiwidiI6ImQ3ODZmMjExLTFiYzgtMTFlYi05MDFiLTAyNDJhYzEyMDAwZCIsInQiOjE2MDQyMjc5NDUzMTcsImgiOiJkODg2MmU2NmE2OTU5YWIwNjgxNzNiMzYzNjgxNjliZmI3NDEwNTI4MDNhODg2OGZkYmE1Y2IyYmEyZjVlZTJkIn0=; _px=w/qCqBNBxhBTeU+zfO591WFEsJqutcw6U7n9errsI+xEy2fLyPeEn35P78ra8kDm1P9kmiwSKy24a9vmKw7p1Q==:1000:Eig29eQx/FpjWUNvTBi5GTllsDMMmirQe78EMhOg5eBSqjyHdGq3WqEEhaYVoDre9WRiHsfAo4QSgmoT76KGT6nEbCmDf+cGPO31GyBLgez+O1ffoNqa9uZfmrF1l+4vCuoYoIwaNWw60bwwT4EqJTGSGQcJjGZ4SezAnom8M0eBbFC9k2qBD9UDQSUG+3paHpP0wpTJilqzcU1q4LGFxkGPJ6ZivSels5f9GAivYJem1Hx/+Rv28C91lpmn9H5hDjQlPiyhBYnkm4WD3GySrA==; h_px=1; __tbc=%7Bjzx%7DlCp-P5kTOqotpgFeypItnNSHepGuZ_OFjfcftbGhC6zQMv81qzaEgXRCTpnUukcbRCpXM-K3ofkGOtxDuI6SbAqxG0ziqR_pVmDY2t6BhNouYYqJhh4oz30Pb5UXfTdCUVgFAp6l8Mpn6PJS7yCpyA; __pat=-18000000; __pvi=%7B%22id%22%3A%22v-2020-11-01-10-44-04-675-R39eysV0WcNy53nn-a927d5bd3b5fe4201a870024ddbb9469%22%2C%22domain%22%3A%22.seekingalpha.com%22%2C%22time%22%3A1604227444675%7D; xbc=%7Bjzx%7D-7mUXQL3YWTSiybPi3xKp28qynX7swzefW-sFc_l2GXOt5xRFFTMP-V0xs2P98cc4J4mA3Rz-tGS-OMtqlqp9egQDvH-KxQob903LcyJ3ifRhhR0SjoiuBPsbJI1Ka0iYjQwb3wXBWHbzQwA5jZFS98BHXs2m1oEYYCErqzGXtzeYIdWZJGrs085PN5vqUf7CgO9RB1Vy_CDYfDEwy4TmUvgM-x799yf4IGPmr8C0d36rDW1kWOLSNHKDhwe3aWidAjJYphXYRJeoTCD_JWONkYuSUjggWm5akx37iXLxb4H1kz6mRf15rtzvUcRj97Ww1OaQLWsb4PfnRvV1maIcoy7A6nbE4toPgBvoVkRFd7LArmrw0Lbj4pjMhV0W4wd2gRjBdnEfadi1NC_6izwKxD9mPIAEB4hmwb9D2pRX8gLSpmN8hMJgUwY9krKJPuGeBDVV5DwfBx-uAwBPI2mw0umMn97LOEB34yB_2QSeDZ6RyoPFpWSHHnSdaxnTFrKd0xIsC9aRnTjH_C_uecydLZcYICIzmGcSDNds5W9lcdV_1peF25vMhz7MgGxx7TCuiudktdozyRH9-OPiEymdmWpAv8vENWRxlShK1QY8UhGESOIGkbugD1VzkPNWgCOFRaYHFNcmzD5R1a4QvIp8J12LTw0tv4as_sINQ2kEOI; __pnahc=0; g_state={"i_p":1604191545884,"i_l":1}'
    }
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = json.loads(response.text)
    return data

def request_data(symbol, next_id=None):
    INITIAL_URL = 'https://seekingalpha.com/api/v3/symbols/{2}/news?cachebuster={0}&id={2}&include=author,primaryTickers,secondaryTickers,sentiments&isMounting=true&page[size]={1}'
    NEXT_URL = 'https://seekingalpha.com/api/v3/symbols/{2}/news?cachebuster={0}&filter[until]={3}&id={2}&include=author,primaryTickers,secondaryTickers,sentiments&isMounting=false&page[size]={1}'
    LAST_DATE = '2020-10-31'
    PAGE_SIZE = 25
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
            response = do_request(url)
            done = True
        except requests.HTTPError as err:
            if err.response.status_code != 403:
                raise
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
        parsed_data['date'].append(pd.to_datetime(piece_of_news['attributes']['publishOn'], utc=True))
        parsed_data['title'].append(piece_of_news['attributes']['title'])
        parsed_data['comments'].append(piece_of_news['attributes']['commentCount'])
    news_df = pd.DataFrame(parsed_data, columns = column_names)
    news_df.set_index('id', inplace=True)
    return news_df

def next_id_from_request(data):
    return data['meta']['page']['minmaxPublishOn']['min']

def store_news(symbol, oldes_news):
    response = request_data(symbol)
    news_df = get_df_from_request(response)

    next_id = next_id_from_request(response)
    while (news_df.iloc[-1]['date'] > oldes_news):
        response = request_data(symbol, next_id)
        news_df = news_df.append(get_df_from_request(response))
        next_id = next_id_from_request(response)
        print(f'{symbol}: Last date scraped: {news_df.iloc[-1]["date"]}')
    return news_df

def store_symbols_news(symbols, oldest_news_date, dir_name):
    for symbol in args.symbols:
        symbol = symbol.lower()
        print(f'Starting with {symbol}')
        file_name = f'{dir_name}/{symbol}.csv'
        news_df = store_news(symbol.lower(), args.date)
        news_df.to_csv(file_name, index=True)
        print(f'{symbol}: Data stored in {file_name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape seeking alpha news.')
    parser.add_argument(
        '-d', '--date',
        type=lambda s: pd.to_datetime(s, utc=True), required=True,
        help='Search data from now until this date. Format: YYYY-MM-DD')
    parser.add_argument(
        '-s','--symbols',
        nargs='+', required=True,
        help='List of symbols to scrap')
    args = parser.parse_args()

    dir_name = time.strftime("%Y_%m_%d_%H_%M_%S")
    os.mkdir(dir_name)

    store_symbols_news(args.symbols, args.date, dir_name)

    print(f'Data of {args.symbols} have been saved in {dir_name}')
