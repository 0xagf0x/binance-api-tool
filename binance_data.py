# IMPORTS
import asyncio
import pandas as pd
import ccxt.async_support as ccxt  
import math
import os
import os.path
import sys
from binance.client import Client
from csv import writer
from datetime import timedelta, datetime
from dateutil import parser
from pprint import pprint
from tqdm import tqdm_notebook #(Optional, used for progress-bars)


root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

# API
binance_api_key = 'YOUR_BINANCE_API_KEY'
binance_api_secret = 'YOUR_BINANCE_SECRET_KEY'


# CONSTANTS
binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
batch_size = 750
binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)


# HISTORICAL
def minutes_of_new_data(symbol, kline_size, data, source):
    if len(data) > 0:  old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance": old = datetime.strptime('1 Jan 2017', '%d %b %Y')
    if source == "binance": new = pd.to_datetime(binance_client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')
    return old, new

def get_all_binance(symbol, kline_size, save = False):
    filename = '%s-%s-data.csv' % (symbol, kline_size)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "binance")
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])

    if oldest_point == datetime.strptime('1 Jan 2021', '%d %b %Y'): print('Downloading all available %s data for %s. Please wait...' % (kline_size, symbol))
    else: print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))

    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime("%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
    data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df[:len(data_df)-1]
        data_df = data_df.append(temp_df)
    else: data_df = data
    data_df.set_index('timestamp', inplace=True)
    if save: data_df.to_csv(filename)
    print('All caught up..!')
    return data_df

historical_data = get_all_binance("BTCUSDT", "1h", save=True)
# change first agrument for different coin
# "ETHUSDC"

# REALTIME DATA 
async def main(symbol):
    # you can set enableRateLimit = True to enable the built-in rate limiter
    # this way you request rate will never hit the limit of an exchange
    # the library will throttle your requests to avoid that

    exchange = ccxt.binance({
        'enableRateLimit': True,  # this option enables the built-in rate limiter
    })

    header = await exchange.fetch_ticker(symbol)
    del header['symbol']
    del header['timestamp']

    with open('BTCUSDT-1h-data.csv', 'a') as new_csv:
        # Pass this file object to csv.writer() and get a writer object
        writer_object = writer(new_csv)

        # Pass the list as an argument into the writerow()
        writer_object.writerow(header.keys());

    while True:
        # this can be any call instead of fetch_ticker, really
        try:
            ticker = await exchange.fetch_ticker(symbol)
            del ticker['symbol']
            del ticker['timestamp']

            # Open our existing CSV file in append mode + Create a file object for this file
            with open('BTCUSDT-1h-data.csv', 'a') as f_object:
            
                # Pass this file object to csv.writer() and get a writer object
                writer_object = writer(f_object)
            
                # Pass the list as an argument into the writerow()
                writer_object.writerow(ticker.values()) 
                pprint(ticker)
            
                #Close the file object
                f_object.close()

            # print(ticker) 
        except ccxt.RequestTimeout as e:
            print('[' + type(e).__name__ + ']')
            print(str(e)[0:200])
            # will retry
        except ccxt.DDoSProtection as e:
            print('[' + type(e).__name__ + ']')
            print(str(e.args)[0:200])
            # will retry
        except ccxt.ExchangeNotAvailable as e:
            print('[' + type(e).__name__ + ']')
            print(str(e.args)[0:200])
            # will retry
        except ccxt.ExchangeError as e:
            print('[' + type(e).__name__ + ']')
            print(str(e)[0:200])
            break  # won't retry


asyncio.get_event_loop().run_until_complete(main('BTC/USDT'))
