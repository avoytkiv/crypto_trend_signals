import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd

from get_binance import get_all_binance
from tools import send_post_to_telegram, visualize_candlestick, get_historical_start_date
from trend import calc_strategy

logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

period = 15
coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'EOSUSDT', 'ADAUSDT', 'LTCBTC', 'EOSETH', 'ETHBTC', 'XMRBTC']

d = [{'channel_name': 'TradingRoom_VIP channel', 'channel_id': '-1001407228571', 'lang': 'ru'},
     {'channel_name': 'VIP Signal P&C', 'channel_id': '-1001412423063', 'lang': 'eng'},
     {'channel_name': 'Crypto Libertex', 'channel_id': '@libertex_crypto', 'lang': 'eng'},
     {'channel_name': 'Криптоисследование 2.0', 'channel_id': '-1001482165395', 'lang': 'ru'}]


path = './bugs/'


def compare_dfs(df1_filename, df2_filename):
    df1 = pd.read_csv(path+df1_filename)
    ls_ix = df1.columns.get_loc('support_04')
    df1 = df1.iloc[:, 1:ls_ix]
    df1 = df1.fillna(0)
    df1 = df1.applymap(lambda x: 1 if x == True else x)
    df1 = df1.applymap(lambda x: 0 if x == False else x)
    df2 = pd.read_csv(path+df2_filename)
    df2 = df2.iloc[:, 1:ls_ix]
    df2 = df2.fillna(0)
    df2 = df2.applymap(lambda x: 1 if x == True else x)
    df2 = df2.applymap(lambda x: 0 if x == False else x)
    df2_copy = df2[:-1]
    df3 = df1.subtract(df2_copy)
    df3.loc["Total"] = df3.sum()
    return df1, df2, df2_copy, df3

# compare_dfs('ETHUSDT-2019-08-23 19:04:12.884645.csv', 'ETHUSDT-2019-08-23 19:05:10.929608.csv')

def save_dfs(coin='ETHUSDT'):
    df_data = get_all_binance(coin, '{}m'.format(period), start_date=get_historical_start_date(1))
    df = calc_strategy(df_data)
    filename = '{}-{}.csv'.format(coin, datetime.now())
    df.to_csv(path+filename)


default_trades = defaultdict(list)
def get_last_orders():
    for coin in coins:
        df_data = get_all_binance(coin, '{}m'.format(period), get_historical_start_date(10))
        df = calc_strategy(df_data)
        # Filter signals
        df_signals = df[(df['signal_order'] == 'Long') |
                        (df['signal_order'] == 'Short') |
                        (df['signal_order'] == 'Close')]
        if df_signals.empty:
            logger.info('No signals {}'.format(coin))
            continue
        row = df_signals.iloc[-1]

        default_trades[coin].append({'timestamp': row['timestamp'],
                                     'price': row['close'],
                                     'direction': row['signal_order']})
        logger.info('Appended {} to dict'.format(coin))
    return default_trades


def main():
    logging.info('Load history trades')
    with open('history-{}min.json'.format(period), 'r') as outfile:
        data = json.load(outfile)
    logger.info('Retrieve prices')
    for coin in coins:
        trades = sorted(data[coin], key=lambda i: i['timestamp'])
        last_trade = trades[-1]
        last_trade_direction = last_trade['direction']

        # Get data and calculate strategy
        df_data = get_all_binance(coin, '{}m'.format(period), start_date=get_historical_start_date(5))
        df = calc_strategy(df_data)
        row = df.iloc[-1]

        # current_position
        if (row['signal'] == 'Long' and last_trade_direction != 'Long') \
                or (row['signal'] == 'Short' and last_trade_direction != 'Short')\
                or (row['signal'] == 'Close' and row['prob_ema'] < 50 and last_trade_direction == 'Long') \
                or (row['signal'] == 'Close' and row['prob_ema'] > 50 and last_trade_direction == 'Short'):
            logger.info('{} signal in {}'.format(row['signal'], coin))
            # Append new signal
            data[coin].append({'timestamp': row['timestamp'],
                               'price': row['close'],
                               'direction': row['signal']})
            # Update json file
            with open('history-{}min.json'.format(period), 'w') as outfile:
                json.dump(data, outfile, indent=4)
            logger.info('Json was updated')
            ####### Create and send messages to channels #######
            # Long/Short signal
            if row['signal'] != 'Close':
                logger.info('{} signal in {}'.format(row['signal'], coin))
                # Messages
                msg_eng = '{} {} at {}\nThis position is only fraction of our capital.\n' \
                          'Please, control your risk!'.format(row['signal'], coin, row['close'])
                msg_ru = '{} {} по {}\nВ эту позицию мы вложили только небольшую часть нашего капитала.\n' \
                         'Пожалуйста, контролируйте свой риск!'.format(
                    'Купить' if row['signal'] == 'Long' else 'Продать', coin, row['close'])
                # Send messages to channels
                for dic in d:
                    if dic['lang'] == 'ru':
                        send_post_to_telegram('Message', dic['channel_id'], msg_ru)
                    else:
                        send_post_to_telegram('Message', dic['channel_id'], msg_eng)
                    send_post_to_telegram('Photo', dic['channel_id'],
                                          visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
                    logger.info('Message posted in {}'.format(dic['channel_name']))
            # Close signal
            else:
                logger.info('Close signal in {}'.format(coin))
                # Messages
                msg_eng = 'Cover {} at {}\nLets move on to next Good trade!'.format(coin, row['close'])
                msg_ru = 'Закрыть {} по {}\nПереходим к следующему хорошему трейду!'.format(coin, row['close'])
                # Send messages to channels
                for dic in d:
                    if dic['lang'] == 'ru':
                        send_post_to_telegram('Message', dic['channel_id'], msg_ru)
                    else:
                        send_post_to_telegram('Message', dic['channel_id'], msg_eng)
                    send_post_to_telegram('Photo', dic['channel_id'],
                                          visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
                    logger.info('Message posted in {}'.format(dic['channel_name']))
        else:
            logger.info('Last trade for {} is {}. Current signal: {}'.format(coin, last_trade_direction, row['signal']))
            continue


if __name__ == '__main__':
    logger.info('data dir = {}'.format(data_dir))

    while True:
        interval = 15 * 60 * 1000
        run_delay = 3000
        now = int(time.time() * 1000)
        next_run = now - now % interval + interval + run_delay
        sleep_time = (next_run - now) * 0.001
        logger.info('Waiting...Job will start in {0:.3f} seconds'.format(sleep_time))
        time.sleep(sleep_time)

        # do something
        logger.info('Job started')
        main()
        logger.info('Job finished')
