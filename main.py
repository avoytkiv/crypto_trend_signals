import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd
import sqlite3

from get_binance import get_all_binance
from tools import send_post_to_telegram, visualize_candlestick, get_historical_start_date
from trend import calc_strategy

logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

data_dir = os.environ.get('DATA_PATH', '.')

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


class Database:
    def __init__(self, datadir: str):
        self.__datadir = datadir
        self.__connection = None

    def __enter__(self):
        dbpath = os.path.join(self.__datadir, 'app.db')
        perform_import = not os.path.isfile(dbpath)
        self.__connection = sqlite3.connect(dbpath)
        self.__connection.__enter__()
        self._init_tables()
        if perform_import:
            self._import_trades()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__connection.__exit__(exc_type, exc_val, exc_tb)

    def _init_tables(self):
        self.__connection.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            symbol TEXT,
            ts INTEGER,
            price REAL,
            direction TEXT
        );
        ''')
        self.__connection.execute('''
        CREATE INDEX IF NOT EXISTS trades_symbol_ts ON trades(symbol, ts);
        ''')

    def _import_trades(self):
        history_json = 'history-{}min.json'.format(period)
        if not os.path.isfile(history_json):
            return

        with open(history_json, 'r') as infile:
            data = json.load(infile)
            c = self.__connection.cursor()
            for symbol in data:
                trades = map(lambda x: (symbol, int(x['timestamp'] * 1000), x['price'], x['direction']), data[symbol])
                c.executemany('INSERT INTO trades (symbol, ts, price, direction) VALUES (?, ?, ?, ?)', trades)

            self.__connection.commit()
            c.close()

    def insert_trade(self, trade):
        c = self.__connection.cursor()
        c.execute('INSERT INTO trades (symbol, ts, price, direction) VALUES (?, ?, ?, ?)',
                  (trade['symbol'], int(trade['timestamp'] * 1000), trade['price'], trade['direction']))
        self.__connection.commit()
        c.close()

    def fetch_last_trade(self, symbol):
        c = self.__connection.cursor()
        c.execute('SELECT * FROM trades WHERE symbol = ? ORDER BY ts DESC LIMIT 1;', [symbol])
        row = c.fetchone()
        c.close()

        return {
            'symbol': row[0],
            'timestamp': row[1] / 1000,
            'price': row[2],
            'direction': row[3]
        } if row is not None else None

    def fetch_last_trades(self, symbol, limit: int=50):
        c = self.__connection.cursor()
        c.execute('SELECT * FROM (SELECT * FROM trades WHERE symbol = ? ORDER BY ts DESC LIMIT ?) ORDER BY ts ASC;', [symbol, limit])
        rows = c.fetchall()
        c.close()

        return list(map(lambda row: {'symbol': row[0], 'timestamp': row[1] / 1000, 'price': row[2], 'direction': row[3]}, rows))


class Strategy:
    def __init__(self, datadir: str):
        self.__db = Database(datadir)

    def __enter__(self):
        self.__db.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__db.__exit__(exc_type, exc_val, exc_tb)

    def runone(self):
        logging.info('Load history trades')
        logger.info('Retrieve prices')

        for coin in coins:
            last_trade = self.__db.fetch_last_trade(coin)
            last_trade_direction = last_trade.get('direction')

            # Get data and calculate strategy
            df_data = get_all_binance(coin, '{}m'.format(period), start_date=get_historical_start_date(5))
            df = calc_strategy(df_data)
            row = df.iloc[-1]

            # current_position
            if (row['signal'] == 'Long' and last_trade_direction != 'Long') \
                    or (row['signal'] == 'Short' and last_trade_direction != 'Short') \
                    or (row['signal'] == 'Close' and row['prob_ema'] < 50 and last_trade_direction == 'Long') \
                    or (row['signal'] == 'Close' and row['prob_ema'] > 50 and last_trade_direction == 'Short'):
                logger.info('{} signal in {}'.format(row['signal'], coin))
                # Append new signal
                self.__db.insert_trade({
                    'symbol': coin,
                    'ts': row['timestamp'],
                    'price': row['close'],
                    'direction': row['signal']
                })

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
                                              visualize_candlestick(df=df, symbol=coin, period=period,
                                                                    time=df.index[-1],
                                                                    trades=self.__db.fetch_last_trades(coin)))
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
                                              visualize_candlestick(df=df, symbol=coin, period=period,
                                                                    time=df.index[-1],
                                                                    trades=self.__db.fetch_last_trades(coin)))
                        logger.info('Message posted in {}'.format(dic['channel_name']))
            else:
                logger.info(
                    'Last trade for {} is {}. Current signal: {}'.format(coin, last_trade_direction, row['signal']))
                continue


if __name__ == '__main__':
    logger.info('data dir = {}'.format(data_dir))

    with Strategy(data_dir) as strategy:
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
            strategy.runone()
            logger.info('Job finished')
