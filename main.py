import json
import logging
import os
import time
import pandas as pd
import math
import traceback
from emoji import emojize

import sqlite3

from get_binance import get_all_binance
from tools import send_post_to_telegram, visualize_candlestick, get_historical_start_date
from trend import calc_strategy
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

data_dir = os.environ.get('DATA_PATH', '.')

period = 15
# telegram
coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'LINKUSDT', 'ADAUSDT', 'TRXUSDT']
all_coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'LINKUSDT', 'ADAUSDT', 'TRXUSDT', 'EOSUSDT']
libertex_coins = {'BTCUSDT': 'Bitcoin',
                  'ETHUSDT': 'ETHUSD',
                  'XRPUSDT': 'XRPUSD',
                  'LINKUSDT': 'LNKUSD',
                  'ADAUSDT': 'ADAUSD',
                  'TRXUSDT': 'TRXUSD'}
# storm
# coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'LTCUSDT', 'BCHUSDT', 'ETHBTC', 'LTCBTC', 'BCHBTC', 'DASHBTC']
# all_coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'LTCUSDT', 'BCHUSDT', 'ETHBTC', 'LTCBTC', 'BCHBTC', 'DASHBTC']

# {'channel_name': 'TradingRoom_VIP channel', 'channel_id': '-1001407228571', 'lang': 'ru'},
# {'channel_name': 'VIP Signal P&C', 'channel_id': '-1001412423063', 'lang': 'eng'},
# {'channel_name': 'Crypto Libertex', 'channel_id': '@libertex_crypto', 'lang': 'eng'},
d = [{'channel_name': 'Stormgain', 'channel_id': '-1001442509377', 'lang': 'eng', 'source': 'storm'},
     {'channel_name': 'Stormgain', 'channel_id': '-1001208185244', 'lang': 'ru', 'source': 'storm'},
     {'channel_name': 'Stormgain', 'channel_id': '-1001108189618', 'lang': 'es', 'source': 'storm'},
     {'channel_name': 'Stormgain', 'channel_id': '-1001414652913', 'lang': 'tr', 'source': 'storm'},
     {'channel_name': 'Libertex CryptoResearch 3.0 Europe', 'channel_id': '-1001423335370', 'lang': 'eng', 'source': 'cysec'},
     {'channel_name': 'CryproResearch 3.0 Signals RU', 'channel_id': '-1001461362070', 'lang': 'ru', 'source': 'bvi'},
     {'channel_name': 'CryptoResearch 3.0 Signals EN', 'channel_id': '-1001186344499', 'lang': 'eng', 'source': 'bvi'},
     {'channel_name': 'Crypto3.0 Storm Signals RU', 'channel_id': '-1001378503756', 'lang': 'ru', 'source': 'storm'},
     {'channel_name': 'Crypto3.0 Storm Signals ES', 'channel_id': '-1001397885499', 'lang': 'es', 'source': 'storm'},
     {'channel_name': 'Crypto3.0 Storm Signals EN', 'channel_id': '-1001357818917', 'lang': 'eng', 'source': 'storm'},
     {'channel_name': 'Crypto3.0 Storm Signals TR', 'channel_id': '-1001256354224', 'lang': 'tr', 'source': 'storm'},
     {'channel_name': 'Crypto3.0 Storm Signals DE', 'channel_id': '-1001131605190', 'lang': 'de', 'source': 'storm'}]

icid_link = lambda coin, lang: 'https://app.stormgain.com/deeplink.html?mobile=instrument/instruments/{0}&desktop=%23modal_newInvest_{0}&icid=academy_sgcrypto_{1}_telegram'.format(coin, lang)
cysec_link = lambda coin: 'https://app.libertex.com/deeplink.html?icid=Research_Crypto3&mobile=new-investment/{0}&desktop=%23modal_newInvest_{0}&accounttype=real'.format(coin)
bvi_link = lambda coin: 'https://libertex.fxclub.org/deeplink.html?icid=Research_Crypto3&mobile=new-investment/{0}&desktop=%23modal_newInvest_{0}&accounttype=real'.format(coin)
bvi_mirror_link = lambda coin: 'https://libertex-fxclub.gofxclub.org/deeplink.html?icid=Research_Crypto3&mobile=new-investment/{0}&desktop=%23modal_newInvest_{0}&accounttype=real'.format(coin)

open_emoji = emojize(":rotating_light:", use_aliases=True)
close_emoji = emojize(":bell:", use_aliases=True)
new_high_emoji = emojize(":tada:", use_aliases=True)

monthly_highs = defaultdict(int)

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

    def fetch_all(self, symbol):
        c = self.__connection.cursor()
        c.execute('SELECT * FROM trades WHERE symbol = ? ORDER BY ts ASC;', [symbol])
        rows = c.fetchall()
        c.close()

        return rows


# with Database(datadir='./data/') as db:
#     all_dfs = pd.DataFrame()
#     for coin in coins:
#         all_trades = db.fetch_all(coin)
#         df = pd.DataFrame(all_trades, columns=['symbol', 'timestamp', 'price', 'direction'])
#
#         if df.empty:
#             logger.info('No signals in {}'.format(coin))
#             continue
#         if df['direction'].iloc[-1] != 'Close':
#             logger.info('Retrieve prices')
#             btc_usdt = get_all_binance(coin, '{}m'.format(period), get_historical_start_date(1))
#             df = df.append(pd.DataFrame([[coin, btc_usdt['timestamp'].iloc[-1] * 1000, btc_usdt['close'].iloc[-1], 'Close']],
#                                         columns=df.columns))
#
#         df['price_shift'] = df['price'].shift(1)
#         df['direction_shift'] = df['direction'].shift(1)
#         # Difference between previous signal price and current signal price
#         df['diff'] = (df['price'] - df['price_shift']) / df['price_shift']
#         # Count short positions to opposite value and if first signal after close then zero pnl
#         df['diff'] = df.apply(lambda row: -1 * row['diff'] if row['direction_shift'] == 'Short' else row['diff'], axis=1)
#         df['diff'] = df.apply(lambda row: 0 if row['direction_shift'] == 'Close' else row['diff'], axis=1)
#
#         df = df.dropna()
#
#         all_dfs = pd.concat([all_dfs, df])
#
#     all_dfs.reset_index()
#     all_dfs['timeindex'] = pd.to_datetime(all_dfs['timestamp'], unit='ms')
#     all_dfs.set_index('timeindex', inplace=True)
#     all_dfs['investment'] = 2000
#     all_dfs['pnl'] = all_dfs['investment'] * all_dfs['diff']
#     all_dfs = all_dfs.sort_values('timestamp')
#     all_dfs.drop('timestamp', axis=1, inplace=True)
#     all_dfs = all_dfs.loc[all_dfs['pnl'] != 0]
#     all_dfs['cum_pnl'] = all_dfs['pnl'].cumsum()
#     all_dfs['total'] = all_dfs['cum_pnl'] + 10000
#     # Rename
#     all_dfs.rename({'direction_shift': 'open_direction', 'price_shift': 'open_price', 'price': 'close_price'}, axis=1, inplace=True)
#     # Save
#     all_dfs.to_csv('./data/'+'stats.csv')


class Strategy:
    def __init__(self, datadir: str):
        self.__db = Database(datadir)

    def __enter__(self):
        self.__db.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__db.__exit__(exc_type, exc_val, exc_tb)

    def runone(self):
        logger.info('Retrieve prices')

        for coin in coins:
            # Get data and calculate strategy
            df_data = get_all_binance(coin, '{}m'.format(period), start_date=get_historical_start_date(5))
            df = calc_strategy(df_data)
            row = df.iloc[-1]

            last_trade = self.__db.fetch_last_trade(coin)
            if last_trade is not None:
                last_trade_direction = last_trade.get('direction')
                last_trade_price = last_trade.get('price')
                # Change
                pct_chg = (row['close'] - last_trade_price) * 100 / last_trade_price if last_trade_direction == 'Long' else \
                    (last_trade_price - row['close']) * 100 / last_trade_price if last_trade_direction == 'Short' else 0
                pct_chg = round(pct_chg, 2)
            else:
                last_trade_direction = 'Close'
                pct_chg = 0


            # current_position
            if (row['signal'] == 'Long' and last_trade_direction != 'Long') \
                    or (row['signal'] == 'Short' and last_trade_direction != 'Short') \
                    or (row['signal'] == 'Close' and row['prob_ema'] < 50 and last_trade_direction == 'Long') \
                    or (row['signal'] == 'Close' and row['prob_ema'] > 50 and last_trade_direction == 'Short'):
                logger.info('{} signal in {}'.format(row['signal'], coin))
                # Append new signal
                self.__db.insert_trade({
                    'symbol': coin,
                    'timestamp': row['timestamp'],
                    'price': row['close'],
                    'direction': row['signal']
                })

                ####### Create and send messages to channels #######
                # Long/Short signal
                if row['signal'] != 'Close':
                    logger.info('{} signal in {}'.format(row['signal'], coin))
                    # Stop loss price for message only
                    digits = str(row['close'])[::-1].find('.')
                    stop_loss_price = round(0.97 * row['close'], digits) if row['signal'] == 'Long' else round(
                        1.03 * row['close'], digits) if row['signal'] == 'Short' else 0
                    # Messages

                    msg_en_stormgain = '{} *{}* #{} at {}\nStop loss: {}\nThis position is only 3% of our capital.\n[Please, press the link to open terminal]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'eng'))
                    msg_ru_stormgain = '{} *{}* #{} по {}\nСтоп лосс: {}\nВ эту позицию мы вложили только 3% нашего капитала.\n[Перейти в терминал Stormgain]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'ru'))
                    msg_es_stormgain = '{} *{}* #{} at {}\nStop loss: {}\nHemos invertido solo 3% de nuestro capital en esta posición.\n[Ir a la terminal Stormgain]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'es'))
                    msg_tr_stormgain = '{} *{}* #{} at {}\nStop loss: {}\nBu pozisyon sermayemizin sadece% 3ü\n[Terminali açmak için lütfen bağlantıya basın]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'tr'))
                    msg_de_stormgain = '{} *{}* #{} at {}\nStop loss: {}\nDiese Position beträgt nur 3% unseres Kapitals.\n[Bitte klicken Sie auf den Link, um das Terminal zu öffnen]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'de'))
                    msg_en_bvi = '{} *{}* #{} at {}\nStop loss: {}\nThis position is only 3% of our capital.\n[Please, press the link to open terminal]({})'.format(
                        open_emoji, row['signal'], libertex_coins[coin], row['close'], stop_loss_price, bvi_link(libertex_coins[coin]))
                    msg_ru_bvi = '{} *{}* #{} по {}\nСтоп лосс: {}\nВ эту позицию мы вложили только 3% нашего капитала.\n[Перейти в терминал на инструмент]({})\nТакже для перехода в терминал можете использовать [запасную ссылку]({})'.format(
                        open_emoji, row['signal'], libertex_coins[coin], row['close'], stop_loss_price, bvi_link(libertex_coins[coin]), bvi_mirror_link(libertex_coins[coin]))
                    msg_en_cysec = '{} *{}* #{} at {}\nStop loss: {}\nThis position is only 3% of our capital.\n[Please, press the link to open terminal]({})'.format(
                        open_emoji, row['signal'], libertex_coins[coin], row['close'], stop_loss_price, cysec_link(libertex_coins[coin]))
                    # Send messages to channels
                    for dic in d:
                        if dic['source'] == 'storm':
                            if dic['lang'] == 'ru':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_stormgain)
                            elif dic['lang'] == 'eng':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_stormgain)
                            elif dic['lang'] == 'es':
                                send_post_to_telegram('Message', dic['channel_id'], msg_es_stormgain)
                            elif dic['lang'] == 'tr':
                                send_post_to_telegram('Message', dic['channel_id'], msg_tr_stormgain)
                            elif dic['lang'] == 'de':
                                send_post_to_telegram('Message', dic['channel_id'], msg_de_stormgain)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=coin, period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))
                        elif dic['source'] == 'bvi':
                            if dic['lang'] == 'ru':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_bvi)
                            elif dic['lang'] == 'eng':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_bvi)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=libertex_coins[coin], period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))
                        elif dic['source'] == 'cysec':
                            send_post_to_telegram('Message', dic['channel_id'], msg_en_cysec)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=libertex_coins[coin],
                                                                        period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))

                        logger.info('Message posted in {}'.format(dic['channel_name']))
                # Close signal
                else:
                    logger.info('Close signal in {}'.format(coin))
                    # Messages
                    msg_en_stormgain = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'eng'))
                    msg_ru_stormgain = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!\n[Перейти в терминал Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'ru'))
                    msg_es_stormgain = '{} Posición cerrada en #{} por {}\nPorcentaje de cambio desde el punto de entrada: {}%\n¡Pasemos al próximo buen comercio!\n[Ir a la terminal Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'es'))
                    msg_tr_stormgain = "{} {}'de #{}'i kapatın\nGiriş fiyatından yüzde değişim: %{}\nBir sonraki İyi ticarete geçelim!\n[Terminali açmak için lütfen bağlantıya basın]({})".format(
                        close_emoji, row['close'], coin, pct_chg, icid_link(coin, 'tr'))
                    msg_de_stormgain = '{} Decken Sie #{} zum Preis von {}\nDie prozentuale Veränderung gegenüber dem Einstiegspreis beträgt: {}%\nFahren wir mit dem nächsten guten Handel fort!\n[Bitte klicken Sie auf den Link, um das Terminal zu öffnen]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'de'))
                    msg_en_bvi = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, libertex_coins[coin], row['close'], pct_chg, bvi_link(libertex_coins[coin]))
                    msg_ru_bvi = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!\n[Перейти в терминал на инструмент]({})\nТакже для перехода в терминал можете использовать [запасную ссылку]({})'.format(
                        close_emoji, libertex_coins[coin], row['close'], pct_chg, bvi_link(libertex_coins[coin]), bvi_mirror_link(libertex_coins[coin]))
                    msg_en_cysec = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, libertex_coins[coin], row['close'], pct_chg, cysec_link(libertex_coins[coin]))

                    # Send messages to channels
                    for dic in d:
                        if dic['source'] == 'storm':
                            if dic['lang'] == 'ru':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_stormgain)
                            elif dic['lang'] == 'eng':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_stormgain)
                            elif dic['lang'] == 'es':
                                send_post_to_telegram('Message', dic['channel_id'], msg_es_stormgain)
                            elif dic['lang'] == 'tr':
                                send_post_to_telegram('Message', dic['channel_id'], msg_tr_stormgain)
                            elif dic['lang'] == 'de':
                                send_post_to_telegram('Message', dic['channel_id'], msg_de_stormgain)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=coin, period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))
                        elif dic['source'] == 'bvi':
                            if dic['lang'] == 'ru':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_bvi)
                            elif dic['lang'] == 'eng':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_bvi)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=libertex_coins[coin],
                                                                        period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))
                        elif dic['source'] == 'cysec':
                            send_post_to_telegram('Message', dic['channel_id'], msg_en_cysec)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=libertex_coins[coin],
                                                                        period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))


                        logger.info('Message posted in {}'.format(dic['channel_name']))
            else:
                logger.info('{}, {}, chg: {}, prob: {}, range: {}'.format(coin,
                                                                          last_trade_direction,
                                                                          round(pct_chg, 2),
                                                                          round(row['prob_ema'], 2),
                                                                          row['ind1']))
                # Stop loss
                if pct_chg < -3:
                    # Append signal
                    self.__db.insert_trade({
                        'symbol': coin,
                        'timestamp': row['timestamp'],
                        'price': row['close'],
                        'direction': 'Close'})

                    logger.info('Stop loss signal in {}'.format(coin))
                    # Messages
                    msg_en_stormgain = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'eng'))
                    msg_ru_stormgain = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!\n[Перейти в терминал Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'ru'))
                    msg_es_stormgain = '{} Posición cerrada en #{} por {}\nPorcentaje de cambio desde el punto de entrada: {}%\n¡Pasemos al próximo buen comercio!\n[Ir a la terminal Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'es'))
                    msg_tr_stormgain = "{} {}'de #{}'i kapatın\nGiriş fiyatından yüzde değişim: %{}\nBir sonraki İyi ticarete geçelim!\n[Terminali açmak için lütfen bağlantıya basın]({})".format(
                        close_emoji, row['close'], coin, pct_chg, icid_link(coin, 'tr'))
                    msg_de_stormgain = '{} Decken Sie #{} zum Preis von {}\nDie prozentuale Veränderung gegenüber dem Einstiegspreis beträgt: {}%\nFahren wir mit dem nächsten guten Handel fort!\n[Bitte klicken Sie auf den Link, um das Terminal zu öffnen]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'de'))
                    msg_en_bvi = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, libertex_coins[coin], row['close'], pct_chg, bvi_link(libertex_coins[coin]))
                    msg_ru_bvi = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!\n[Перейти в терминал на инструмент]({})\nТакже для перехода в терминал можете использовать [запасную ссылку]({})'.format(
                        close_emoji, libertex_coins[coin], row['close'], pct_chg, bvi_link(libertex_coins[coin]), bvi_mirror_link(libertex_coins[coin]))
                    msg_en_cysec = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, libertex_coins[coin], row['close'], pct_chg, cysec_link(libertex_coins[coin]))
                    # Send messages to channels
                    for dic in d:
                        if dic['source'] == 'storm':
                            if dic['lang'] == 'ru':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_stormgain)
                            elif dic['lang'] == 'eng':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_stormgain)
                            elif dic['lang'] == 'es':
                                send_post_to_telegram('Message', dic['channel_id'], msg_es_stormgain)
                            elif dic['lang'] == 'tr':
                                send_post_to_telegram('Message', dic['channel_id'], msg_tr_stormgain)
                            elif dic['lang'] == 'de':
                                send_post_to_telegram('Message', dic['channel_id'], msg_de_stormgain)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=coin, period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))
                        elif dic['source'] == 'bvi':
                            if dic['lang'] == 'ru':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_bvi)
                            elif dic['lang'] == 'eng':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_bvi)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=libertex_coins[coin],
                                                                        period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))
                        elif dic['source'] == 'cysec':
                            send_post_to_telegram('Message', dic['channel_id'], msg_en_cysec)
                            # Chart
                            send_post_to_telegram('Photo', dic['channel_id'],
                                                  visualize_candlestick(df=df, symbol=libertex_coins[coin],
                                                                        period=period,
                                                                        time=df.index[-1],
                                                                        trades=self.__db.fetch_last_trades(coin)))

                        logger.info('Message posted in {}'.format(dic['channel_name']))

                continue

        logger.info('Check database for stats')
        all_dfs = pd.DataFrame()
        for coin in all_coins:
            all_trades = self.__db.fetch_all(coin)
            df = pd.DataFrame(all_trades, columns=['symbol', 'timestamp', 'price', 'direction'])

            if df.empty:
                logger.info('No signals in {}'.format(coin))
                continue

            df['price_shift'] = df['price'].shift(1)
            df['direction_shift'] = df['direction'].shift(1)
            # Difference between previous signal price and current signal price
            df['diff'] = (df['price'] - df['price_shift']) * 100 / df['price_shift']
            # Count short positions to opposite value and if first signal after close then zero pnl
            df['diff'] = df.apply(
                lambda row: -1 * row['diff'] if row['direction_shift'] == 'Short' else row['diff'], axis=1)
            df['diff'] = df.apply(lambda row: 0 if row['direction_shift'] == 'Close' else row['diff'], axis=1)

            df = df.dropna()

            all_dfs = pd.concat([all_dfs, df])

        all_dfs.reset_index()
        all_dfs['timeindex'] = pd.to_datetime(all_dfs['timestamp'], unit='ms')
        all_dfs.set_index('timeindex', inplace=True)

        # Seasonality
        all_dfs = all_dfs[all_dfs.index >= '2019-09-18 19:30:00']
        all_dfs['year'] = all_dfs.index.map(lambda row: row.year)
        all_dfs['month'] = all_dfs.index.map(lambda row: row.month)

        group_df = all_dfs.resample('M').sum()

        current_year= group_df.index[-1].year
        current_month = group_df.index[-1].month
        key = str(current_year) + ',' + str(current_month)
        monthly_return = np.round(group_df['diff'].iloc[-1], 2)

        if monthly_return > monthly_highs[key]:
            group_daily = all_dfs.groupby(['year', 'month'])['diff'].sum()
            id_array = np.arange(0, len(group_daily))
            figure_name = 'new_monthly_high.png'
            ax = group_daily.plot.bar()
            ax.set_ylabel('%')
            ax.hlines(0, id_array[0], id_array[-1], linestyles='dashed', alpha=0.3)
            fig = ax.get_figure()
            plt.tight_layout()
            plt.savefig(figure_name)
            plt.show(block=False)
            plt.close(fig)
            # Messages
            msg_en = '{} New monthly high: {}%'.format(new_high_emoji, monthly_return)
            msg_ru = '{} Новый месячный максимум: {}%'.format(new_high_emoji, monthly_return)
            msg_es = '{} Nuevo máximo mensual: {}%'.format(new_high_emoji, monthly_return)
            msg_tr = '{} Yeni aylık en yüksek: %{}'.format(new_high_emoji, monthly_return)
            msg_de = '{} Neues Monatshoch: {}%'.format(new_high_emoji, monthly_return)

            # Send messages to channels
            for dic in d:
                if dic['lang'] == 'ru':
                    send_post_to_telegram('Message', dic['channel_id'], msg_ru)
                elif dic['lang'] == 'eng':
                    send_post_to_telegram('Message', dic['channel_id'], msg_en)
                elif dic['lang'] == 'es':
                    send_post_to_telegram('Message', dic['channel_id'], msg_es)
                elif dic['lang'] == 'tr':
                    send_post_to_telegram('Message', dic['channel_id'], msg_tr)
                elif dic['lang'] == 'de':
                    send_post_to_telegram('Message', dic['channel_id'], msg_de)

                send_post_to_telegram('Photo', dic['channel_id'], figure_name)
                logger.info('Message posted in {}'.format(dic['channel_name']))

            # assign new value for the key
            monthly_highs[key] = monthly_return

        logger.info('Monthly high: {}'.format(monthly_highs[key]))


if __name__ == '__main__':
    logger.info('data dir = {}'.format(data_dir))

    with Strategy(data_dir) as strategy:
        while True:
            try:
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
            except:
                traceback.print_exc()
                time.sleep(3)
