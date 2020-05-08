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

logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

data_dir = os.environ.get('DATA_PATH', '.')

period = 15
coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'EOSUSDT', 'ADAUSDT']

# {'channel_name': 'TradingRoom_VIP channel', 'channel_id': '-1001407228571', 'lang': 'ru'},
# {'channel_name': 'VIP Signal P&C', 'channel_id': '-1001412423063', 'lang': 'eng'},
# {'channel_name': 'Crypto Libertex', 'channel_id': '@libertex_crypto', 'lang': 'eng'},
d = [{'channel_name': 'Криптоисследование 2.0', 'channel_id': '-1001482165395', 'lang': 'ru'},
     {'channel_name': 'Investigación criptográfica 2.0', 'channel_id': '-1001237960088', 'lang': 'es'},
     {'channel_name': 'Stormgain', 'channel_id': '-1001442509377', 'lang': 'eng'},
     {'channel_name': 'Stormgain', 'channel_id': '-1001208185244', 'lang': 'ru'},
     {'channel_name': 'Stormgain', 'channel_id': '-1001108189618', 'lang': 'es'},
     {'channel_name': 'Stormgain', 'channel_id': '-1001414652913', 'lang': 'tr'}]

icid_link = lambda coin, lang: 'https://app.stormgain.com/deeplink.html?mobile=instrument/instruments/{0}&desktop=%23modal_newInvest_{0}&icid=academy_sgcrypto_{1}_telegram'.format(coin, lang)

open_emoji = emojize(":rotating_light:", use_aliases=True)
close_emoji = emojize(":bell:", use_aliases=True)


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
            last_trade = self.__db.fetch_last_trade(coin)
            last_trade_direction = last_trade.get('direction')
            last_trade_price = last_trade.get('price')

            # Get data and calculate strategy
            df_data = get_all_binance(coin, '{}m'.format(period), start_date=get_historical_start_date(5))
            df = calc_strategy(df_data)
            row = df.iloc[-1]

            # Change
            pct_chg = (row['close'] - last_trade_price) * 100 / last_trade_price if last_trade_direction == 'Long' else \
                (last_trade_price - row['close']) * 100 / last_trade_price if last_trade_direction == 'Short' else 0
            pct_chg = round(pct_chg, 2)


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
                    msg_en = '{} *{}* #{} at {}\nThis position is only 3% of our capital.\n' \
                              'Please, control your risk!'.format(open_emoji, row['signal'], coin, row['close'])
                    msg_ru = '{} *{}* #{} по {}\nВ эту позицию мы вложили только 3% нашего капитала.\n' \
                             'Пожалуйста, контролируйте свой риск!'.format(
                        open_emoji, 'Купить' if row['signal'] == 'Long' else 'Продать', coin, row['close'])
                    msg_es = '{} *{}* #{} por {}\nHemos invertido solo 3%-5% de nuestro capital en esta posición.\n' \
                             '¡Por favor controle su riesgo!'.format(
                        open_emoji, 'Comprar' if row['signal'] == 'Long' else 'Vender', coin, row['close'])
                    msg_en_stormgain = '{} *{}* #{} at {}\nStop loss: {}\nThis position is only 3% of our capital.\n[Please, press the link to open terminal]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'eng'))
                    msg_ru_stormgain = '{} *{}* #{} по {}\nСтоп лосс: {}\nВ эту позицию мы вложили только 3% нашего капитала.\n[Перейти в терминал Stormgain]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'ru'))
                    msg_es_stormgain = '{} *{}* #{} at {}\nStop loss: {}\nHemos invertido solo 3% de nuestro capital en esta posición.\n[Ir a la terminal Stormgain]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'es'))
                    msg_tr_stormgain = '{} *{}* #{} at {}\nStop loss: {}\nBu pozisyon sermayemizin sadece% 3ü\n[Terminali açmak için lütfen bağlantıya basın]({})'.format(
                        open_emoji, row['signal'], coin, row['close'], stop_loss_price, icid_link(coin, 'tr'))
                    # Send messages to channels
                    for dic in d:
                        if dic['lang'] == 'ru':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru)
                        elif dic['lang'] == 'eng':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_en)
                        elif dic['lang'] == 'es':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_es_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_es)
                        elif dic['lang'] == 'tr':
                            send_post_to_telegram('Message', dic['channel_id'], msg_tr_stormgain)

                        send_post_to_telegram('Photo', dic['channel_id'],
                                              visualize_candlestick(df=df, symbol=coin, period=period,
                                                                    time=df.index[-1],
                                                                    trades=self.__db.fetch_last_trades(coin)))
                        logger.info('Message posted in {}'.format(dic['channel_name']))
                # Close signal
                else:
                    logger.info('Close signal in {}'.format(coin))
                    # Messages
                    msg_en = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!'.format(close_emoji, coin, row['close'], pct_chg)
                    msg_ru = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!'.format(close_emoji, coin, row['close'], pct_chg)
                    msg_es = '{} Posición cerrada en #{} por {}\nPorcentaje de cambio desde el punto de entrada: {}%\n¡Pasemos al próximo buen comercio!'.format(close_emoji, coin, row['close'], pct_chg)
                    msg_en_stormgain = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'eng'))
                    msg_ru_stormgain = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!\n[Перейти в терминал Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'ru'))
                    msg_es_stormgain = '{} Posición cerrada en #{} por {}\nPorcentaje de cambio desde el punto de entrada: {}%\n¡Pasemos al próximo buen comercio!\n[Ir a la terminal Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'es'))
                    msg_tr_stormgain = "{} {}'de #{}'i kapatın\nGiriş fiyatından yüzde değişim: %{}\nBir sonraki İyi ticarete geçelim!\n[Terminali açmak için lütfen bağlantıya basın]({})".format(
                        close_emoji, row['close'], coin, pct_chg, icid_link(coin, 'tr'))
                    # Send messages to channels
                    for dic in d:
                        if dic['lang'] == 'ru':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru)
                        elif dic['lang'] == 'eng':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_en)
                        elif dic['lang'] == 'es':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_es_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_es)
                        elif dic['lang'] == 'tr':
                            send_post_to_telegram('Message', dic['channel_id'], msg_tr_stormgain)

                        send_post_to_telegram('Photo', dic['channel_id'],
                                              visualize_candlestick(df=df, symbol=coin, period=period,
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
                    msg_en = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!'.format(
                        close_emoji, coin, row['close'], pct_chg)
                    msg_ru = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!'.format(
                        close_emoji, coin, row['close'], pct_chg)
                    msg_es = '{} Posición cerrada en #{} por {}\nPorcentaje de cambio desde el punto de entrada: {}%\n¡Pasemos al próximo buen comercio!'.format(
                        close_emoji, coin, row['close'], pct_chg)
                    msg_en_stormgain = '{} Cover #{} at {}\nPercent change from entry price is: {}%\nLets move on to next Good trade!\n[Please, press the link to open terminal]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'eng'))
                    msg_ru_stormgain = '{} Закрыть #{} по {}\nПроцент изменения от точки входа: {}%\nПереходим к следующему хорошему трейду!\n[Перейти в терминал Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'ru'))
                    msg_es_stormgain = '{} Posición cerrada en #{} por {}\nPorcentaje de cambio desde el punto de entrada: {}%\n¡Pasemos al próximo buen comercio!\n[Ir a la terminal Stormgain]({})'.format(
                        close_emoji, coin, row['close'], pct_chg, icid_link(coin, 'es'))
                    msg_tr_stormgain = "{} {}'de #{}'i kapatın\nGiriş fiyatından yüzde değişim: %{}\nBir sonraki İyi ticarete geçelim!\n[Terminali açmak için lütfen bağlantıya basın]({})".format(
                        close_emoji, row['close'], coin, pct_chg, icid_link(coin, 'tr'))
                    # Send messages to channels
                    for dic in d:
                        if dic['lang'] == 'ru':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_ru)
                        elif dic['lang'] == 'eng':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_en_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_en)
                        elif dic['lang'] == 'es':
                            if dic['channel_name'] == 'Stormgain':
                                send_post_to_telegram('Message', dic['channel_id'], msg_es_stormgain)
                            else:
                                send_post_to_telegram('Message', dic['channel_id'], msg_es)
                        elif dic['lang'] == 'tr':
                            send_post_to_telegram('Message', dic['channel_id'], msg_tr_stormgain)
                        send_post_to_telegram('Photo', dic['channel_id'],
                                              visualize_candlestick(df=df, symbol=coin, period=period,
                                                                    time=df.index[-1],
                                                                    trades=self.__db.fetch_last_trades(coin)))
                        logger.info('Message posted in {}'.format(dic['channel_name']))

                continue


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
