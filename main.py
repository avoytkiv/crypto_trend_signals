from trend import calc_strategy
from get_binance import get_all_binance
from tools import send_post_to_telegram, visualize_candlestick, daily_time_intervals, get_historical_start_date
from datetime import datetime
import logging
from collections import defaultdict
import pause
import pandas as pd
import json


logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

period = 15
coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'EOSUSDT', 'ADAUSDT', 'LTCBTC', 'EOSETH', 'ETHBTC', 'XMRBTC']

d = [{'channel_name': 'TradingRoom_VIP channel', 'channel_id': '-1001407228571', 'lang': 'ru'},
     {'channel_name': 'VIP Signal P&C', 'channel_id': '-1001412423063', 'lang': 'eng'},
     {'channel_name': 'Crypto Libertex', 'channel_id': '@libertex_crypto', 'lang': 'eng'},
     {'channel_name': 'Криптоисследование 2.0', 'channel_id': '-1001482165395', 'lang': 'ru'}]

now = datetime.now()
t = datetime.strptime('{}-{}-{} 00:00:00'.format(now.year, now.month, now.day, now.hour),
                      '%Y-%m-%d %H:%M:%S')


seq = daily_time_intervals(t, period)
sent_messages = defaultdict(list)

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

# last_orders = get_last_orders()
# with open('history-{}min.json'.format(period), 'w') as outfile:
#     json.dump(last_orders, outfile, indent=4)
# with open('history-15min.json', 'r') as outfile:
#     data = json.load(outfile)
# for coin in coins:
#     trades = sorted(data[coin], key=lambda i: i['timestamp'])
#     df_trades = pd.DataFrame(trades)
#     last_trade = trades[-1]
#     last_trade_direction = last_trade['direction']
#
#     # Get data and calculate strategy
#     df_data = get_all_binance(coin, '{}m'.format(period), start_date=get_historical_start_date(5))
#     df = calc_strategy(df_data)
#     send_post_to_telegram('Photo', '@libertex_crypto',visualize_candlestick(df=df, df_trades=df_trades, symbol=coin, period=period,time=df.index[-1]))


def main2():
    logging.info('Load history trades')
    with open('history-{}min.json'.format(period), 'r') as outfile:
        data = json.load(outfile)
    logger.info('Retrieve prices')
    for coin in coins:
        trades = sorted(data[coin], key=lambda i: i['timestamp'])
        df_trades = pd.DataFrame(trades)
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
                                          visualize_candlestick(df=df, df_trades=df_trades, symbol=coin, period=period, time=df.index[-1]))
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
                                          visualize_candlestick(df=df, df_trades=df_trades, symbol=coin, period=period, time=df.index[-1]))
                    logger.info('Message posted in {}'.format(dic['channel_name']))
        else:
            logger.info('Last trade for {} is {}. Current signal: {}'.format(coin, last_trade_direction, row['signal']))
            continue


def main():
    logging.info('Retrieve prices for {} assets'.format(len(coins)))
    for coin in coins:
        df_data = get_all_binance(coin, '{}m'.format(period), start_date=get_historical_start_date(5))
        df = calc_strategy(df_data)
        row = df.iloc[-1]
        # Filter signals
        df_signals = df[(df['signal_order'] == 'Long') |
                        (df['signal_order'] == 'Short') |
                        (df['signal_order'] == 'Close')]
        if df_signals.empty:
            logger.info('No history signals in {}'.format(coin))
            continue

        # Prices
        open_price = df_signals['close'].iloc[-1]
        last_price = df['close'].iloc[-1]
        last_signal = df_signals['signal_order'].iloc[-1]
        # Calculate price percent change from open
        if last_signal != 'Close':
            open_position_price_chg = last_price - open_price if last_signal == 'Long' else open_price - last_price
            open_position_price_pct_chg = open_position_price_chg * 100 / open_price
            logger.info('Result for {} {} opened from {}: {}'.format(coin, last_signal, df_signals.index[-1],
                                                                       round(open_position_price_pct_chg, 2)))
            logger.info('{}, sentiment: {}, range: {}'.format(coin, row['prob_ema'], row['ind1']))
        else:
            open_position_price_pct_chg = 0

        # Send message to channel Криптоисследование 2.0 to reenter because of limits on their platform
        if open_position_price_pct_chg >= 5 and df_signals.index[-1] not in sent_messages[coin]:
            sent_messages[coin].append(df_signals.index[-1])
            msg_ru = 'Цена {} изменилась более чем на 5% в Вашу пользу.\n' \
                     'Пожалуйста, перезайдите в позицию если она была автоматически закрыта.'.format(coin)
            send_post_to_telegram('Message', '-1001482165395', msg_ru)
            send_post_to_telegram('Photo', '-1001482165395',
                                  visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
            logger.info('Message about reaching target was sent to Криптоисслдеование 2.0')

        if row['signal_order'] == 'Long' or row['signal_order'] == 'Short':
            logger.info('{} signal in {}'.format(row['signal_order'], coin))
            # Messages
            msg_eng = '{} {} at {}\nThis position is only fraction of our capital. Please, control your risk!'.format(
                row['signal_order'], coin, row['close'])
            msg_ru = u'{} {} по {}\nВ эту позицию мы вложили только небольшую часть нашего капитала.\n' \
                     'Пожалуйста, контролируйте свой риск!'.format(
                'Купить' if row['signal_order'] == 'Long' else 'Продать', coin, row['close'])
            # Send messages to channels
            for dic in d:
                if dic['lang'] == 'ru':
                    send_post_to_telegram('Message', dic['channel_id'], msg_ru)
                else:
                    send_post_to_telegram('Message', dic['channel_id'], msg_eng)
                send_post_to_telegram('Photo', dic['channel_id'],
                                      visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
                logger.info('Message posted in {}'.format(dic['channel_name']))
        elif row['signal_order'] == 'Close':
            logger.info('Close signal in {}'.format(coin))
            # Messages
            msg_eng = '{} {} at {}\nLets move on to next Good trade!'.format(row['signal_order'], coin, row['close'])
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
            logger.info('No signal in {}'.format(coin))


if __name__ == '__main__':
    while seq:
        s = seq[0]
        if s < now:
            seq.pop(0)
            logger.info('Skiped {}'.format(s))
            continue
        logger.info('Waiting...Job will start at {}'.format(s))
        pause.until(datetime(s.year, s.month, s.day, s.hour, s.minute))
        # do something
        logger.info('Job started')
        main2()
        logger.info('Job finished')
        seq.pop(0)
        # Check length
        if len(seq) != 0:
            continue
        else:
            logger.info('Day finished: {}'.format(s))
            t2 = datetime.strptime('{}-{}-{} 00:00:00'.format(s.year, s.month, s.day, s.hour),
                                  '%Y-%m-%d %H:%M:%S')
            # generate new daily sequence
            seq = daily_time_intervals(t2, period)
            logger.info('New Day started: {}'.format(t2))
