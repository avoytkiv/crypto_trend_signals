from trend import calc_strategy
from get_kraken import get_history_kraken
from tools import send_post_to_telegram, visualize_candlestick
import time
import logging
from collections import defaultdict


logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

period = '15'
coins = ['XETHXXBT', 'XETHZUSD', 'XLTCZUSD', 'XLTCXXBT', 'XREPXXBT', 'XXBTZUSD', 'XXRPXXBT', 'BCHXBT']

d = [{'channel_name': 'TradingRoom_VIP channel', 'channel_id': '-1001407228571', 'lang': 'ru'},
     {'channel_name': 'VIP Signal P&C', 'channel_id': '-1001412423063', 'lang': 'eng'},
     {'channel_name': 'Crypto Libertex', 'channel_id': '@libertex_crypto', 'lang': 'eng'},
     {'channel_name': 'Криптоисследование 2.0', 'channel_id': '-1001482165395', 'lang': 'ru'}]

sent_messages = defaultdict(list)
while True:
    logging.info('Retrieve prices for {} assets'.format(len(coins)))
    for coin in coins:
        df_data = get_history_kraken(coin, period)
        df = calc_strategy(df_data)
        row = df.iloc[-1]
        # Filter signals
        df_signals = df[(df['signal_order'] == 'Long') |
                        (df['signal_order'] == 'Sell') |
                        (df['signal_order'] == 'Close')]
        if df_signals.empty:
            logger.info('No history signals in {}'.format(coin))

        last_signal = df_signals['signal_order'].iloc[-1]
        if last_signal == 'Long':
            price_change = (row['close'] - df_signals['close'].iloc[-1]) * 100 / df_signals['close'].iloc[-1]
        elif last_signal == 'Sell':
            price_change = (df_signals['close'].iloc[-1] - row['close']) * 100 / df_signals['close'].iloc[-1]
        else:
            price_change = 0
        # Send message to channel Криптоисследование 2.0 to reenter because of limits on their platform
        if price_change >= 5 and df_signals.date.iloc[-1] not in sent_messages[coin]:
            sent_messages[coin].append(df_signals.date.iloc[-1])
            msg_ru = 'Цена {} изменилась более чем на 5% в Вашу пользу.\n' \
                     'Пожалуйста, перезайдите в позицию если она была автоматически закрыта.'.format(coin)
            send_post_to_telegram('Message', '-1001482165395', msg_ru)
            send_post_to_telegram('Photo', '-1001482165395',
                                  visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
            logger.info('Message about reaching target was sent to Криптоисслдеование 2.0')

        if row['signal_order'] == 'Long' or row['signal_order'] == 'Sell':
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
            msg_ru = u'Закрыть {} по {}\nПереходим к следующему хорошему трейду!'.format(coin, row['close'])
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

    logger.info('Sleep...')
    time.sleep(60 * int(period))
