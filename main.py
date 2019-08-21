from trend import calc_strategy
from get_binance import get_all_binance
from tools import send_post_to_telegram, visualize_candlestick
import time
import logging
from collections import defaultdict


logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

period = 15
coins = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'EOSUSDT', 'ADAUSDT', 'LTCBTC', 'EOSETH', 'ETHBTC', 'XMRBTC']

d = [{'channel_name': 'TradingRoom_VIP channel', 'channel_id': '-1001407228571', 'lang': 'ru'},
     {'channel_name': 'VIP Signal P&C', 'channel_id': '-1001412423063', 'lang': 'eng'},
     {'channel_name': 'Crypto Libertex', 'channel_id': '@libertex_crypto', 'lang': 'eng'},
     {'channel_name': 'Криптоисследование 2.0', 'channel_id': '-1001482165395', 'lang': 'ru'}]


sent_messages = defaultdict(list)
while True:
    logging.info('Retrieve prices for {} assets'.format(len(coins)))
    for coin in coins:
        df_data = get_all_binance(coin, '{}m'.format(period))
        df = calc_strategy(df_data)
        row = df.iloc[-1]
        # Filter signals
        df_signals = df[(df['signal_order'] == 'Long') |
                        (df['signal_order'] == 'Short') |
                        (df['signal_order'] == 'Close')]
        if df_signals.empty:
            logger.info('No history signals in {}'.format(coin))
            continue

        df_signals['close_shift'] = df_signals['close'].shift(1)
        df_signals = df_signals.loc[:, ['close', 'close_shift', 'signal_ffill_shift', 'signal_order']]
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

    logger.info('Sleep...')
    time.sleep(60 * period)
