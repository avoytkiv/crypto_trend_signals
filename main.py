from trend import calc_strategy
from get_kraken import get_history_kraken
from tools import send_post_to_telegram, visualize_candlestick
import time
import logging


logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

period = '15'
coins = ['XETHXXBT', 'XETHZUSD', 'XLTCZUSD', 'XLTCXXBT', 'XREPXXBT', 'XXBTZUSD', 'XXRPXXBT', 'BCHXBT']

d = [{'channel_name': 'TradingRoom_VIP channel', 'channel_id': '-1001407228571'},
     {'channel_name': 'VIP Signal P&C', 'channel_id': '-1001412423063'},
     {'channel_name': 'Crypto Libertex', 'channel_id': '@libertex_crypto'}]
#{'channel_name': 'Криптоисследование 2.0', 'channel_id': '-1001482165395'}


while True:
    logging.info('Retrieve prices for {} assets'.format(len(coins)))
    for coin in coins:
        df_data = get_history_kraken(coin, period)
        df = calc_strategy(df_data)
        row = df.iloc[-1]
        if row['signal_order'] == 'Long' or row['signal_order'] == 'Sell':
            logger.info('{} signal in {}'.format(row['signal_order'], coin))
            msg = '{} {} at {}\nThis position is only fraction of our capital. Please, control your risk!'.format(
                row['signal_order'], coin, row['close'])
            for dic in d:
                send_post_to_telegram('Message', dic['channel_id'], msg)
                send_post_to_telegram('Photo', dic['channel_id'],
                                      visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
                logger.info('Message posted in {}'.format(dic['channel_name']))
        elif row['signal_order'] == 'Close':
            logger.info('Close signal in {}'.format(coin))
            msg = '{} {} at {}\nLets move on to next Good trade!'.format(
                row['signal_order'], coin, row['close'])
            for dic in d:
                send_post_to_telegram('Message', dic['channel_id'], msg)
                send_post_to_telegram('Photo', dic['channel_id'],
                                      visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
                logger.info('Message posted in {}'.format(dic['channel_name']))
        else:
            logger.info('No signal in {}'.format(coin))

    logger.info('Sleep...')
    time.sleep(60 * int(period))
