from trend import calc_strategy
from get_kraken import get_history_kraken
from tools import send_post_to_telegram, visualize_candlestick
import time
from datetime import datetime, date, timedelta
import pause
import logging
import pandas as pd


logging.basicConfig(format='%(asctime)-15s [%(levelname)s]: %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

period = '15'
start_hr = 8
stop_hr = 20
d1 = str(datetime.now())
d0 = str(date.today()) + ' ' + '00:00:00'
tomorrow_d = datetime.today() + timedelta(days=1)

coins = ['XETHXXBT', 'XETHZUSD', 'XLTCZUSD', 'XLTCXXBT', 'XREPXXBT', 'XXBTZUSD', 'XXRPXXBT', 'BCHXBT']


while True:
    while start_hr * 60 <= datetime.now().hour * 60 + datetime.now().minute <= stop_hr * 60:
        if datetime.now().hour * 60 + datetime.now().minute == start_hr * 60:
            logging.info('Start of trading day')
            overnight_changes = []
            for coin in coins:
                df = get_history_kraken(coin, period)
                df['timeindex'] = pd.to_datetime(df['date'], unit='s')
                df.set_index('timeindex', inplace=True)
                df = df.loc[d0:d1]
                chg = (df['close'].iloc[-1] - df['close'].iloc[0]) * 100 / df['close'].iloc[0]
                overnight_changes.append({'symbol': coin, 'change': chg})
            df_changes = pd.DataFrame(overnight_changes).sort_values('change', ascending=False)
            df_changes.set_index('symbol', inplace=True)
            df_changes.index.name = None
            message = 'Good morning! Trading day just started! Good Luck!\n\nChanges overnight, %:\n\n{}'.format(
                df_changes.to_string())
            send_post_to_telegram('Message', message)
            logger.info('Morning message sent')

        logging.info('Retrieve prices for {} assets'.format(len(coins)))
        for coin in coins:
            df_data = get_history_kraken(coin, period)
            df = calc_strategy(df_data)
            row = df.iloc[-1]
            if row['signal_order'] == 'Long' or row['signal_order'] == 'Sell':
                logger.info('Signal in {} {}'.format(coin, row['signal_order']))
                msg = '{} {} at {}\nThis position is only fraction of our capital. Control your risk!'.format(
                    row['signal_order'], coin, row['close'])
                send_post_to_telegram('Message', msg)
                send_post_to_telegram('Photo',
                                      visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
            elif row['signal_order'] == 'Close':
                logger.info('Close signal in {}'.format(coin))
                msg = '{} {} at {}\nLets move on to next Good trade!'.format(
                    row['signal_order'], coin, row['close'])
                send_post_to_telegram('Message', msg)
                send_post_to_telegram('Photo',
                                      visualize_candlestick(df=df, symbol=coin, period=period, time=df.index[-1]))
            else:
                logger.info('No signal in {}'.format(coin))

        if datetime.now().hour * 60 + datetime.now().minute >= stop_hr * 60:
            logging.info('End of trading day')
            open_positions = []
            for coin in coins:
                df_data = get_history_kraken(coin, period)
                df = calc_strategy(df_data)

                if df['signal_ffill'].iloc[-1] == 'Long':
                    df_long = df[df['signal_order'] == 'Long']
                    open_positions.append({'symbol': coin, 'direction': 'Long', 'price': df_long['close'].iloc[-1],
                                           'stop': df['close-2std'].iloc[-1]})
                elif df['signal_ffill'].iloc[-1] == 'Sell':
                    df_sell = df[df['signal_order'] == 'Sell']
                    open_positions.append({'symbol': coin, 'direction': 'Sell', 'price': df_sell['close'].iloc[-1],
                                           'stop': df['close+2std'].iloc[-1]})
                else:
                    continue
            df_recommended_sl = pd.DataFrame(open_positions)
            df_recommended_sl.set_index('symbol', inplace=True)
            df_recommended_sl.index.name = None
            message = 'End of trading day! Good night\n\nRecommended stop losses overnight:\n\n{}'.format(
                df_recommended_sl.to_string())
            send_post_to_telegram('Message', message)
            logging.info('End of trading day, pause until 7:55 am')
            # 7:55 then wait 5 minute till 8 am
            pause.until(datetime(tomorrow_d.year, tomorrow_d.month, tomorrow_d.day, 7, 60-int(period)))
            logger.info('Its 5 minutes before trading starts')

        time.sleep(60 * int(period))
        logger.info('Sleep...')
