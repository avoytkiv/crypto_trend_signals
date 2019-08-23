import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException


api_key = 'L8wvQXOvuHXcl2xMjiQ3vP7OPMg3G9wOMSzX86OTug7X9GwXHS80wgfWkAQfZB54'
api_secret = 'rgp5iv5I0TRc12uqxSQH0APK9ISSHdmhrajtjKlqrghDQVHEhyD5UmAjcsiLzR1V'
client = Client(api_key, api_secret)


def get_all_binance(symbol, kline_size):
    klines = client.get_historical_klines(symbol, kline_size, '15 August, 2019')
    data = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                         'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    # Remove last row of active candle
    data = data.iloc[:-1]
    # From milliseconds to seconds
    data['timestamp'] = data['timestamp'] / 1000
    data['close_time'] = data['close_time'] + 1

    return data.astype(float)


