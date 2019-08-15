import requests
import pandas as pd
import os


def get_history_kraken(symbol, period):
    """
    Retrieve OHLC data from Kraken exchange.
    :param symbol: 'XZECZJPY'
    :param period: type str, 1, 5, 15, 30, 60, 240, 1440, 10080, 21600
    :return: data frame with 'Date', 'Open', 'High', 'Low', 'Close', 'VVWAP', 'Volume' columns and default index
    """
    path = './data/'

    filename = '{}-{}.csv'.format(symbol, period)
    # Check if we have history file
    if os.access(path+filename, os.R_OK):
        old_df = pd.read_csv(path + filename, index_col=False)
    else:
        old_df = pd.DataFrame()
    # Get last date from history file or set date if no such
    if len(old_df) > 0:
        # s = pd.to_datetime(old_df['date'].iloc[-1], unit='s')
        starting = '{}'.format(old_df['date'].iloc[-1])
    else:
        # Retrieve all available history data
        starting = 0

    ohlc = []

    parameters = {'pair': symbol,'interval': period, 'since': starting}
    response = requests.get('https://api.kraken.com/0/public/OHLC', params=parameters)
    krakohlc = response.json()['result'][symbol]

    for i in range(len(krakohlc)):
        ohlcdata = krakohlc[i][0:7]
        ohlc.append(ohlcdata)
    labels = ['date', 'open', 'high', 'low', 'close', 'vwap', 'volume']
    ohlc_df = pd.DataFrame.from_records(ohlc, columns=labels)
    # Add new data
    new_df = old_df.append(ohlc_df)
    # Save
    new_df = new_df.drop_duplicates(subset='date', keep='last')
    new_df.to_csv(path + filename, index=False)

    # cast data to float
    return new_df.astype(float)


def get_assets_symbols():
    assets_dict_response = requests.get('https://api.kraken.com/0/public/AssetPairs')
    assets_dict_result = assets_dict_response.json()['result']

    return list(assets_dict_result.keys())
