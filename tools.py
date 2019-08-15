import matplotlib.pyplot as plt
from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
import datetime
import plotly.graph_objs as go
import requests


# def corrdot(*args, **kwargs):
#     corr_r = args[0].corr(args[1], 'pearson')
#     corr_text = f"{corr_r:2.2f}".replace("0.", ".")
#     ax = plt.gca()
#     ax.set_axis_off()
#     marker_size = abs(corr_r) * 10000
#     ax.scatter([.5], [.5], marker_size, [corr_r], alpha=0.6, cmap="coolwarm",
#                vmin=-1, vmax=1, transform=ax.transAxes)
#     font_size = abs(corr_r) * 40 + 5
#     ax.annotate(corr_text, [.5, .5,],  xycoords="axes fraction",
#                 ha='center', va='center', fontsize=font_size)
#
# df = get_history_kraken('XXBTZUSD', period)
# sns.set(style='white', font_scale=1.6)
# iris = calc_strategy(df)
# iris = iris.loc[:, ['ma_points', 'stochastic', 'vol_points', 'candle']]
# g = sns.PairGrid(iris, aspect=1.4, diag_sharey=False)
# g.map_lower(sns.regplot, lowess=True, ci=False, line_kws={'color': 'black'})
# g.map_diag(sns.distplot, kde_kws={'color': 'black'})
# g.map_upper(corrdot)


def send_post_to_telegram(type, message):
    """
    :param
    type: str, Message or Photo
    message: str, text that will be send to telegram,
    for Photos message is figure name
    :return: posting message or photo to telegram
    """
    # telegram url
    url = 'https://api.telegram.org/bot'
    token = '744251948:AAFOjpwvLA8tEGlh5j99Tc8HW4ad-qmQ0qI'
    channel_id = '@libertex_crypto'

    common_url = url + token + '/send{}'.format(type) + '?chat_id=' + channel_id
    if type == 'Message':
        final_url = common_url + '&text=' + message
        return requests.post(final_url, message)
    else:
        files = {'photo': open(message, 'rb')}
        return requests.post(common_url, files=files)


def visualize_candlestick(df, symbol, period, time):
    """
    First, we transform time index to matplotlib format timeindex
    :param df: data frame, reset index
    :param symbol: string, use for filename
    :param time:
    :return:
    """
    # if df.shape[0] > 300:
    #     df = df.tail(300)
    f = lambda x: mdates.date2num(datetime.datetime.fromtimestamp(x))
    df['date2num'] = df['date'].apply(f)
    ohlc = df[['date2num', 'open', 'high', 'low', 'close']].values
    # Making plot area
    fig = plt.figure()
    ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=6, colspan=1)
    # Making candlestick plot
    width = .6 / (24 * 60) * int(period)
    candlestick_ohlc(ax1, ohlc, width=width, colorup='g', colordown='r', alpha=0.75)
    # Making signals overlay
    buy_signals = df[df['signal_order'] == 'Long']
    sell_signals = df[df['signal_order'] == 'Sell']
    close_signals = df[df['signal_order'] == 'Close']
    # Plot signals
    plt.scatter(buy_signals['date2num'].values, buy_signals['close'].values, marker='^', label='Buy', s=50, alpha=1)
    plt.scatter(sell_signals['date2num'].values, sell_signals['close'].values, marker='o', label='Sell', s=50, alpha=1)
    plt.scatter(close_signals['date2num'].values, close_signals['close'].values, marker='X', label='Close', s=50, alpha=1)
    # Axis
    ax1.xaxis_date()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(byhour=[0]))
    fig.autofmt_xdate()
    ax1.grid(True)
    # Lables
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('{} {} chart'.format(symbol, period))
    plt.legend()
    # Save figure
    figure_name = '{}-{}min-{}.png'.format(symbol, period, time)
    plt.savefig(figure_name)

    return figure_name


def find_between(s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""


def line_trace(df, column_name, n_trace, fig):
    xaxis = df.index.tolist()
    yaxis = df[column_name].tolist()

    trace = go.Scatter(x=xaxis, y=yaxis, line=dict(width=2))

    fig.append_trace(trace, n_trace, 1)
    return fig


def bband(price, length=50, numsd=2):
    ave = price.rolling(length).mean()
    sd = price.rolling(length).std()
    upband = ave + numsd * sd
    dnband = ave - numsd * sd
    return ave, upband, dnband


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pandas import Series


def cross_over(a, b):
    if isinstance(a, Series):
        prev_a = a.shift(1)
    else:
        prev_a = a

    if isinstance(b, Series):
        prev_b = b.shift(1)
    else:
        prev_b = b

    return (prev_a < prev_b) & (a > b)


def cross_under(a, b):
    if isinstance(a, Series):
        prev_a = a.shift(1)
    else:
        prev_a = a

    if isinstance(b, Series):
        prev_b = b.shift(1)
    else:
        prev_b = b

    return (prev_a > prev_b) & (a < b)

def cross(a, b):
    if isinstance(a, Series):
        prev_a = a.shift(1)
    else:
        prev_a = a

    if isinstance(b, Series):
        prev_b = b.shift(1)
    else:
        prev_b = b

    return ((prev_a > prev_b) & (a < b)) | ((prev_a < prev_b) & (a > b))
