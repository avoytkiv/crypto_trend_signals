import pandas as pd
import numpy as np
import tools as tools
import talib.abstract as ta
from scipy import stats
import distance


a = 3
b = 1
timeperiod = 14
resample_period = 240
# range threshold, number of candles
thresh = 18


class Increment:
    def __init__(self):
        self.__increment = 0
        self.__value = 0

    def __call__(self, row):
        if row:
            self.__increment = 1
            self.__value = 0
        else:
            self.__value = self.__value + self.__increment

        return self.__value


def calc_strategy(df_data):
    df_data['timeindex'] = pd.to_datetime(df_data['timestamp'], unit='s')
    df_data.set_index('timeindex', inplace=True)

    curr_day_df = df_data.resample('1D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
    })

    daily_indexes = curr_day_df.index.values
    daily_indexes = np.append(daily_indexes, df_data.index.values[-1])
    list_of_dfs = [df_data.loc[daily_indexes[n]:daily_indexes[n + 1]] for n in range(len(daily_indexes) - 1)]

    df = pd.DataFrame()
    prev_d_high = np.nan
    prev_d_low = np.nan
    prev_d_close = np.nan
    for sample in list_of_dfs:
        sample['d_high'] = sample['high'].cummax(axis=0)
        sample['d_low'] = sample['low'].cummin(axis=0)
        sample['d_open'] = sample['open'].values[0]
        sample['d_close'] = sample['close']

        sample['prev_d_high'] = prev_d_high
        sample['prev_d_low'] = prev_d_low
        sample['prev_d_close'] = prev_d_close

        prev_d_high = sample['d_high'].iloc[-1]
        prev_d_low = sample['d_low'].iloc[-1]
        prev_d_close = sample['d_close'].iloc[-1]

        df = df.append(sample)
        df = df.drop_duplicates(subset='timestamp', keep='last')

    def daily_candle_compare(prev_high, prev_low, prev_close, curr_high, curr_low, curr_open, curr_close):
        InsideDown = curr_high <= prev_high and curr_low >= prev_low and curr_close <= curr_open
        InsideUp = curr_high <= prev_high and curr_low >= prev_low and curr_close > curr_open
        OutsideInsideDownTrans = curr_high > prev_high and curr_close <= prev_high and curr_close > curr_open
        OutsideInsideUpTrans = curr_low < prev_low and curr_close >= prev_low and curr_close < curr_open
        RdrUp = curr_low < prev_low and curr_close > curr_open and curr_close >= prev_low
        RdrDown = curr_high > prev_high and curr_close < curr_open and curr_close <= prev_high
        OutsideUp = curr_close > prev_high and curr_close >= curr_open
        OutsideDown = curr_close < prev_low and curr_close <= curr_open
        GapUp_Red1 = curr_close > prev_high and curr_close < curr_open and (
                (curr_close - prev_close) >= (curr_open - curr_close))
        GapUp_Red2 = curr_close > prev_high and curr_close < curr_open and (
                (curr_close - prev_close) < (curr_open - curr_close))
        GapDown_Green1 = curr_close < prev_low and curr_close > curr_open and (
                (prev_close - curr_close) >= (curr_close - curr_open))
        GapDown_Green2 = curr_close < prev_low and curr_close > curr_open and (
                (prev_close - curr_close) < (curr_close - curr_open))

        candle = 0
        if InsideDown:
            candle = -b
        elif InsideUp:
            candle = b
        elif OutsideInsideDownTrans:
            candle = -b
        elif OutsideInsideUpTrans:
            candle = b
        elif RdrUp:
            candle = a
        elif RdrDown:
            candle = -a
        elif OutsideUp:
            candle = a
        elif OutsideDown:
            candle = -a
        elif GapUp_Red1:
            candle = b
        elif GapUp_Red2:
            candle = -b
        elif GapDown_Green1:
            candle = -b
        elif GapDown_Green2:
            candle = b

        return candle

    df['candle'] = df.apply(lambda row: daily_candle_compare(row['prev_d_high'], row['prev_d_low'], row['prev_d_close'],
                                                             row['d_high'], row['d_low'], row['d_open'],
                                                             row['d_close']), axis=1)

    df['dhigh'] = df['high'].diff()
    df['dlow'] = df['low'].diff()
    df['hl2'] = (df['high'] + df['low']) / 2
    df['hl2_prev'] = df['hl2'].shift(1)
    df['highest3'] = df['high'].rolling(3).max()
    df['lowest3'] = df['low'].rolling(3).min()
    df['f_up_bar'] = (df['dhigh'] > 0) & (df['dlow'] > 0)
    df['f_down_bar'] = (df['dhigh'] < 0) & (df['dlow'] < 0)
    df['f_inside_bar'] = (df['dhigh'] <= 0) & (df['dlow'] >= 0)
    df['f_outside_bar'] = (df['dhigh'] >= 0) & (df['dlow'] <= 0)
    df['f_up_bar_prev'] = df['f_up_bar'].shift(1)
    df['f_down_bar_prev'] = df['f_down_bar'].shift(1)
    df['f_inside_bar_prev'] = df['f_inside_bar'].shift(1)
    df['f_outside_bar_prev'] = df['f_outside_bar'].shift(1)
    df['f_swing_high'] = (df['f_up_bar_prev'] & df['f_down_bar']) | \
                         (df['f_outside_bar_prev'] & df['f_down_bar']) | \
                         (df['f_inside_bar_prev'] & df['f_down_bar']) | \
                         (df['f_up_bar_prev'] & df['f_inside_bar'] & (df['close'] < df['hl2_prev'])) | \
                         (df['f_outside_bar'] & (df['close'] < df['hl2']))
    df['f_swing_low'] = (df['f_down_bar_prev'] & df['f_up_bar']) | \
                        (df['f_outside_bar_prev'] & df['f_up_bar']) | \
                        (df['f_inside_bar_prev'] & df['f_up_bar']) | \
                        (df['f_down_bar_prev'] & df['f_inside_bar'] & (df['close'] > df['hl2_prev'])) | \
                        (df['f_outside_bar'] & (df['close'] > df['hl2']))
    # df['swings_high_lvl0'] = float('nan')

    df.loc[df['f_swing_high'], 'swings_high_lvl0'] = df['highest3']
    df.loc[df['f_swing_low'], 'swings_low_lvl0'] = df['lowest3']
    df['f_swingchart_trend'] = 1
    df['swing_chart_lvl0'] = 0
    for i in range(1, len(df)):
        prev_trend = df['f_swingchart_trend'].iloc[i - 1]
        if prev_trend > 0 and not np.isnan(df['swings_low_lvl0'].iloc[i]):
            df.loc[df.index[i], 'f_swingchart_trend'] = -1
        elif prev_trend < 0 and not np.isnan(df['swings_high_lvl0'].iloc[i]):
            df.loc[df.index[i], 'f_swingchart_trend'] = 1
        else:
            df.loc[df.index[i], 'f_swingchart_trend'] = prev_trend

        dtrend = df['f_swingchart_trend'].iloc[i] - prev_trend
        if dtrend > 0:
            df.loc[df.index[i], 'swing_chart_lvl0'] = df['high'].iloc[i - 1] if np.isnan(
                df['swings_high_lvl0'].iloc[i]) else df['swings_high_lvl0'].iloc[i]
        elif dtrend < 0:
            df.loc[df.index[i], 'swing_chart_lvl0'] = df['low'].iloc[i - 1] if np.isnan(
                df['swings_low_lvl0'].iloc[i]) else df['swings_low_lvl0'].iloc[i]
        else:
            df.loc[df.index[i], 'swing_chart_lvl0'] = df['swing_chart_lvl0'].iloc[i - 1]

    df['swing_chart_lvl0_diff'] = df['swing_chart_lvl0'].diff().fillna(0)
    df.loc[df['swing_chart_lvl0_diff'] != 0, 'zigzag_lvl0'] = df['swing_chart_lvl0']

    df.loc[df['swing_chart_lvl0_diff'] > 0, 'zigzag_lvl0_trend'] = 1
    df.loc[df['swing_chart_lvl0_diff'] < 0, 'zigzag_lvl0_trend'] = -1

    df['c'] = df.loc[df['swing_chart_lvl0_diff'] != 0, 'swing_chart_lvl0'].shift(-1)
    df['d'] = df.loc[df['swing_chart_lvl0_diff'] != 0, 'swing_chart_lvl0']

    df.loc[df['c'] < df['d'], 'resistance_00'] = df['d']
    df['resistance_00'].fillna(inplace=True, method='ffill')
    df['resistance_01'] = df.loc[df['resistance_00'].diff().fillna(0) != 0, 'resistance_00'].shift(1)
    df['resistance_01'].fillna(inplace=True, method='ffill')
    df['resistance_02'] = df.loc[df['resistance_01'].diff().fillna(0) != 0, 'resistance_01'].shift(1)
    df['resistance_02'].fillna(inplace=True, method='ffill')
    df['resistance_03'] = df.loc[df['resistance_02'].diff().fillna(0) != 0, 'resistance_02'].shift(1)
    df['resistance_03'].fillna(inplace=True, method='ffill')
    df['resistance_04'] = df.loc[df['resistance_03'].diff().fillna(0) != 0, 'resistance_03'].shift(1)
    df['resistance_04'].fillna(inplace=True, method='ffill')

    df.loc[df['c'] > df['d'], 'support_00'] = df['d']
    df['support_00'].fillna(inplace=True, method='ffill')
    df['support_01'] = df.loc[df['support_00'].diff().fillna(0) != 0, 'support_00'].shift(1)
    df['support_01'].fillna(inplace=True, method='ffill')
    df['support_02'] = df.loc[df['support_00'].diff().fillna(0) != 0, 'support_01'].shift(1)
    df['support_02'].fillna(inplace=True, method='ffill')
    df['support_03'] = df.loc[df['support_00'].diff().fillna(0) != 0, 'support_02'].shift(1)
    df['support_03'].fillna(inplace=True, method='ffill')
    df['support_04'] = df.loc[df['support_00'].diff().fillna(0) != 0, 'support_03'].shift(1)
    df['support_04'].fillna(inplace=True, method='ffill')


    # Ranking system
    # Assign points due to position of four MAs with periods: 8, 15, 22, 29
    rolling_means = {}
    for i in np.linspace(8, 50, 15):
        X = df['close'].ewm(span=i).mean()
        # X = df['Close].rolling(window=int(i), center=False).mean()
        rolling_means[i] = X
    rolling_means = pd.DataFrame(rolling_means).dropna()

    hamming = pd.Series(index=rolling_means.index)
    thickness = pd.Series(index=rolling_means.index)
    correlation = pd.Series(index=rolling_means.index)

    for date in rolling_means.index:
        mavg_values = rolling_means.loc[date]
        ranking = stats.rankdata(mavg_values.values)
        d = distance.hamming(ranking, range(1, 16))
        _, c = stats.spearmanr(ranking, range(1, 16))
        dif = np.max(mavg_values) - np.min(mavg_values)
        hamming[date] = d
        thickness[date] = dif
        correlation[date] = c
    rolling_means['hamming'] = hamming
    rolling_means['thickness'] = thickness
    rolling_means['correlation'] = correlation
    # rolling_means['ma_points'] = rolling_means['hamming'].map({4: 3, 3: 1, 2: 0, 1: -1, 0: -3})
    rolling_means['ma_points'] = pd.cut(rolling_means['hamming'], bins=[-1, 3, 6, 9, 12, np.inf],
                                        labels=[-3, -1, 0, 1, 3])
    df['ma_points'] = rolling_means['ma_points']
    df['ma_cross_signal'] = rolling_means['correlation']

    # Stochastic
    high = df['high'].astype(int)
    st = ta.STOCH(df, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0,
                  prices=['high', 'low', 'open'])
    df['slowk'] = st['slowk']
    df['slowd'] = st['slowd']

    df.loc[(df['slowd'] < 30) & (df['slowd'] > 20), 'stochastic'] = b
    df.loc[df['slowd'] < 20, 'stochastic'] = a
    df.loc[(df['slowd'] > 70) & (df['slowd'] < 80), 'stochastic'] = -b
    df.loc[df['slowd'] > 80, 'stochastic'] = -a
    df['stochastic'].fillna(0, inplace=True)

    df['avg_vol'] = ta.SMA(df, price='volume', timeperiod=timeperiod)

    df['pct_change'] = df['close'].pct_change()
    df['rolling_std'] = df['close'].rolling(100).std()
    df['close+2std'] = df['close'] + 2 * df['rolling_std']
    df['close-2std'] = df['close'] - 2 * df['rolling_std']

    df.loc[(df['close'] < df['open']) & (df['volume'] > df['avg_vol']), 'vol_points'] = -a
    df.loc[(df['close'] > df['open']) & (df['volume'] > df['avg_vol']), 'vol_points'] = a
    df['vol_points'].fillna(0, inplace=True)

    max_prob = 4 * a
    df['points'] = df['ma_points'].astype(float) + df['candle'] + df['stochastic'] + df['vol_points']
    df['prob'] = ((df['points'] + max_prob) * 100 / (2 * max_prob)).fillna(method='ffill')

    df['prob_ema'] = df['prob'].ewm(span=5).mean()

    # # vwap of time intervals
    df['ohlc4'] = (df['open'] + df['close'] + df['high'] + df['low']) / 4
    # resample
    df['vwap_30Min'] = df.ohlc4.shift(1).resample('{}Min'.format(resample_period), how='last', label='right', closed='right')
    df['vwap_30Min'].fillna(method='ffill', inplace=True)
    # cross count
    df['cross_30Min_vwap'] = tools.cross(df['close'], df['vwap_30Min'])
    df.loc[df.index[0], 'cross_30Min_vwap'] = True
    df['ind1'] = df['cross_30Min_vwap'].apply(Increment())
    # differentiate when above or below vwap
    df['ind1'] = df.apply(lambda x: x['ind1'] * -1 if x['close'] < x['vwap_30Min'] else x['ind1'], axis=1)

    df['long_condition'] = (df['prob_ema'] > 50) & (
            tools.cross_over(df['close'], df['resistance_00']) | tools.cross_over(df['close'], df['resistance_01']))
    df['short_condition'] = (df['prob_ema'] < 50) & (
            tools.cross_under(df['close'], df['support_00']) | tools.cross_under(df['close'], df['support_01']))

    df['close_long_condition'] = (df['prob_ema'] > 50) & (
            tools.cross_over(df['close'], df['resistance_00']) | tools.cross_over(df['close'], df['resistance_01']))
    df['close_short_condition'] = (df['prob_ema'] < 50) & (
            tools.cross_under(df['close'], df['support_00']) | tools.cross_under(df['close'], df['support_01']))

    df.loc[df['long_condition'] & (df['ind1'] >= thresh), 'signal'] = 'Long'
    df.loc[df['short_condition'] & (df['ind1'] <= -thresh), 'signal'] = 'Short'
    df['signal_ffill'] = df['signal'].fillna(method='ffill').fillna('')
    df.loc[df['long_condition'] & (df['ind1'].abs() < thresh) & (df['signal_ffill'] == 'Short'), 'signal'] = 'Close'
    df.loc[df['short_condition'] & (df['ind1'].abs() < thresh) & (df['signal_ffill'] == 'Long'), 'signal'] = 'Close'
    df['signal_ffill'] = df['signal'].fillna(method='ffill')
    df['signal_ffill_shift'] = df['signal_ffill'].shift(1)
    df.loc[df['signal_ffill'] != df['signal_ffill_shift'], 'signal_order'] = df['signal_ffill']
    df['signal_order'] = df['signal_order'].fillna('')

    return df