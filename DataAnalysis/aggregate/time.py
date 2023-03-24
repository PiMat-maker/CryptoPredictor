import numpy as np
import pandas as pd


def agg_to_hour_data(df):
    """
    Aggregate data from data for minute to data for hour
    :param df:
    :return: df with new column day_hour. It contains hour of a day
    """
    df["date"] = pd.to_datetime(df["open_time"], unit='ms')

    # docs about freq https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    df = df.groupby(by=[pd.Grouper(key='date', freq='H')]).agg({'open_time': np.min, 'open_price': 'first',
                                                                'high_price': np.max, 'low_price': np.min,
                                                                'close_price': 'last', 'volume': np.sum,
                                                                'close_time': np.max, 'quote_asset_volume': np.sum,
                                                                'trades_number': np.sum,
                                                                'taker_buy_base_asset_volume': np.sum,
                                                                'taker_buy_quote_asset_volume': np.sum})
    df = df.reset_index()
    return df
