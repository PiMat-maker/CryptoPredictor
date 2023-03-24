import pandas as pd

import aggregate.time as agg_t
import plots.time_diagrams as td


def main():
    df = pd.read_csv("../DataMining/binance_data.csv")
    df.drop_duplicates(inplace=True)
    df = agg_t.agg_to_hour_data(df)
    df["hour"] = df.date.agg(lambda x: x.hour)
    df["day_date"] = df.date.agg(lambda x: x.dt.date)
    td.heatmap(df, "day_date", "hour", "volume")


if __name__ == '__main__':
    main()
