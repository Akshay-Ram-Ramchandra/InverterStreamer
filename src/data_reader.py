import os
import pandas as pd
import datetime
from aws_s3.read_s3 import get_csv_for_stream


def get_inverter_data(df):
    # print(df.columns)
    t = pd.to_datetime(datetime.datetime.now())
    d = t.dayofweek
    h = t.hour
    m = t.minute
    s = t.second
    data = df[(df['day'] == d) & (df['hour'] == h) & (df['min'] == m) & (df['sec'] == s)][
        ['AphA', 'Conn', 'Conn_WinTms',
         'Hz', 'OutPFSet',
         'OutPFSet_RmpTms', 'PF',
         'PhVphA', 'Ris', 'St',
         'StActCtl',
         'VA', 'VAMax', 'VAr',
         'VArMaxPct', 'VArPct_RmpTms',
         'VRef', 'W', 'WH',
         'WMaxLimPct',
         'WMaxLimPct_RmpTms', 'DCV',
         'DCA', 'DCW']].to_dict(
        'records')
    if data:
        return data[0]
    else:
        return {}
    # d = df[('day' == 1)][['AphA', 'Conn', 'Conn_WinTms', 'Hz', 'OutPFSet',
    # 'OutPFSet_RmpTms', 'PF', 'PhVphA', 'Ris', 'St', 'StActCtl',
    # 'VA', 'VAMax', 'VAr', 'VArMaxPct', 'VArPct_RmpTms', 'VRef', 'W', 'WH', 'WMaxLimPct', 'WMaxLimPct_RmpTms', 'DCV',
    # 'DCA', 'DCW']]


# ddict = df.to_dict()
# print(ddict.keys())
# print(df.head())
# print(df.columns)
# df['time'] = pd.to_datetime(df['datetimestamp'], utc=True)
# df['time'] = df['time'].apply(lambda x: x.tz_convert('America/Chicago'))
# df['day'] = df['time'].dt.dayofweek
# df['hour'] = df['time'].dt.hour
# df['min'] = df['time'].dt.minute
# df['sec'] = df['time'].dt.second
#
# print(df.head())
# df.to_csv("data/494654.csv")

if __name__ == "__main__":
    df = pd.read_csv("dataset/494654.csv")
    get_inverter_data(df=df)
