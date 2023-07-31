import pandas as pd
import numpy as np


def convert_df(df, col_types):

    for x, y in zip(list(df.columns), col_types):
        if y == 'integer':
            # df[x] = df[x].fillna(-9223372036854775808).astype('int').replace({-9223372036854775808: None})
            df[x] = df[x].fillna(-9999999999).astype('int').replace({-9999999999: np.nan})
        elif y == 'float':
            # df[x] = df[x].fillna(-1.7e+308).astype('float').replace({-1.7e+308: None})
            df[x] = pd.to_numeric(df[x], errors='ignore')
        elif y == 'datetime':
            # min_dt = pd.Timestamp.min
            # max_dt = pd.Timestamp.max
            # df[x] = df[x].fillna(min_dt)
            # df[x] = pd.to_datetime(df[x], errors='coerce').replace({None: max_dt}).replace({min_dt: None})
            # df[x] = df[x].astype('datetime64')
            df[x] = pd.to_datetime(df[x], errors='ignore')
        else:
            df[x] = df[x].astype(y, errors='ignore')

    return df
