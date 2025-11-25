# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 21:00:06 2025

@author: dgold
"""

import dask
from dask.utils import format_bytes
#import matplotlib

#df = dask.datasets.timeseries(start='2000-01-01', end='2000-01-31', freq='1s', partition_freq='1D', dtypes=None, seed=100)#, **kwargs)

df = dask.datasets.timeseries(
    '2000', '2001',
    freq='2h', partition_freq='1D', seed=1,  # data frequency
    dtypes={'value': float, 'name': str, 'id': int},  # data types
    id_lam=1000  # control number of items in id column
)

df
dir(df)
df.dtypes
df.npartitions
df.shape
df.shape[0].compute() # number of rows
df.describe().compute() # usual summary statistics
df.size.compute() #  columns * rows
format_bytes(df.memory_usage(deep=True).sum().compute())


df.head(3)

df2 = df[df.y > 0]
df3 = df2.groupby("name").x.std()
df3

computed_df = df3.compute()
type(computed_df)

computed_df

df4 = df.groupby("name").aggregate({"x": "sum", "y": "max"})
df4.compute()

df4 = df4.repartition(npartitions=1) # relatively small df can be in 1 partition
joined = df.merge(
    df4, left_on="name", right_index=True, suffixes=("_original", "_aggregated")
)
joined.head()

df = df.persist() # only if enough ram

df[["x", "y"]].resample("1h").mean().head()

%matplotlib inline
df[['x', 'y']].resample('24h').mean().compute().plot();

df[["x", "y"]].rolling(window="24h").mean().head()

df.loc["2000-01-05"]

%time df.loc['2000-01-05'].compute()

df5 = df.set_index("name")
df5

df5 = df5.persist()
df5

%time df5.loc['Alice'].compute()

from sklearn.linear_model import LinearRegression


def train(partition):
    if not len(partition):
        return
    est = LinearRegression()
    est.fit(partition[["x"]].values, partition.y.values)
    return est

df6 = df5.groupby("name").apply(
    train, meta=("LinearRegression", object)
).compute()
df6

