'''
This is a test workflow to build a sample Dask dataframe (lazy) over 16g in size, using a datetime index column, partitioned by week, transform the dataframe and then write to a parquet file.

Written by: David Goldstrom
Date: 24 November, 2025
Version: 0.5
Last Edited: 24 November, 2025
'''
# import necessary libraries
import os
import shutil
import dask
# following imports may be used but are not necessary
#import dask.dataframe as dd
#from dask.utils import format_bytes
# import dask.array as da
# import pandas as pd
# import numpy as np 
# import pyarrow as pa
# import pyarrow.parquet as pq
# from dask import delayed

# %% Configure Dask temporary directory
DASK_TEMP_DIR = "C:/Temp/dask_workspace"
if os.path.exists(DASK_TEMP_DIR):
    shutil.rmtree(DASK_TEMP_DIR)
os.makedirs(DASK_TEMP_DIR, exist_ok=True)
dask.config.set({'temporary_directory': DASK_TEMP_DIR})
print(f"Dask temporary directory set to: {DASK_TEMP_DIR}")

# %% Create a large sample Dask dataframe
dask_df = dask.datasets.timeseries(
    '2021-01-01', '2022-01-01',
    freq='1D', partition_freq='1ME', seed=1,  # data frequency
    dtypes={'value': float, 'name': str, 'id': int},  # data types
    id_lam=1000  # control number of items in id column
)

dask_df.tail(1)

# %% Descriptives and Stats
print(dask_df.dtypes)
print(dask_df.npartitions)
# dask_df.shape
dask_df.shape[0].compute() # number of rows
# dask_df.describe().compute() # usual summary statistics
# dask_df.size.compute() #  columns * rows
#df_size = format_bytes(dask_df.memory_usage(deep=True).sum().compute())

# %% Perform Transformation Function
dask_df['rolling_mean_value'] = dask_df['value'].rolling('60min').mean() #.compute()

# %% Write the Dask dataframe to a Parquet file, using existing df partitions

output_path = 'C:/Temp'#'/large_dask_dataframe.parquet'
dask_df.to_parquet(
    output_path,
    engine='pyarrow',
    write_index=True,
    #partition_on=[dd.Grouper(freq='W', key='datetime_index')],
    compression='snappy'
)

