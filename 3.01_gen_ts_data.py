# -*- coding: utf-8 -*-
"""
Linear Interpolation of the missing data in time series.
"""

#%% data generation
# ID: 100
# cell: 100
# parameter: 10
# measure: 100 sec
# 100*100*10*100 = 10,000,000
# insert 10 missing values randomly

#%% setting the directories & conditions
base_dir = '/data/demo/3.in_db_ml/load/ts_data/' # set wity new directory

## setting the PoC conditions 
lot_num = 100
cell_num = 100
param_num = 10
missing_num = 10
ts_num = 100 # number of TimeStamps


#%% [Step 1] generating the sample dataset

import numpy as np
import pandas as pd
import os
from itertools import chain, repeat

## defining the UDF
def ts_random_generator(lot_id, cell_num, param_num, ts_num, missing_num, base_dir):
    # blank DataFrame for saving the sample datasets later
    ts_df = pd.DataFrame()
    
    for cell_id in np.arange(cell_num):
        for param_id in np.arange(param_num):
            # making a DataFrame with colums of lot_id, cell_cd, param_id, ts_id, and measure_val
            ts_df_tmp = pd.DataFrame({
                'lot_id': list(chain.from_iterable(repeat([lot_id + 1], ts_num))), 
                'cell_id': list(chain.from_iterable(repeat([cell_id + 1], ts_num))), 
                'param_id': list(chain.from_iterable(repeat([param_id + 1], ts_num))), 
                'timestamp_id': (np.arange(ts_num) + 1), 
                'measure_val': np.random.normal(10, 3, ts_num)# X~N(mean, stddev, size)
            })
            
            # inserting the missing values randomly
            nan_mask = np.random.choice(np.arange(ts_num), missing_num)
            ts_df_tmp.loc[nan_mask, 'measure_val'] = np.nan
            
            # concatenate the generated random dataset(ts_df_tmp) to the lot based DataFrame(ts_df) 
            ts_df = pd.concat([ts_df, ts_df_tmp], axis=0)
    
    # exporting the DataFrame to local csv file
    base_dir = base_dir
    file_nm = 'lot_' + \
        str(lot_id+1).zfill(4) + \
        '.csv'
        
    ts_df.to_csv(os.path.join(base_dir, file_nm), index=False)
    
    print(file_nm, "is successfully generated.") 


#%% Executing the ts_random_generator UDF above

## running the UDF above using for loop sts\atement
for lot_id in np.arange(lot_num):
    ts_random_generator(
        lot_id, 
        cell_num, 
        param_num, 
        ts_num, 
        missing_num, 
        base_dir
        )


#%%
