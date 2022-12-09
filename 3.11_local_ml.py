#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SKON PoC
Linear Interpolation of the missing data in time series.
[Step 02] linear interpolation
"""
#%%
## setting the PoC conditions 
lot_num = 100
cell_num = 100
param_num = 10


#%% Greenplum credentials
user = 'gpadmin' 
password = 'changeme' 
host = '10.118.100.60'
port = '5432'
db = 'skon'

connection_string = "postgresql://{user}:{password}@{host}:{port}/{db}".\
        format(user=user, 
               password=password, 
               host=host, 
               port=port,
               db=db)

#%%
# helper function: query to pandas DataFrame
def gpdb_query(query):
    import psycopg2 as pg
    import pandas as pd
    
    conn = pg.connect(connection_string)
    cursor = conn.cursor()
    
    cursor.execute(query)
    col_names = [desc[0] for desc in cursor.description]
    
    result_df = pd.DataFrame(cursor.fetchall(), columns=col_names)
    
    cursor.close()
    conn.close()
    
    return result_df


#%% 
# UDF for running a query

def interpolator(lot_id):
    
    #import pandas as pd
    
    query = """
        SELECT * 
        FROM equipment.ts_data 
        WHERE 
            lot_id = {lot_id}
    """.format(
            lot_id = lot_id)
    
    ts_df = gpdb_query(query)
    ts_df = ts_df.astype({
        'measure_val': float
        })
    
    ## interpolating the missing values for equaly spaced time series data
    ts_df_interpolated = pd.DataFrame()
    
    for cell_id in (np.arange(cell_num)+1):
        for param_id in (np.arange(param_num)+1):
            ts_df_tmp = ts_df[(ts_df.cell_id == cell_id) & (ts_df.param_id == param_id)]
    
            ts_df_tmp.sort_values(by='timestamp_id', ascending=True) # sorting by TimeStamp first
            ts_df_interpolated_tmp = ts_df_tmp.interpolate(method='values') # linear interploation
            ts_df_interpolated_tmp = ts_df_interpolated_tmp.fillna(method='bfill') # backward fill for the first missing row
            
            ts_df_interpolated = pd.concat([ts_df_interpolated, ts_df_interpolated_tmp], axis=0)
    
    # export DataFrame to local folder as a csv file
    #ts_df_interpolated.to_csv(os.path.join(interpolated_dir, file_nm), index=False)        
    #print(file_nm, "is successfully interpolated.")
    
    return ts_df_interpolated


#%%
# 아래 sql 쿼리를 먼저 실행해주세요. 
# drop table if exists ts_data_local_ml;


#%% 
# UDF for importing pandas DataFrame to Greenplum DB
def gpdb_importer(lot_id, connection_string):
    
    import sqlalchemy
    from sqlalchemy import create_engine
    
    engine = create_engine(connection_string)
    
    # interpolation
    ts_data_indb_ml = interpolator(lot_id)
    
    # inserting to Greenplum    
    ts_data_indb_ml.to_sql(
        name = 'ts_data_local_ml', 
        con = engine, 
        schema = 'equipment', 
        if_exists = 'append', 
        index = False, 
        dtype = {'lot_id': sqlalchemy.types.INTEGER(), 
                 'cell_id': sqlalchemy.types.INTEGER(), 
                 'param_id': sqlalchemy.types.INTEGER(),
                 'timestamp_id': sqlalchemy.types.INTEGER(), 
                 'measure_val': sqlalchemy.types.Float(precision=6)
                 })
    

#%%
from datetime import datetime
start_time = datetime.now()

import pandas as pd
import os
import numpy as np

for lot_id in (np.arange(lot_num)+1):
    lot_start_time = datetime.now()
    gpdb_importer(lot_id, connection_string)
    print("lot_id", lot_id, "is successfully interpolated.")
    time_elapsed = datetime.now() - lot_start_time
    print("Time elapsed (hh:mm:ss.ms) {}".format(time_elapsed))

time_elapsed = datetime.now() - start_time
print("----------" * 5)
print("Total elapsed time (hh:mm:ss.ms) {}".format(time_elapsed))
print("----------" * 5)


#%%

