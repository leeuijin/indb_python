
# indb_python

################################ Purpose of test ################################

The data preprocessing process goes through several processes.

Usually, the data preprocessing process takes data from the database, goes through the preprocessing process, and then writes it back to the database.

This process involves significant inefficient work and requires a lot of time in the process of exporting and importing data rather than pretreatment.

Greenplum can preprocess the database itself through pl/python
The MPP architecture allows for parallel preprocessing and allows for faster preprocessing.

Through this test, it was conducted to confirm the efficiency and rapid preprocessing of the Greenplum database preprocessing.

#################################################################################
# test
#################################################################################

#### create test table 
psql -d mydb -f ./load/create_ts_data.sql

equipment.ts_data          : Table Before Interpolation
equipment.ts_data_local_ml : Interpolated with python code (local server or Laptop)
equipment.ts_data_indb_ml : Interpolated with pl/python & Greenplum

#### create sample data set 
./3.01_gen_ts_data.sh OR python ./3.01_gen_ts_data.py

#### check sample data set (ts_data)

skon=# select * from equipment.ts_data limit 20;
 lot_id | cell_id | param_id | timestamp_id |    measure_val
--------+---------+----------+--------------+--------------------
     15 |      16 |        7 |           93 | 5.5638489074846165
     15 |      16 |        7 |           94 |  8.482529973773792
     15 |      16 |        7 |           95 | 13.060923447957842
     15 |      16 |        7 |           96 |
     15 |      16 |        7 |           97 |  16.94887276014358
     15 |      16 |        7 |           98 |  9.191982968073475
     15 |      16 |        7 |           99 |  9.774796026424681
     15 |      16 |        7 |          100 |  7.450130404805947
     15 |      16 |        8 |            1 |  8.785590409513478
     15 |      16 |        8 |            2 |  9.793442160617602
     15 |      16 |        8 |            3 |  8.851026436141359
     15 |      16 |        8 |            4 |
     15 |      16 |        8 |            5 |  7.919585900124809
     15 |      16 |        8 |            6 |
     15 |      16 |        8 |            7 | 12.419575236133463
     15 |      16 |        8 |            8 | 10.871508670720477
     15 |      16 |        8 |            9 | 13.924408165114958
     15 |      16 |        8 |           10 |
     15 |      16 |        8 |           11 | 12.646671787090733
     15 |      16 |        8 |           12 |   9.75590315845674
(20 rows)

##################################################################################################
#                   data preprocessing(Data interpolation work) by Python code                   #
##################################################################################################

#########################
# setting on conditions #
#########################

lot_num = 100
cell_num = 100
param_num = 10


#%% Greenplum credentials
user = 'gpadmin'
password = 'changeme'
host = 'localhost'
port = '5432'
db = 'test'

###################################
# excute Data interpolation work  #
###################################

./3.11_local_ml.sh OR python 3.11_local_ml.py 

##################################################################################################
#                   data preprocessing(Data interpolation work) by PL/Python                     #
##################################################################################################

###################################
#pl/phthon code                   #
###################################
CREATE OR REPLACE FUNCTION equipment.plpy_interp(measure_val_arr numeric[])
RETURNS numeric[]
AS $$
        import numpy as np
        import pandas as pd

        measure_val = np.array(measure_val_arr, dtype='float')

        ts_df = pd.DataFrame({
           'measure_val': measure_val
            })

        # interpolation by lot, cell, and param IDs
        ts_df_interpolated = ts_df.interpolate(method='values') # linear interploation
        ts_df_interpolated = ts_df_interpolated.fillna(method='bfill') # backward fill for the first missing row

        return ts_df_interpolated['measure_val']

$$ LANGUAGE 'plpythonu';

###################################
#SQL code                         #
###################################

DROP TABLE IF EXISTS tab1;
CREATE TEMPORARY TABLE tab1 AS
SELECT
       lot_id
     , cell_id
     , param_id
     , ARRAY_AGG(timestamp_id ORDER BY timestamp_id) AS timestamp_id_arr
     , ARRAY_AGG(measure_val ORDER BY timestamp_id) AS measure_val_arr
FROM equipment.ts_data
GROUP BY lot_id, cell_id, param_id
DISTRIBUTED RANDOMLY ;

ANALYZE tab1;

insert into  equipment.ts_data_indb_ml
SELECT
        lot_id
       , cell_id
       , param_id
       , timestamp_id_arr
       , equipment.plpy_interp(measure_val_arr) AS measure_val_arr -- plpython UDF
FROM tab1
;
###############################################
# excute Data interpolation work (pl/python)  #
###############################################
./3.21_indb_ml.sh 

##################################################################################################
#             interpolated data check                                                            #
##################################################################################################
./3.31_check_ml.sh

##############################################################################
raw : Table Before Interpolation
local_py : Interpolated with python code
indb_py : Interpolated with pl/python & Greenplum
##############################################################################

 lot_id | cell_id | param_id | timestamp_id |   raw   | local_py | indb_py
--------+---------+----------+--------------+---------+----------+---------
      1 |       1 |        1 |            1 | 13.3904 |  13.3904 | 13.3904
      1 |       1 |        1 |            2 |         |  11.0671 | 11.0671
      1 |       1 |        1 |           21 |         |   9.7938 |  9.7938
      1 |       1 |        1 |            3 |  8.7438 |   8.7438 |  8.7438
      1 |       1 |        1 |           46 |         |  10.2638 | 10.2638
      1 |       1 |        1 |            4 | 12.8205 |  12.8205 | 12.8205
      1 |       1 |        1 |            5 |  5.7031 |   5.7031 |  5.7031
      1 |       1 |        1 |            6 | 12.0334 |  12.0334 | 12.0334
      1 |       1 |        1 |            7 | 12.7073 |  12.7073 | 12.7073
      1 |       1 |        1 |            8 |  4.8995 |   4.8995 |  4.8995
      1 |       1 |        1 |            9 |  6.6540 |   6.6540 |  6.6540
      1 |       1 |        1 |           10 | 14.5093 |  14.5093 | 14.5093
      1 |       1 |        1 |           11 |  8.6384 |   8.6384 |  8.6384
      1 |       1 |        1 |           12 | 12.0011 |  12.0011 | 12.0011
      1 |       1 |        1 |           13 | 13.4893 |  13.4893 | 13.4893
      1 |       1 |        1 |           14 | 10.2551 |  10.2551 | 10.2551
      1 |       1 |        1 |           15 |         |   8.3128 |  8.3128
      1 |       1 |        1 |           43 |         |   9.0967 |  9.0967


##################################################################################################
#                             Interpolation performance check                                    #
##################################################################################################
execute time check :
local_py : 800 sec
indb_py : 44 sec

##############################################################################

################################### Summary ##################################

Greenplum's data preprocessing (interpolation) through pl/python is also efficient,
Traditional Python preprocessing (interpolation) can reduce network and disk costs by allowing the database to be processed internally by exporting data from the database and importing data after preprocessing.
Even in simple tests, you can see 20 times the performance difference.
Similar performance improvements can be expected not only in interpolation but also in various tasks.

##############################################################################

If you want to code review or question
uijinl@vmware.com
geartec82@gmail.com










