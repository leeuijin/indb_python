#!/bin/bash

BMT_NO=$0
dir=`pwd -P`
time=`date +"%y%m%d_%H%M%S"`
LOGDIR=/$dir/log
LOGFILE=$LOGDIR"/"$BMT_NO".log"

START_TM1=`date "+%Y-%m-%d %H:%M:%S"`
echo "$0: START TIME : " $START_TM1

###### query start
psql -U udba -d skon -e > $LOGFILE 2>&1 <<-!

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

-- executing the PL/Python UDF
truncate table equipment.ts_data_indb_ml;

insert into  equipment.ts_data_indb_ml 
SELECT
        lot_id
       , cell_id
       , param_id
       , timestamp_id_arr
       , equipment.plpy_interp(measure_val_arr) AS measure_val_arr -- plpython UDF
FROM tab1 
;

analyze equipment.ts_data_indb_ml;
!
###### query end

END_TM1=`date "+%Y-%m-%d %H:%M:%S"`

SHMS=`echo $START_TM1 | awk '{print $2}'`
EHMS=`echo $END_TM1   | awk '{print $2}'`

SEC1=`date +%s -d ${SHMS}`
SEC2=`date +%s -d ${EHMS}`
DIFFSEC=`expr ${SEC2} - ${SEC1}`

echo "Result:""|"$BMT_NO"|"$START_TM1"|"$END_TM1"|"$DIFFSEC  >> $LOGFILE
echo "$0: End TIME : "$END_TM1

echo -e "\033[43;31m$0: Total Elapsed TIME : "$DIFFSEC "sec\033[0m"

