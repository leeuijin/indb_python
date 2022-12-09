#!/bin/bash

BMT_NO=$0
dir=`pwd -P`
time=`date +"%y%m%d_%H%M%S"`
LOGDIR=/$dir/log
LOGFILE=$LOGDIR"/"$BMT_NO".log"

START_TM1=`date "+%Y-%m-%d %H:%M:%S"`
echo "$0: START TIME : " $START_TM1

while true
do
###### query start
psql -U udba -d skon -se > $LOGFILE 2>&1 <<-!

select 
(select count(*) from equipment.ts_data) ts_data
,(select count(*) from equipment.ts_data_indb_ml) ts_data_indb_ml
,(select count(*) from (select unnest(timestamp_id_arr) cnt from  equipment.ts_data_indb_ml) tmp) unnest_indb_cnt
,(select count(*) from equipment.ts_data_local_ml) ts_data_local_ml
;
!
###### query end
tail -n 6 $LOGFILE
sleep 2
done

END_TM1=`date "+%Y-%m-%d %H:%M:%S"`

SHMS=`echo $START_TM1 | awk '{print $2}'`
EHMS=`echo $END_TM1   | awk '{print $2}'`

SEC1=`date +%s -d ${SHMS}`
SEC2=`date +%s -d ${EHMS}`
DIFFSEC=`expr ${SEC2} - ${SEC1}`

echo "Result:""|"$BMT_NO"|"$START_TM1"|"$END_TM1"|"$DIFFSEC  >> $LOGFILE
echo "$0: End TIME : "$END_TM1
echo -e "\033[43;31m$0: Total Elapsed TIME : "$DIFFSEC "sec\033[0m"

