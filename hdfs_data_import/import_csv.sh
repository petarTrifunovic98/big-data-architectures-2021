#!/bin/bash

echo ">>>>>>>>>>>>>>>>EXECUTING<<<<<<<<<<<<<<<<<"
hdfs dfs -test -e $CORE_CONF_fs_defaultFS/batch/seismic_activity_info.csv

if [ $? -eq 1 ] 
then
    echo ">>>>>>>>>>>FILE DOES NOT EXIST<<<<<<<<<<<<"
    hdfs dfs -put /data/batch/seismic_activity_info.csv $CORE_CONF_fs_defaultFS/batch/seismic_activity_info.csv
    echo ">>>>>>>>>>>FILE IMPORTED INTO HDFS<<<<<<<<<<<<<"
else
    echo ">>>>>>>>>>>FILE ALREADY EXISTS<<<<<<<<<<<<<<<<<"
fi
