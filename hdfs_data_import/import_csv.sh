#!/bin/bash

while ! jps | grep -q NameNode
do
    echo ">>>>>>>> NAMENODE NOT UP <<<<<<<<<"
    sleep 3
done
echo ">>>>>>>>>> NAMENODE UP <<<<<<<<<<"


while ! hdfs dfs -test -d /batch
do
    echo ">>>>>>>>>>>DIRECTORY DOES NOT EXIST<<<<<<<<<<<<"
    sleep 3
    hdfs dfs -put /home/data/batch /batch
done
echo ">>>>>>>>>>>DIRECTORY IS ON HDFS<<<<<<<<<<<<<<<<<"

