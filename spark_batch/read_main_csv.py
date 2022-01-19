#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Read main CSV").getOrCreate()

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

dfFromCSV = dfFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/batch/seismic_activity_info.csv")

dfFromCSV.printSchema()
dfFromCSV.show(10, truncate=True)