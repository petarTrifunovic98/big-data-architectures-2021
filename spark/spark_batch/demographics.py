#!/usr/bin/python

import os
import time
from sqlite3 import Timestamp
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import expr
from pyspark.sql.functions import year
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *

print("<<<<<<<<<<<<<<<<<<<<< SPARK SCRIPT >>>>>>>>>>>>>>>>>>>>>>")
spark = SparkSession.builder.appName("demographics").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

dfDemographicsFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/batch/birth_death_growth_rates.csv")

dfDemographics = dfDemographicsFromCSV
dfDemographics.persist()
dfBirthDeathRate = dfDemographics.select("country_name", "year", "crude_birth_rate", "crude_death_rate", "rate_natural_increase") \
    .where((dfDemographics.year > 1994) & (dfDemographics.year < 2020))

dfMigrations = dfDemographics.select("country_name", "year", "net_migration") \
    .where((dfDemographics.year > 1994) & (dfDemographics.year < 2020))

dfBirthDeathRate.show()
dfMigrations.show()

while True:
    try:
        dfBirthDeathRate.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer/birth_death_rate", mode="ignore")
        dfMigrations.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer/migrations", mode="ignore")
        print("\n\n<<<<<<<<<< SPARK WROTE DEMOGRAPHICS AND MIGRATIONS TO HDFS >>>>>>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<< Failure! Trying again... >>>>>>>>>>>>")
        time.sleep(5)

# dfReadMigr = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/birth_death_rate")

# dfReadBirthDeathR = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/migrations")

dfDemographics.unpersist()