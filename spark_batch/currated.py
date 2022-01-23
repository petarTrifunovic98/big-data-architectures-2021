#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode
from pyspark.sql.functions import create_map
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName("currated") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/currated-data.seismic") \
    .getOrCreate()

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# --------- Departures and arrivals --------- #
dfArrs = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/arrivals")

dfDeps = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/departures")

# dfReadArrs.show(10)
# dfReadDeps.show(10)
dfArrs.printSchema()
dfDeps.printSchema()

# ------------------------------------------- #
# --------- Migrations and death-birth rates --------- #

dfBirthDeathRate = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/birth_death_rate")

dfMigr = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/migrations")

# dfReadMigr.show(10)
# dfReadDeps.show(10)
dfMigr.printSchema()
dfBirthDeathRate.printSchema()

# ---------------------------------------------------- #
# --------- Seismic info --------- #

dfSeismic = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/seismic_info")

#dfReadSeismic.show(10)
dfSeismic.persist()
dfSeismic.printSchema()

# -------------------------------- #

columnsToJoinOn = ['country_name', 'year']
dfSeismicData = dfSeismic.join(dfArrs, columnsToJoinOn).join(dfDeps, columnsToJoinOn) \
    .join(dfMigr, columnsToJoinOn).join(dfBirthDeathRate, columnsToJoinOn)
dfSeismicData = dfSeismicData.withColumn("occurrence_y", dfSeismicData.year).drop("year")
dfSeismicData.write.format("mongo").mode("overwrite").save()
#dfSeismicAndArrs.printSchema()
#dfSeismicAndArrs.show(20)

