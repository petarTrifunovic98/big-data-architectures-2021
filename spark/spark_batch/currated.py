#!/usr/bin/python

import os
import time
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
while True:
    try:
        dfArrs = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
            .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/arrivals")

        dfDeps = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
            .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/departures")
        print("\n\n<<<<<<<<< CURATED COMPONENT READ DEPARTURES AND ARRIVALS >>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<<<<< Failure reading! Retrying... >>>>>>>>>>>>>>>>>")
        time.sleep(5)

# dfReadArrs.show(10)
# dfReadDeps.show(10)
dfArrs.printSchema()
dfDeps.printSchema()

# ------------------------------------------- #
# --------- Migrations and death-birth rates --------- #
while True:
    try:
        dfBirthDeathRate = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
            .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/birth_death_rate")

        dfMigr = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
            .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/migrations")
        print("\n\n<<<<<<<<< CURATED COMPONENT READ MIGRATIONS AND DEMOGRAPHICS >>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<<<<< Failure reading! Retrying... >>>>>>>>>>>>>>>>>")
        time.sleep(5)

dfMigr.printSchema()
dfBirthDeathRate.printSchema()

# ---------------------------------------------------- #
# --------- Seismic info --------- #
while True:
    try:
        dfNumQuakesAndAvgMag = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
            .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/num_quakes_avg_mag")
        dfNumQuakesWithHighMag = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
            .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/num_quakes_high_mag")
        print("\n\n<<<<<<<<< CURATED COMPONENT READ SEISMIC INFO >>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<<<<< Failure reading! Retrying... >>>>>>>>>>>>>>>>>")
        time.sleep(5)

# -------------------------------- #

columnsToJoinOn = ['country_name', 'year']
dfSeismicData = dfNumQuakesAndAvgMag.join(dfNumQuakesWithHighMag, columnsToJoinOn)\
    .join(dfArrs, columnsToJoinOn).join(dfDeps, columnsToJoinOn) \
    .join(dfMigr, columnsToJoinOn).join(dfBirthDeathRate, columnsToJoinOn)
dfSeismicData = dfSeismicData.withColumn("occurrence_y", dfSeismicData.year).drop("year")
dfSeismicData.write.format("mongo").mode("overwrite").save()

