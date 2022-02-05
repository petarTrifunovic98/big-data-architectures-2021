#!/usr/bin/python

import os
import time
from sqlite3 import Timestamp
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import count, avg
from pyspark.sql.functions import trim, lit
from pyspark.sql.functions import year
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *


spark = SparkSession.builder.appName("Read main CSV").getOrCreate()

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

customSchemaEarthquakes = StructType().add("id_num", IntegerType()).add("time", StringType()) \
    .add("latitude", StringType()).add("longitude", StringType()).add("depth", StringType()) \
    .add("mag", StringType()).add("magType", StringType()).add("nst", StringType()) \
    .add("gap", StringType()).add("dmin", StringType()).add("rms", StringType()) \
    .add("net", StringType()).add("id", StringType()).add("updated", StringType()) \
    .add("place", StringType()).add("type", StringType()).add("horizontalError", StringType()) \
    .add("depthError", StringType()).add("magError", StringType()).add("status", StringType()) \
    .add("locationSource", StringType()).add("magSource", StringType())   

customSchemaCountries = StructType() \
    .add("country_code", StringType()) \
    .add("country_name", StringType()) \
    .add("country_area", IntegerType()) \

customSchemaStates = StructType() \
    .add("Type", StringType()).add("Name", StringType())

dfEarthquakesFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "false").option("encoding", "Windows-1250").schema(customSchemaEarthquakes) \
    .csv(HDFS_NAMENODE + "/batch/seismic_activity_info.csv")

dfCountriesFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "false").option("encoding", "Windows-1250").schema(customSchemaCountries) \
    .csv(HDFS_NAMENODE + "/batch/country_names_area.csv")

dfUSStatesFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "false").option("encoding", "Windows-1250") \
    .csv(HDFS_NAMENODE + "/batch/us_states.csv")



dfEarthquakes = dfEarthquakesFromCSV.drop("nst", "rms", "net", "updated", "horizontalError", \
    "depthError", "magError", "magNst", "status", "locationSource", "magSource")

dfCountries = dfCountriesFromCSV.drop("country_area", "country_code")

dfUSStates = dfUSStatesFromCSV.withColumn('state_name', trim(dfUSStatesFromCSV.Name)) \
    .withColumn('state_code', trim(dfUSStatesFromCSV.Abbreviation))
dfUSStates = dfUSStates.drop("Type", "Name", "Abbreviation", "Capital", \
    "Population (2015)", "Population (2019)", "area (square miles)")


dfEarthquakes.createOrReplaceTempView("EARTHQUAKES")
dfCountries.createOrReplaceTempView("COUNTRIES")
dfUSStates.createOrReplaceTempView("STATES")

dfEarthquakesCountries = spark.sql("select * from EARTHQUAKES e inner join COUNTRIES c on \
    e.place like concat('%', c.country_name, '%')")
    #or e.place like concat(c.country_name, ' %')
    #or e.place like concat('% ', c.country_name)

dfEarthquakesStates = spark.sql("select * from EARTHQUAKES e inner join STATES s on \
    e.place like concat('%km NE %', s.state_code, '%') or \
    (e.place not like '%km NE %' and (e.place like concat('%', s.state_code, '%'))) or \
    e.place like concat('% ', s.state_name, ' %') or \
    e.place like concat(s.state_name, ' %') or \
    e.place like concat('% ', s.state_name)")

# dfEarthquakesStates = spark.sql("select * from EARTHQUAKES e inner join STATES s on \
#     e.place like concat('%km NE %', s.state_code, '%') or \
#     (e.place not like '%km NE %' and \
#         (e.place like concat('% ', s.state_code, ' %') or \
#          e.place like concat('% ', s.state_code) or \
#          e.place like concat(s.state_code, ' %'))) or \
#     e.place like concat('% ', s.state_name, ' %') or \
#     e.place like concat(s.state_name, ' %') or \
#     e.place like concat('% ', s.state_name)")

dfEarthquakesStates = dfEarthquakesStates.withColumn("country_name", lit("United States"))
dfEarthquakesStates = dfEarthquakesStates.drop("state_code", "state_name")

dfEarthquakesWithCountries = dfEarthquakesCountries.union(dfEarthquakesStates)

dfEarthquakesWithCountries = dfEarthquakesWithCountries.where(dfEarthquakesWithCountries.time > "1995")

dfEarthquakesWithcountries = dfEarthquakesWithCountries. \
    withColumn("year", year(to_timestamp(dfEarthquakesWithCountries["time"]))).drop("time"). \
    where(dfEarthquakesWithCountries.mag.isNotNull())

dfEarthquakesWithcountries.persist()

dfNumQuakesAndAvgMag = dfEarthquakesWithcountries.groupBy("country_name", "year").agg(count(lit(1)).alias("num_quakes"), avg("mag").alias("avg_mag"))
dfNumQuakesWithHighMag = dfEarthquakesWithcountries.filter(dfEarthquakesWithCountries.mag.cast(FloatType()) > 5.5). \
    groupBy("country_name", "year").agg(count(lit(1)).alias("num_quakes_high_mag"))

while True:
    try:
        dfNumQuakesAndAvgMag.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer/num_quakes_avg_mag", mode="ignore")
        dfNumQuakesWithHighMag.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer/num_quakes_high_mag", mode="ignore")
        print("\n\n<<<<<<<<<< SPARK WROTE SEISMIC INFO BY COUNTRY TO HDFS >>>>>>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<< Failure! Trying again... >>>>>>>>>>>>")
        time.sleep(5)
dfEarthquakesWithCountries.unpersist()

dfReadSeismic1 = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/num_quakes_avg_mag")
dfReadSeismic2 = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/num_quakes_high_mag")

dfReadSeismic1.show(10)
dfReadSeismic2.show(10)
