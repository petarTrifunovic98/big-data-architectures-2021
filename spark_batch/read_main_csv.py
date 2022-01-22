#!/usr/bin/python

import os
from sqlite3 import Timestamp
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
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
dfEarthquakes.printSchema()

dfCountries = dfCountriesFromCSV.drop("country_area", "country_code")
dfCountries.printSchema()

dfUSStates = dfUSStatesFromCSV.withColumn('state_name', trim(dfUSStatesFromCSV.Name)) \
    .withColumn('state_code', trim(dfUSStatesFromCSV.Abbreviation))
dfUSStates = dfUSStates.drop("Type", "Name", "Abbreviation", "Capital", \
    "Population (2015)", "Population (2019)", "area (square miles)")
dfUSStates.printSchema()
dfUSStates.show()


dfEarthquakes.createOrReplaceTempView("EARTHQUAKES")
dfCountries.createOrReplaceTempView("COUNTRIES")
dfUSStates.createOrReplaceTempView("STATES")

dfEarthquakesCountries = spark.sql("select * from EARTHQUAKES e inner join COUNTRIES c on \
    e.place like concat('% ', c.country_name, ' %') or \
    e.place like concat(c.country_name, ' %') or \
    e.place like concat('% ', c.country_name)")
    
#dfEarthquakesStates = dfUSStates.filter(dfUSStates.state_name == 'Arizona')
dfEarthquakesStates = spark.sql("select * from EARTHQUAKES e inner join STATES s on \
    e.place like concat('%km NE %', s.state_code, '%') or \
    (e.place not like '%km NE %' and \
        (e.place like concat('% ', s.state_code, ' %') or \
         e.place like concat('% ', s.state_code) or \
         e.place like concat(s.state_code, ' %'))) or \
    e.place like concat('% ', s.state_name, ' %') or \
    e.place like concat(s.state_name, ' %') or \
    e.place like concat('% ', s.state_name)")

dfEarthquakesStates = dfEarthquakesStates.withColumn("country_name", lit("United States"))
dfEarthquakesStates = dfEarthquakesStates.drop("state_code", "state_name")

dfEarthquakesWithCountries = dfEarthquakesCountries.union(dfEarthquakesStates)

dfEarthquakesWithCountries = dfEarthquakesWithCountries.where(dfEarthquakesWithCountries.time > "1995")
dfEarthquakesWithCountries = dfEarthquakesWithCountries.withColumn("year", year(to_timestamp(dfEarthquakesWithCountries["time"])))\
    .drop("time").groupBy("country_name", "year").count()

dfEarthquakesWithCountries.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer/seismic_info", mode="overwrite")

# dfReadSeismic = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/seismic_info")
