#!/usr/bin/python

import os
from sqlite3 import Timestamp
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import expr
from pyspark.sql.functions import year
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from geopy.geocoders import Bing


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

dfEarthquakesFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "false").option("encoding", "Windows-1250").schema(customSchemaEarthquakes) \
    .csv(HDFS_NAMENODE + "/batch/seismic_activity_info.csv")

dfCountriesFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "false").option("encoding", "Windows-1250").schema(customSchemaCountries) \
    .csv(HDFS_NAMENODE + "/batch/country_names_area.csv")



dfEarthquakes = dfEarthquakesFromCSV.drop("nst", "rms", "net", "updated", "horizontalError", \
    "depthError", "magError", "magNst", "status", "locationSource", "magSource")
dfEarthquakes.printSchema()

dfCountries = dfCountriesFromCSV.drop("country_area")
dfCountries.printSchema()


dfEarthquakes.createOrReplaceTempView("EARTHQUAKES")
dfCountries.createOrReplaceTempView("COUNTRIES")
dfEarthquakes = spark.sql("select * from EARTHQUAKES e inner join COUNTRIES c on e.place like concat('% ', c.country_name, ' %')")
dfEarthquakes = dfEarthquakes.where(dfEarthquakes.time > "1995")
dfEarthquakes = dfEarthquakes.withColumn("year", year(to_timestamp(dfEarthquakes["time"]))).drop("time") \
   .groupBy("country_name", "year").count()

dfToWrite = dfEarthquakes
dfToWrite.show()
#dfToWrite.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer")
#dfEarthquakes.show(30, truncate=False)