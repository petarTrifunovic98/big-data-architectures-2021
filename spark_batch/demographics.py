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

spark = SparkSession.builder.appName("demographics").getOrCreate()

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

dfDemographicsFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/batch/birth_death_growth_rates.csv")

dfDemographics = dfDemographicsFromCSV
# dfDemographics.persist()
dfBirthDeathRate = dfDemographics.select("country_name", "year", "crude_birth_rate", "crude_death_rate", "rate_natural_increase") \
    .where((dfDemographics.year > 1994) & (dfDemographics.year < 2020))

dfMigrations = dfDemographics.select("country_name", "year", "net_migration") \
    .where((dfDemographics.year > 1994) & (dfDemographics.year < 2020))

dfBirthDeathRate.show()
dfMigrations.show()