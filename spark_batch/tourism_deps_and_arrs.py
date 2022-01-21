#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode
from pyspark.sql.functions import create_map
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName("deps and arrs").getOrCreate()

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

dfArrivalsFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250") \
    .csv(HDFS_NAMENODE + "/batch/tourism_arrivals.csv")
dfArrivals = dfArrivalsFromCSV.select([col(c).cast("string") for c in dfArrivalsFromCSV.columns])

dfDeparturesFromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true").option("encoding", "Windows-1250") \
    .csv(HDFS_NAMENODE + "/batch/tourism_departures.csv")
dfDepartures = dfDeparturesFromCSV.select([col(c).cast("string") for c in dfArrivalsFromCSV.columns])

unpivotExpr = "stack(25, '1995', `1995`, '1996', `1996`, '1997', `1997`, '1998', `1998`, '1999', `1999`, \
    '2000', `2000`, '2001', `2001`, '2002', `2002`, '2003', `2003`, '2004', `2004`, '2005', `2005`, '2006', `2006`, \
    '2007', `2007`, '2008', `2008`, '2009', `2009`, '2010', `2010`, '2011', `2011`, '2012', `2012`, '2013', `2013`, \
    '2014', `2014`, '2015', `2015`, '2016', `2016`, '2017', `2017`, '2018', `2018`, '2019', `2019`)"

unpivotExprArr = unpivotExpr + " as (year, arrivals)"
unpivotExprDep = unpivotExpr + " as (year, departures)"

dfArrivals = dfArrivals.select("Country Name", expr(unpivotExprArr))
dfArrivals.printSchema()
dfArrivals.show()

dfDepartures = dfDepartures.select("Country Name", expr(unpivotExprDep))
dfDepartures.printSchema()
dfDepartures.show()


dfArrivals.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer/arrivals", mode="overwrite")
dfDepartures.write.option("header", "true").csv(HDFS_NAMENODE + "/transformation_layer/departures", mode="overwrite")

# dfReadArrs = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/arrivals")

# dfReadDeps = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true").option("encoding", "Windows-1250").csv(HDFS_NAMENODE + "/transformation_layer/departures")

