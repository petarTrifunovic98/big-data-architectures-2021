from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


def foreach_batch_action_relevant_tweets(df, epoch_id):
    df.write.format("mongo").mode("overwrite"). \
    option("database", "streaming_data").option("collection", "relevant_tweets_count").save()

def foreach_batch_action_tweets_with_earthquake(df, epoch_id):
    df.write.format("mongo").mode("overwrite"). \
    option("database", "streaming_data").option("collection", "tweets_with_earthquake").save()
    df.show()


spark = SparkSession \
    .builder \
    .appName("Twitter consumer") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/streaming_data.seismic") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

tweets = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "test_topic") \
  .load()

relevantUsers = ["USGS_ShakeAlert", "QuakesToday", "earthquakeBot", "USGS_Quakes", "USGSted", "SeismicFox1", \
    "EQAlerts", "raspishakEQ", "earthshook", "VolcanoWatching", "LastQuake", "SeismoSue", "EMSC", "myearthquakeapp", \
    "MapQuake", "earthb0t", "phivolcs_dost"]

schema = StructType() \
    .add("text", StringType()) \
    .add("user", StringType())

tweets = tweets.withColumn('data', from_json(tweets.value.cast(StringType()), schema))

tweetsWithEarthquakeAndMagnitude = tweets.select(tweets.timestamp). \
    filter(lower(tweets.data.text).contains("earthquake") & \
        (lower(tweets.data.text).contains("magnitude") | lower(tweets.data.text).contains("mag")))
tweetsWithEarthquakeAndMagnitudeCount = tweetsWithEarthquakeAndMagnitude. \
    groupBy(window(tweetsWithEarthquakeAndMagnitude.timestamp, "10 seconds")). \
    count()

relevantTweets = tweets.select(
   tweets.data.user.alias("user"),
   tweets.timestamp
).filter(col("user").isin(relevantUsers))
relevantTweetsCount = relevantTweets.groupBy(window(relevantTweets.timestamp, "30 seconds")).count()

query = relevantTweetsCount \
    .writeStream \
    .foreachBatch(foreach_batch_action_relevant_tweets) \
    .outputMode("complete") \
    .start()

query2 = tweetsWithEarthquakeAndMagnitudeCount \
    .writeStream \
    .foreachBatch(foreach_batch_action_tweets_with_earthquake) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
query2.awaitTermination()