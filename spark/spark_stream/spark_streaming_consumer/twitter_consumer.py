from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


def foreach_batch_action_tweets_with_earthquake(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("overwrite").save()

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

tweetSchema = StructType() \
    .add("text", StringType()) \
    .add("user", StringType())

tweets = tweets.withColumn('data', from_json(tweets.value.cast(StringType()), tweetSchema))

tweetsWithEarthquakeAndMagnitude = tweets.select(tweets.timestamp). \
    filter(lower(tweets.data.text).contains("earthquake") & \
        (lower(tweets.data.text).contains("magnitude") | lower(tweets.data.text).contains("mag")))
tweetsWithEarthquakeAndMagnitudeCount = tweetsWithEarthquakeAndMagnitude. \
    groupBy(window(tweetsWithEarthquakeAndMagnitude.timestamp, "30 seconds")). \
    count()


qTweetsWithEarthquakeAndMagCount = tweetsWithEarthquakeAndMagnitudeCount \
    .writeStream \
    .foreachBatch(foreach_batch_action_tweets_with_earthquake) \
    .outputMode("complete") \
    .start()

qTweetsWithEarthquakeAndMagCount.awaitTermination()