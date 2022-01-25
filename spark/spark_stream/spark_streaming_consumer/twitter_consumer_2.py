from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


def foreach_batch_action_relevant_tweets_per_user(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("overwrite").save()


spark = SparkSession \
    .builder \
    .appName("Twitter consumer 2") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb2:27017/streaming_data.seismic") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

tweets = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "test_topic") \
  .load()

# relevantUserList = ["QuakesToday", "earthquakeBot", "USGS_ShakeAlert",\
#     "USGS_Quakes", "USGSted", "SeismicFox1", "EQAlerts", \
#     "raspishakEQ", "earthshook", "VolcanoWatching", "LastQuake", \
#     "SeismoSue", "EMSC", "myearthquakeapp", "MapQuake", "earthb0t", "phivolcs_dost"]
relevantUsers = [Row("QuakesToday"), Row("earthquakeBot"), Row("USGS_ShakeAlert"),\
    Row("USGS_Quakes"), Row("USGSted"), Row("SeismicFox1"), Row("EQAlerts"), \
    Row("raspishakEQ"), Row("earthshook"), Row("VolcanoWatching"), Row("LastQuake"), \
    Row("SeismoSue"), Row("EMSC"), Row("myearthquakeapp"), Row("MapQuake"), Row("earthb0t"), Row("phivolcs_dost")]
usersSchema = StructType().add("user", StringType())
dfRelevantUsers = spark.createDataFrame(spark.sparkContext.parallelize(relevantUsers), usersSchema)

tweetSchema = StructType() \
    .add("text", StringType()) \
    .add("user", StringType())

tweets = tweets.withColumn('data', from_json(tweets.value.cast(StringType()), tweetSchema))

# relevantTweets = tweets.select(
#    tweets.data.user.alias("user"),
#    tweets.timestamp
# ).filter(col("user").isin(relevantUserList))
relevantTweets = tweets.select(
    tweets.data.user.alias("user"),
    tweets.timestamp
).join(dfRelevantUsers, "user")
relevantTweetsCountPerUser = relevantTweets.groupBy(window(relevantTweets.timestamp, "30 seconds"), relevantTweets.user).count()


qRelevantTweetsCountPerUser = relevantTweetsCountPerUser \
    .writeStream \
    .foreachBatch(foreach_batch_action_relevant_tweets_per_user) \
    .outputMode("complete") \
    .start()

qRelevantTweetsCountPerUser.awaitTermination()