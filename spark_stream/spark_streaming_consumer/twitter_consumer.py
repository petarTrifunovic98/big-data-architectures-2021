from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("Twitter consumer") \
    .getOrCreate()


tweets = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "test_topic") \
  .load()

stopWordList = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",\
                                                        "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",\
                                                        "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",\
                                                        "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",\
                                                        "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by",\
                                                        "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below",\
                                                        "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",\
                                                        "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",\
                                                        "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",\
                                                        "should", "now"]

words = tweets.select(
   explode(
       split(tweets.value, " ")
   ).alias("word")
).filter(~lower(col("word")).isin(stopWordList))

wordCounts = words.groupBy("word").count().orderBy(desc("count")).limit(10)

query = tweets \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .trigger(processingTime='2 seconds') \
#     .start()

query.awaitTermination()