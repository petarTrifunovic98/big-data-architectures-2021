#!/bin/bash

/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /home/spark_stream/twitter_consumer.py &
/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /home/spark_stream/twitter_consumer_2.py &
/spark/bin/spark-submit /home/spark_batch/tourism_deps_and_arrs.py &
/spark/bin/spark-submit /home/spark_batch/demographics.py &
#/spark/bin/spark-submit /home/spark_batch/seismic_info_by_country.py &
/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /home/spark_batch/currated.py &
/bin/bash /master.sh