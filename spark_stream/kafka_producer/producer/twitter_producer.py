#!/usr/bin/python3

import os
import time
import json
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaProducer
import kafka.errors

KAFKA_BROKER=os.environ["KAFKA_BROKER"]
API_key = os.environ["TWITTER_API_KEY"]
API_secret = os.environ["TWITTER_API_SECRET"]
access_token = os.environ["TWITTER_ACCESS_TOKEN"]
access_secret = os.environ["TWITTER_ACCESS_SECRET"]
topic_name = "test_topic"
print(">>>>>>>>>>")
print(KAFKA_BROKER.split(","))
print("<<<<<<<<<<")

#producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(",")[0])
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

class twitterAuth():

    def authenticateTwitterApp(self):
        auth = OAuthHandler(API_key, API_secret)
        auth.set_access_token(access_token, access_token_secret)
        print(">>>>>>AUTH SUCCESSFUL<<<<<<")
        return auth


class TwitterStreamer():

    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        print(">>>>STARTING STREAM<<<<")
        while True:
            print(">>>>>TWEETS<<<<")
            listener = ListenerTS()
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["earthquake"], stall_warnings=True, languages=["en"])


class ListenerTS(Stream):

    def on_status(self, status):
        if hasattr(status, "extended_tweet"):
            text = status.extended_tweet['full_text']
        else:
            text = status.text
        user = status.user.screen_name
        value = {'text': text, 'user': user}
        value_json = json.dumps(value)
        #print(status._json)
        print("<<<<<<<<<<  ", user, "  >>>>>>>>>>>")
        print()
        #print()
        producer.send(topic_name, bytes(value_json, 'utf-8'))
        return True

print(">>>>>PRODUCER<<<<<")
listener = ListenerTS(API_key, API_secret, access_token, access_secret)
listener.filter(track=["earthquake", "#earthquake", "quake", "#quake", "earthquakes", "#earthquakes", "magnitude", "mag"], \
    stall_warnings=True, languages=["en"])
