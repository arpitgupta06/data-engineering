import pandas as pd
import tweepy
import json
from datetime import datetime
from google.cloud import storage


def run_twitter_etl():
    access_key = " "
    access_secret = " "
    consumer_key = " "
    consumer_secret = " "

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # Creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',
                               # 200 is the maximum allowed count
                               count=200,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

    # Converting JSON into Dataframe
    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        new_tweet = {"user": tweet.user.screen_name,
                     'text': text,
                     'favorite_count': tweet.favorite_count,
                     'retweet_count': tweet.retweet_count,
                     'created_at': tweet.created_at}

        list.append(new_tweet)

    df = pd.DataFrame(list)
    df.to_csv('gs://twitter-etl/new_tweets.csv')
