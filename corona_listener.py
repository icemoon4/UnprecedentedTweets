#!/usr/bin/env python3.8

global gmaps, api, TARGET_LANGUAGE
import tweepy
import csv
import googlemaps
from urllib3.exceptions import ProtocolError
import config
import snscrape.modules.twitter as sntwitter

MAX_COUNT = 500
TARGET_LANGUAGE = ['es']
KEYWORDS = ['coronavirus', 'covid', 'pandemic', 'pandemia', 'コロナ', '汎発']


def location_valid(location):
    import googlemaps
    if not location:
        return False
    try:
        place = gmaps.find_place(location, "textquery", fields=["business_status"]) # to exclude random business results
        if place.get('status') == 'OK' and (place.get('candidates', [{}]) == [{}] or place.get('candidates', []) == []):
            return True
        return False
    except googlemaps.exceptions.ApiError:
        return False

def is_target_language(text):
    from textblob import TextBlob
    blob = TextBlob(text)
    if len(text) >= 3 and blob.detect_language() in TARGET_LANGUAGE:
        return True
    return False

def get_tweet_data(status, filename):
    import tweepy
    import csv
    try:
        location = status.user.location
        try:
            quoted_tweet_id = status.quoted_status_id_str
        except AttributeError:
            quoted_tweet_id = None
        quoted_tweet = ""
        if quoted_tweet_id:
            quoted_tweet = api.get_status(quoted_tweet_id, tweet_mode="extended").full_text
        full_text = ''
        try:
            full_text = status.retweeted_status.full_text
        except AttributeError:
            full_text = status.full_text
        
        if location_valid(location) and is_target_language(full_text):
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=',', quotechar='"')
                writer.writerow([status.id, status.created_at, location, full_text, quoted_tweet])
            return "OK"
    except tweepy.TweepError:
        print("Tweet {} deleted".format(status.id))
    return "ERROR"


class CoronaListener(tweepy.StreamListener):
    def __init__(self, *args, **kwargs):
        self.api = kwargs.get('api')
        self.max_count = kwargs.get('max_count')
        #self.target_language = kwargs.get('target_language')
        #self.gmaps = kwargs.get('gmaps')
        #self.count = 0
        self.current_count = kwargs.get('current_count')
        self.filename = kwargs.get('filename')
    
    def on_status(self, status):
        try:
            extended_status = self.api.get_status(status.id, tweet_mode="extended")
            success = get_tweet_data(extended_status, self.filename)
            if success == 'OK':
                self.current_count += 1
            if self.current_count > self.max_count:
                return "Finished"
        except tweepy.TweepError:
            print("Tweet {} deleted".format(status.id))
    
    def on_error(self, status_code):
        if status_code == 420: # nice
            return False


auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
auth.set_access_token(config.access_token, config.access_token_secret)

gmaps = googlemaps.Client(key=config.g_api_key)

api = tweepy.API(auth, wait_on_rate_limit=True)

msl = CoronaListener(api=api, max_count=MAX_COUNT, current_count=0, filename='recent_tweets.csv')
stream = tweepy.Stream(auth=api.auth, listener=msl)

with open('recent_tweets.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')
    writer.writerow(['ID', 'Date', 'Location', 'Tweet', 'Quoted Tweet Text'])

while True:
    try:
        s = stream.filter(languages=TARGET_LANGUAGE, track=KEYWORDS, stall_warnings=True)
        if s == "Finished":
            break
    except (ProtocolError, AttributeError):
        print("ProtocolError, skipping") # me being lazy lol
        continue

stream.disconnect()


# part 2

with open('january_tweets.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')
    writer.writerow(['ID', 'Date', 'Location', 'Tweet', 'Quoted Tweet Text'])

query_kwargs = ' OR '.join(KEYWORDS)
current_count = 0
for t in sntwitter.TwitterSearchScraper('{} since:2020-01-15 until:2020-01-28 lang:{}'.format(query_kwargs, TARGET_LANGUAGE[0])).get_items():
    tweet = api.get_status(t.id, tweet_mode="extended")
    success = get_tweet_data(tweet, 'january_tweets.csv')
    if success == "OK":
        current_count += 1
    if current_count > MAX_COUNT:
        break


with open('march_tweets.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')
    writer.writerow(['ID', 'Date', 'Location', 'Tweet', 'Quoted Tweet Text'])

query_kwargs = ' OR '.join(KEYWORDS)
current_count = 0
for t in sntwitter.TwitterSearchScraper('{} since:2020-03-09 until:2020-03-12 lang:{}'.format(query_kwargs, TARGET_LANGUAGE[0])).get_items():
    tweet = api.get_status(t.id, tweet_mode="extended")
    success = get_tweet_data(tweet, 'march_tweets.csv')
    if success == "OK":
        current_count += 1
    if current_count > MAX_COUNT:
        break

with open('may_tweets.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')
    writer.writerow(['ID', 'Date', 'Location', 'Tweet', 'Quoted Tweet Text'])

query_kwargs = ' OR '.join(KEYWORDS)
current_count = 0
for t in sntwitter.TwitterSearchScraper('{} since:2020-05-01 until:2020-05-15 lang:{}'.format(query_kwargs, TARGET_LANGUAGE[0])).get_items():
    tweet = api.get_status(t.id, tweet_mode="extended")
    success = get_tweet_data(tweet, 'may_tweets.csv')
    if success == "OK":
        current_count += 1
    if current_count > MAX_COUNT:
        break

