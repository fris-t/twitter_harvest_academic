from logging import error
from re import L
from time import CLOCK_THREAD_CPUTIME_ID
from typing import Text
from twarc import Twarc2, expansions
import json
import datetime
import pandas as pd
import pickle
import mysql.connector
import sqlalchemy
import pymysql
from sqlalchemy.dialects.oracle import VARCHAR2

bearertoken = "__YOUR_BEARER_TOKEN_HERE____YOUR_BEARER_TOKEN_HERE____YOUR_BEARER_TOKEN_HERE__"
#initiate client with bearer_token
client = Twarc2(bearer_token=bearertoken)
 
engine= sqlalchemy.create_engine('YOUR_SERVER_HERE_')

# Specify the start time in UTC for the time period you want Tweets from
#for example: start on 2020 January 1 00:00:00 and end 2020 December 31st 23:59
start_time = datetime.datetime(2020, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)
end_time = datetime.datetime(2020, 12, 31, 23, 59, 0, 0, datetime.timezone.utc)

# Search query
#for example we want to study tweets about climate change
# you can specify multiple search terms divided by OR-operator. 
# You can search for 1 language specific by changing the lang: en part. Use the ISO 639-1 two-letter codes
query = "(climatechange OR 'climate change' OR 'global warming OR #climatechange OR #globalwarming OR #gretaThunberg) lang:en -is:retweet"

# The search_all method call the full-archive search endpoint to get Tweets based on the query, start and end times
search_results = client.search_all(query=query, start_time=start_time, end_time=end_time, max_results=100)

# #Twarc returns all Tweets for the criteria set above, so we page through the results
## Other endpoints are possible as well. See the API documentation for this
tweetnr=1
pagenr=1
for page in search_results:
    df = pd.DataFrame()
    # The Twitter API v2 returns the Tweet information and the user, media etc.  separately
    # so we use expansions.flatten to get all the information in a single JSON
    page = expansions.flatten(page)
    print("ON PAGE NUMBER " + str(pagenr))
    #resultslist.append(page)
    for tweet in page:
        print("Tweet number " + str(tweetnr) + " on page number " + str(pagenr))
        tweet_id = tweet["id"]
        text = tweet["text"]
        author_id = tweet["author_id"]
        conversation_id = tweet["conversation_id"]
        created_at = tweet["created_at"]
        try:
            in_reply_to_user = tweet["in_reply_to_user_id"]
        except:
            in_reply_to_user = "NA"
        lang = tweet["lang"]
        possibly_sensitive = tweet["possibly_sensitive"]
        retweets = tweet["public_metrics"]["retweet_count"]
        replies = tweet["public_metrics"]["reply_count"]
        likes = tweet["public_metrics"]["like_count"]
        quotes = tweet["public_metrics"]["quote_count"]
        source = tweet["source"]
        
        try:
            geo_full_name= tweet["geo"]["full_name"]
        except:
            geo_full_name= "NA"
        
        try:
            geo_country=tweet["geo"]["country"]
        except:
            geo_country= "NA"

        try:
            geo_country_code=tweet["geo"]["country_code"]
        except:
            geo_country_code= "NA"
        
        try:
            geo_coordinates= tweet["geo"]["geo"]["bbox"]
        except:
            geo_coordinates= "NA"
                    

        #referenced tweet level
        try:
            type_retweet = tweet["referenced_tweets"][0]["type"]
        except KeyError:
            type_retweet = "original_tweet"
        try:
            org_tweet_text = tweet["referenced_tweets"][0]["text"]
        except KeyError:
            org_tweet_text = tweet["text"]

        try:
            org_tweet_id = tweet["referenced_tweets"][0]["id"]
        except KeyError:
            org_tweet_id = "NA"

        try:
            org_author_id = tweet["referenced_tweets"][0]["author_id"]
        except KeyError:
            org_author_id = "NA"

        try:
            org_conversation_id = tweet["referenced_tweets"][0]["conversation_id"]
        except KeyError:
            org_conversation_id = "NA"

        try:
            org_created_at = tweet["referenced_tweets"][0]["created_at"]
        except KeyError:
            org_created_at = "NA"

        try:
            org_retweets = tweet["referenced_tweets"][0]["public_metrics"]["retweet_count"]
        except KeyError:
            org_retweets = "NA"
        try:
            org_replies = tweet["referenced_tweets"][0]["public_metrics"]["reply_count"]
        except KeyError:
            org_replies = "NA"

        try:
            org_likes = tweet["referenced_tweets"][0]["public_metrics"]["like_count"]
        except KeyError:
            org_likes = "NA"

        try:
            org_quotes = tweet["referenced_tweets"][0]["public_metrics"]["quote_count"]
        except KeyError:
            org_quotes = "NA"

        org_mentions = []
        try:
            mentionslist = tweet["referenced_tweets"][0]["entities"]["mentions"]
            for mention in mentionslist:
                org_mentions.append(mention["username"])     
        except KeyError:
            org_mentions = "NA"

        org_hashtags =[]
        try:
            for tag in tweet["referenced_tweets"][0]["entities"]["hashtags"]:
                org_hashtags.append(tag["tag"])
        except KeyError:
            org_hashtags = "NA"
            
        org_urls = []
        try:
            for url in tweet["referenced_tweets"][0]["entities"]["urls"]:
                #print(url["expanded_url"])
                org_urls.append(url["expanded_url"])
        except KeyError:
            org_urls = "NA"

        #original tweet account data
        try:
            org_account_username = tweet["referenced_tweets"][0]["author"]["username"]
        except KeyError:
            org_account_username = "NA"

        try:
            org_account_followers = tweet["referenced_tweets"][0]["author"]["public_metrics"]["followers_count"]
        except KeyError:
            org_account_followers = "NA"

        try:
            org_account_following = tweet["referenced_tweets"][0]["author"]["public_metrics"]["following_count"]
        except KeyError:
            org_account_following = "NA"

        try:
            org_account_volume = tweet["referenced_tweets"][0]["author"]["public_metrics"]["tweet_count"]
        except KeyError:
            org_account_volume = "NA"

        try:
            org_account_bio = tweet["referenced_tweets"][0]["author"]["description"]
        except KeyError:
            org_account_bio = "NA"

        try:
            org_account_created_at = tweet["referenced_tweets"][0]["author"]["created_at"]
        except KeyError:
            org_account_created_at = "NA"

        mentions=[]
        try:
            for mention in tweet["entities"]["mentions"]:
                mentions.append(mention["username"])
        except:
            mentions = "NA"

        hashtags =[]
        try:
            for tag in tweet["entities"]["hashtags"]:
                hashtags.append(tag["tag"])
        except:
            hashtags = "NA"

        urls = []
        titles =[]
        descs =[]
        try:
            for url in tweet["entities"]["urls"]:
                urls.append(url["expanded_url"])
                titles.append(url["title"])
                descs.append(url["description"])
        except:
            urls= "NA"
            titles="NA"
            descs= "NA"

        #account level vars
        account_username = tweet["author"]["username"]
        account_followers = tweet["author"]["public_metrics"]["followers_count"]
        account_following = tweet["author"]["public_metrics"]["following_count"]
        account_volume = tweet["author"]["public_metrics"]["tweet_count"]
        account_bio = tweet["author"]["description"]
        account_created_at = tweet["author"]["created_at"]
        try:
            account_location = tweet["author"]["location"]
        except:
            account_location = "NA"
        

        #bind together as row for df
        tweetdata = {
            "tweet_id": tweet_id,
            "text": text,
            "author_id" : author_id,
            "conversation_id": conversation_id,
            "created_at": created_at,
            "in_reply_to_user" : in_reply_to_user,
            "lang" : lang,
            "possibly_sensitive" : possibly_sensitive,
            "retweets" : retweets,
            "replies" : replies, 
            "likes" : likes,
            "quotes" : quotes,
            "source" : source,
            "geo_full_name":geo_full_name,
            "geo_country":geo_country,
            "geo_country_code":geo_country_code,
            "geo_coordinates":str(geo_coordinates),
            "mentions" : str(mentions),
            "hashtags" : str(hashtags),
            "urls" : str(urls),
            "urls_titles" : str(titles),
            "urls_descs" : str(descs),
            "type_retweet" : type_retweet,
            "org_tweet_text" : org_tweet_text,
            "org_tweet_id" : org_tweet_id,
            "org_author_id" : org_author_id,
            "org_conversation_id": org_conversation_id,
            "org_created_at" : org_created_at,
            "org_retweets" : org_retweets,
            "org_replies" : org_replies,
            "org_likes" : org_likes,
            "org_quotes" : org_quotes,
            "org_mentions" : str(org_mentions),
            "org_hashtags" : str(org_hashtags),
            "org_urls" : str(org_urls),
            "org_account_username" : org_account_username,
            "org_account_followers" : org_account_followers,
            "org_account_following" : org_account_following,
            "org_account_volume" : org_account_volume,
            "org_account_bio" : org_account_bio,
            "org_account_created_at" : org_account_created_at,
            "account_username" : account_username,
            "account_followers" : account_followers,
            "account_following" : account_following,
            "account_volume" : account_volume,
            "account_bio" : account_bio,
            "account_created_at" : account_created_at,
            "account_location" : account_location
        }

        df = df.append(tweetdata, ignore_index= True)
        tweetnr = tweetnr+1

    df.to_sql(name='__YOUR_TABLE_NAME__', con=engine, index=False, if_exists='append')
    lastdate= print("last date is: " + str(created_at))

    pagenr=pagenr+1

#You are done!
print("Scraping done!")