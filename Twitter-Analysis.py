import tweepy, pyodbc, dotenv, pickle
from tweepy import OAuthHandler
from CogAPI import CogAPI
from SQLConnection import SQLConnection
from SQLConnection import SQLConnection
from dotenv import load_dotenv
import os
from time import sleep
import datetime

## load authentication details saved in .env file
load_dotenv()

## Authenticate your app
auth = OAuthHandler(os.getenv("TwitterKey"), os.getenv("TwitterSecret"))
auth.set_access_token(os.getenv("TwitterAccessToken"), os.getenv("TwitterAccessSecret"))

## Opens a connection to twitter - respecting rate limits (so you don't get stopped)
apiAccess = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)

## create list to store the tweet data in.
tweets = list()

## settings to use
NumTweets = 4000
SearchTerm = '#okboomer'

print("starting tweet crawling : "+str(datetime))
tweetCount = 1

## specify search criteria and then add each tweet to a list for later use.
## note, this will process about 2600 tweets then sleep for 15 minutes until allowed to grab more then continue.
for tweet in tweepy.Cursor(apiAccess.search, q = SearchTerm+' -filter:retweets', tweet_mode = "extended", lang = 'en').items(NumTweets):
    print("processed tweet #"+str(tweetCount))
    tweets.append(tweet)
    tweetCount += 1

tweetCount = 1
# if some tweets were pulled then log to DB, and get sentiment and key_phrases
if len(tweets) > 0:
    ## connect to DB and log info to db
    connection = SQLConnection(os.getenv("SQLserver"), os.getenv("SQLDatabase"), os.getenv("SQLusername"), os.getenv("SQLPassword"))
    connection.SQLOpenConnection()
    cogService = CogAPI (os.getenv("AZUREsubscription_key"), os.getenv("AZUREendpoint"))
    # ## loop through all tweets
    for tweet in tweets:
        tweetLocation = str()
        ## if tweet location not available use user location
        if(tweet.place == None):
            if(tweet.user.location != None and len(tweet.user.location) > 1):
                tweetLocation = tweet.user.location
            else:
                tweetLocation = 'Unknown'
        else:
            tweetLocation = tweet.place.full_name
        
        ## insert tweet data into TABLE, including calling azure to get sentiment score and key_phrases data
        connection.SQLInsert(\
        command = ("exec TwitterAssignmentDB.insertTweet @TwitterTweetID =?, @SearchTerm=?, @TweetFullText=?,\
                    @TwitterUserName=?, @TweetLocation=?, @TweetTime=?, @NumberOfRetweets=?, @FavoriteCount=?, @SourceApp=?"),\
        args = [tweet.id_str, SearchTerm, tweet.full_text, tweet.user.screen_name, tweetLocation, tweet.created_at, tweet.retweet_count, tweet.favorite_count, tweet.source])
        print("inserted ID# " + tweet.id_str + " tweet #"+str(tweetCount))
        tweetCount += 1
    
    ## send all tweets to cogServices for processing, instead of one at a time the cog services takes a list of tweet
    ## must split each call to a max of 1000 tweets(azure limitation) per second. adding a sleep in between each call
    ## builds a list of all tweets to send
    print("getting sentiment for tweets 0-999 ")
    tweetsSentiment  = list()
    tweetsSentiment.extend(cogService.getSentiment(Tweets = tweets[0:999]))
    sleep(2)
    print("getting sentiment for tweets 1000-1999 ")
    tweetsSentiment.extend(cogService.getSentiment(Tweets = tweets[1000:1999]))
    sleep(2)
    print("getting sentiment for tweets 2000-2999 ")
    tweetsSentiment.extend(cogService.getSentiment(Tweets = tweets[2000:2999]))
    sleep(2)
    print("getting sentiment for tweets 3000-3999 ")
    tweetsSentiment.extend(cogService.getSentiment(Tweets = tweets[3000:3999]))
    sleep(2)
    print("sentiment complete")

    print("getting key_phrases for tweets 0-999")
    tweetsKey_Phrases = list() 
    tweetsKey_Phrases.extend(cogService.getKeyPhrases(Tweets = tweets[0:999]))
    sleep(2)
    print("getting key_phrases for tweets 1000-1999 ")
    tweetsKey_Phrases.extend(cogService.getKeyPhrases(Tweets = tweets[1000:1999]))
    sleep(2)
    print("getting key_phrases for tweets 2000-2999 ")
    tweetsKey_Phrases.extend(cogService.getKeyPhrases(Tweets = tweets[2000:2999]))
    sleep(2)
    print("getting key_phrases for tweets 3000-3999 ")
    tweetsKey_Phrases.extend(cogService.getKeyPhrases(Tweets = tweets[3000:3999]))
    print("key_phrases complete")

    ## keep track of count for the next loop so that the sentiment and key_phrases can be pulled
    tweetCount = 0

    ## loop through tweets again and call azure for sentiment score and key_phrases data, 
    ## this is separate from above loop in case there is a duplicate tweet we don't want to have called azure
    for tweet in tweets:
        ## send sentiment score and key_phrases to DB to be added
        connection.SQLInsert(\
        command = 'EXEC TwitterAssignmentDB.AddSentimentScoreAndKeyPhrases @TweetID = ?, @SentimentScore = ?, @Key_Phrases = ?',
        args= [tweet.id_str, tweetsSentiment[tweetCount], tweetsKey_Phrases[tweetCount]])
        tweetCount += 1
        print("Updated Sentiment and Key_Phrases for ID# " + tweet.id_str+" Line number: "+str(tweetCount))
    
    connection.SQLCloseConnection()
    print("successfully pulled "+str(NumTweets)+" tweets with "+SearchTerm+" from twitter and processed with azure cognitive services for key phrases and sentiment")
else:
    print("no tweets pulled")
