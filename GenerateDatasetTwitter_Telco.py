#!/usr/bin/python
# -*- coding: utf-8 -*-

import json 
import datetime
import sys
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor

#Variables that contains the user credentials to access Twitter API 
access_token = "872829922619871233-KFllKaYzOeeVtH0lzWgQfR2CdJ1Igbk"
access_token_secret = "cWVhnR1KcnPfMHrQkcdnRShzTVo7ukfQPUWEUafi3H4W7"
consumer_key = "2HwaYLCbybRNE927v2J75H0NO"
consumer_secret = "kcAWBsfDAR0pnyqt9jpQyhAgyQl4r3t62jIk3JgjjU673sp3PO"

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    global f_twi

    def on_data(self, data):

        d_twt=json.loads(data)
        f_twi.write(str(d_twt)+"\n")
        print (d_twt)

    def on_error(self, status):
        print (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' > Finished by error: '+str(status))



if __name__ == '__main__':

    print (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '> Start process.')
    print('')
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stop = False   

    lst_words=["@verizon","@ATTCustomerCare","@ATTCares","AT&T","@CenturyLink","@CenturyLinkHelp", "Century Link","@FairPoint", "Fair Point","@AskFrontier","@FrontierCorp","AT&T"]

    f_twi=open("/home/albvargas/poloniex/TelcoTweets.txt","a")
    while True:
        try:
            stream = Stream(auth, l)
            api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=5, retry_delay=60)
            stream.filter(track=lst_words)            

        except KeyboardInterrupt:
            stop = True
        except:
            print (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' > Any error, try newly')
            print (str(sys.exc_info()))
            pass
        if stop:
            break

    f_twi.close()

    print('')
    print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '> Succeded.')