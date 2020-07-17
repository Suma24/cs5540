from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

consumer_key="ZP7bI9huRiUCF6g8lcohyqaV0"
consumer_secret="qQVTCyZX6jU1SvcLDI2k0SNKO8aou4MRfuObnHYJQQoY02Rt2v"
access_token="3405966891-r5CNBJTzrgjQtudAujzm2nhCEkumcriZeUP0ch2"
access_token_secret="GQvL88RMQnn1W2leFsV1YnFJt3CyHmAla2Cj7d9nOnO4J"

class StdOutListener(StreamListener):
    def on_data(self, data):
        try:
            with open('data1.json', 'a') as outfile:
                json.dump(data,outfile)
            with open('data2.json','a') as outputj:
                outputj.write(data)
            with open('tweetsdataimported.txt', 'a') as tweets:
                tweets.write(data)
                tweets.write('\n')
            outfile.close()
            tweets.close()
            outputj.close()
        except BaseException as e:
            print('problem collecting tweet',str(e))
        return True
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['iphone','Samsung','Moto','Redmi','Xiaomi','Nokia','lenovo','oppo','OnePlus','BlackBerry','HTC'])