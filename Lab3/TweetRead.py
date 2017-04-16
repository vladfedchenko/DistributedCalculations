import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = 'xt4ApVMsGfSqy2ZmfgYdzmzfW'
consumer_secret = 'LWxmd4EPZADFt7QOCJMVENslirFUx4PfUkPQU5ZUUGVDafhtyz'
access_token = '851018742222000128-iHjlGdICVAIm6R1PO5JTSlxf5NTU31M'
access_secret = 'LFKfkeGCrmbErl5KL9TmvXTkf64ZbJ6L4UfvNWesuNQnt'

hashtag = u'Trump'

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads( data )
            tags = map(lambda x: x[u'text'], msg[u'entities'][u'hashtags'])
            if (hashtag in tags):
                print msg['text'].decode('utf-8')
                self.client_socket.sendall(msg['text'].decode('utf-8') + '\n')
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=[hashtag])

if __name__ == "__main__":
    s = socket.socket()         # Create a socket object
    host = 'localhost'      # Get local machine name
    port = 9999                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print( "Received request from: " + str( addr ) )

    sendData( c )