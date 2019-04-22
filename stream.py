import json 
import tweepy
import socket
import re
#from pygeocoder import Geocoder
import geocoder
import time

ACCESS_TOKEN = '<put your access token here>'
ACCESS_SECRET = 'put your access secret here>'
CONSUMER_KEY = 'put your consumer key here>'
CONSUMER_SECRET = 'put your consumer secret here>'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#guncontrolnow'
#hashtag = '#trump'


TCP_IP = 'localhost'
TCP_PORT = 9001


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

def clean_tweet(tweet):
	'''
	Function to remove emoticons and other non textual data from the tweet
        '''

	return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def clean_location(location):
	'''
	Function to remove emoticons and other non textual data from the tweet
        '''

	return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^,0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", location).split())




class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
		raw_tweet = status.text
		print(raw_tweet)
		#conn.send(status.text.encode('utf-8'))
		clean = clean_tweet(raw_tweet)
		#print("----------------")
		#print(clean)
		#print("----------------")
		location = status.user.location
		#print(location)
		#print("----------------")
		lat = 0
		lng = 0
		clean_loc = None
		if(location != None and len(location.split(',')) == 2):	
			clean_loc = clean_location(location)
			#result = Geocoder.geocode(clean_loc)
			result = geocoder.google(clean_loc)
			#coordinates = str(result.latlng)#str(result[0].coordinates)
			lat_lng = result.latlng
			print(lat_lng)
			if(lat_lng != None):
				lat = lat_lng[0]
				lng = lat_lng[1]
				
		#print("================")
		
		dict = {'text':clean,'location':clean_loc,'coordinates':[lat,lng]}
		out = json.dumps(dict)
		print(out)        
		out = out + "\n"  #to add new line after every instance
		conn.send(out.encode("utf-8"))

    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)


myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])


