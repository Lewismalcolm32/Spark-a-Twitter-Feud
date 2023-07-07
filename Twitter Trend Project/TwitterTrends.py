#Twitter HTTP Client
#--Uses Twitter API to pass tweets to Spark Streaming instance

import socket 
import sys
import requests #HTTP Requests
import requests_oauthlib
import json

ACCESS_TOKEN = '37060595-FlShwh35livHmS7hozzhUSCViWEhJ4C7feLpdtBjW' #Username (Twitter Account)
ACCESS_SECRET = 'AwcydUYWbIhDBNoChahtyZ0u7d97NGA5eCsTvWem1WL7B' #Password (Twitter Account)
CONSUMER_KEY = 'jnTEuu5nSRGwXyEHbLBjvgzHw' #API Key (Twitter App User Name)
CONSUMER_SECRET = 'DKahlLSTUpS3jvgq8FGQ8E5rpJ22bY7upKOQM364TIU2O8IUlE' #API Secret Key (Twitter App Password)
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

#Create get_tweets function to call Twitter API URL, and return the response for a stream of Tweets
def get_tweets(): #API URL is found at https://developer.twitter.com/en/docs/tweets/sample-realtime/guides/recovery-and-redundancy
	'''Obtains stream of JSON tweets that need to be parsed'''
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True) #When stream=True is set on the request, this avoids reading the content at once into memory for large responses.
	print(query_url, response)
	return response
    #query_url = https://stream.twitter.com/1.1/statuses/filter.json?langauge=en&locations=-130,-20,100,50&track=iphone


#Create function that takes response from get_tweets and extracts tweets' text from
#whole tweets' JSON object
def send_tweets_to_spark(http_request_resp, tcp_connection):
	'''Converts http response to parsable dictonary and obtains tweet
	before sending tweet through TCP communication'''
	for line in http_request_resp.iter_lines():
		try:
			full_tweet = json.loads(line)
			#print(full_tweet)
			tweet_text = full_tweet['text']
			print("Tweet Text: " + tweet_text)
			print ("------------------------------------------")
			tcp_connection.send(tweet_text + '\n')
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)
			print ("------------------------------------------")
			


#Creating the TCP server socket 
serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #AF_NET = IPv4; SOCK_STREAM = TCP (SOCK_DGRAM = UD)P
serv.bind(('localhost', 9009)) #(TCP_IPaddr, TCP_PORTnum)
serv.listen()
print("Waiting for TCP connection...")
#while(True)
conn, addr = serv.accept()
print("Connected... Starting to get tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
#while(True) --- end?
conn.close()
print("Disconnected")

