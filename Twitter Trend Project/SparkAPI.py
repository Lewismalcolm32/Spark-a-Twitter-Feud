#SPark Streaming App
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR") #"Notice we have set the log level to ERROR in order to disable most of the logs that Spark writes."

# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2) 

#RDD Checkpointing(?)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)


#---Start Tutorial from here: "Then we define our main DStream dataStream that will connect to the socket server we created before on port 9009 and read the tweets from that port. Each record in the DStream will be a tweet."