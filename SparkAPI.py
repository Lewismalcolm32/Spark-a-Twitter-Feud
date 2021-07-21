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
