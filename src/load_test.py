'''
Basic Data Loading Test
Author: Yuya Jeremy Ong (yjo5006@psu.edu)
'''
import re
import json
import util.twokenize as tk

from pyspark import SparkContext, SparkConf

# Initialize Spark
sc = SparkContext()

# Load and Parse JSON Data
raw_data = sc.textFile("hdfs:///user/yjo5006/AOIR2017/data/clinton-20161019.json")
json_data = raw_data.map(lambda x: json.loads(x))

print 'TOTAL TWEETS LOADED: ' + str(text_file.count())

# Preprocessing Phase
raw_tweets = json_data.map(lambda x: filter(None, [re.sub(r'[^A-Za-z0-9]+', '', x)tk.simple_tokenize(x['text'])])
print raw_tweets.take(1)
