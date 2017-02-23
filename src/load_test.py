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

print 'TOTAL TWEETS LOADED: ' + str(json_data.count())

# Preprocessing Phase

tokens = json_data.map(lambda x: tk.simple_tokenize(x['text']))                                 # Tokenize
tokens = tokens.map(lambda tok: filter(None, [re.sub(r'[^A-Za-z0-9]+', '', x) for x in tok]))   # Remove Empty Strings
tokens = tokens.map(lambda tok: filter(lambda x: x.startswith('http') == False, tok))           # Remove HTTP URLS
tokens = tokens.map(lambda tok: filter(lambda x: x != 'RT', tok))                               # Filter RT Token

print tokens.take(1)
