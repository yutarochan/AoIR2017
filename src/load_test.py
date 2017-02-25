# -*- coding: utf-8 -*-
'''
Basic Data Loading Test
Author: Yuya Jeremy Ong (yjo5006@psu.edu)
'''
import re
import sys
import json
from operator import add
import util.tokenizer as tk

from pyspark import SparkContext, SparkConf

# Initialize Spark
sc = SparkContext()

# Load and Parse JSON Data
FILENAME = 'clinton-20160926'
'''
FILENAME = 'clinton-20161009'
FILENAME = 'clinton-20161019'
FILENAME = 'trump-20160926'
FILENAME = 'trump-20161009'
'''

raw_data = sc.textFile("hdfs:///user/yjo5006/AOIR2017/data/"+FILENAME+".json")
json_data = raw_data.map(lambda x: json.loads(x))

print 'TOTAL TWEETS LOADED: ' + str(json_data.count())

# Preprocessing Phase
tokens = json_data.map(lambda x: tk.preprocess(x['text'], True, True, True, True, True, True, True))

# Remove Stopwords
stopwords = open('res/stopwords.txt', 'rb')
stop = sc.broadcast(stopwords.read().split('\n')[:-1])
tokens = tokens.map(lambda tok: [t for t in tok if t not in stop.value])

# Compute Word Frequency
token_count = tokens.flatMap(lambda x: [(i,1) for i in x]).reduceByKey(add).sortBy(lambda (word, count): count, False).collect()

# Generate Output Files
output = open(FILENAME+'-wordFreq.csv', 'wb')
for x in token_count:
    output.write(str(x[0].encode('utf-8'))+','+str(x[1]).encode('utf-8')+'\n')
output.close()
