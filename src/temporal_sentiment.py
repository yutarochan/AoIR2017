# -*- coding: utf-8 -*-
'''
Twitter Temporal Sentiment cAnalysis
Author: Yuya Jeremy Ong (yjo5006@psu.edu)
'''
import re
import sys
import json
import string
from operator import add
import util.tokenizer as tk

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext, SparkConf
sc = SparkContext()

# [Application Parameters]
# DATASETS = ['clinton-20160926', 'clinton-20161009', 'clinton-20161019', 'trump-20160926', 'trump-20161009']
DATASETS = ['trump-20161019']

# Preprocessing Regex
emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

regex_str = [
    # emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs

    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]

tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)

# Process Dataset
for FILENAME in DATASETS:
    # Import Dataset
    raw_data = sc.textFile("hdfs:///user/yjo5006/AOIR2017/data/"+FILENAME+".json")
    json_data = raw_data.map(lambda x: json.loads(x))
    print 'TOTAL TWEETS LOADED: ' + str(json_data.count())

    # Filter Retweets
    json_data = json_data.filter(lambda x: not x['text'].startswith('RT'))

    # Preprocess and Tokenize Data
    tweets = json_data.map(lambda x: (int(x['created_at'].split(' ')[3].split(':')[0]), x['text']))
    tweets = tweets.map(lambda x: (x[0], tokens_re.findall(x[1])))
    tweets = tweets.map(lambda tok: (tok[0], [t.lower() for t in tok[1]])) # Text Normalization
    tweets = tweets.map(lambda x: (x[0], filter(None, [token.rstrip(string.punctuation) for token in x[1]]))) # Punctuation Removal
    tweets = tweets.map(lambda tok: (tok[0], filter(lambda x: not x.startswith('http'), tok[1]))) # HTTP Link Removal
    tweets = tweets.map(lambda tok: (tok[0], filter(lambda x: not x.startswith('#'), tok[1]))) # Hashtag Character Remover
    tweets = tweets.map(lambda tok: (tok[0], filter(lambda x: not x.startswith('@'), tok[1]))) # Username Removal
    tweets = tweets.map(lambda tok: (tok[0], filter(lambda x: x.lower() != 'rt', tok[1]))) # RT Token Removal

    # Load Sentiment Dictionary
    pos_sent = open('res/sent_pos.txt', 'rb')
    pos = sc.broadcast(pos_sent.read().split('\n')[:-1])

    neg_sent = open('res/sent_neg.txt', 'rb')
    neg = sc.broadcast(neg_sent.read().split('\n')[:-1])

    # Count Temporal Sentiment
    pos = tweets.flatMap(lambda x: [(x[0], 1) for i in x[1] if i in pos.value]).reduceByKey(add).sortByKey().collect()
    neg = tweets.flatMap(lambda x: [(x[0], 1) for i in x[1] if i in neg.value]).reduceByKey(add).sortByKey().collect()

    # Generate Output Files
    output = open(FILENAME+'-pos_tempsent.csv', 'wb')
    for x in pos: output.write(str(x[0])+','+str(x[1])+'\n')
    output.close()

    output = open(FILENAME+'-neg_tempsent.csv', 'wb')
    for x in neg: output.write(str(x[0])+','+str(x[1])+'\n')
    output.close()
