# -*- coding: utf-8 -*-
'''
Sentiment Keyword Rank Analysis
Author: Yuya Jeremy Ong (yjo5006@psu.edu)
'''
import re
import sys
import json
import math
import string
from operator import add

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext, SparkConf
sc = SparkContext()

# [Application Parameters]
DATASETS = ['clinton-20160926', 'clinton-20161009', 'clinton-20161019', 'trump-20160926', 'trump-20161009', 'trump-20161019']

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

    # Preprocessing Phase
    # tokens = json_data.map(lambda x: tk.preprocess(x['text'].encode('utf-8'), True, True, True, True, True, True, True))
    tokens = json_data.map(lambda x: tokens_re.findall(x['text']))
    tokens = tokens.map(lambda tok: [t.lower() for t in tok]) # Text Normalization
    tokens = tokens.map(lambda x: filter(None, [token.rstrip(string.punctuation) for token in x])) # Punctuation Removal
    tokens = tokens.map(lambda tok: filter(lambda x: not x.startswith('http'), tok)) # HTTP Link Removal
    tokens = tokens.map(lambda tok: filter(lambda x: not x.startswith('#'), tok)) # Hashtag Removal
    tokens = tokens.map(lambda tok: filter(lambda x: not x.startswith('@'), tok)) # Username Removal
    tokens = tokens.map(lambda tok: filter(lambda x: x.lower() != 'rt', tok)) # RT Token Removal

    # Load Sentiment Dictionary
    pos_sent = open('res/sent_pos.txt', 'rb')
    pos = sc.broadcast(pos_sent.read().split('\n')[:-1])

    neg_sent = open('res/sent_neg.txt', 'rb')
    neg = sc.broadcast(neg_sent.read().split('\n')[:-1])

    # Compute Aggregate Sentiment
    pos_words = tokens.flatMap(lambda x: [(i, 1) if i in pos.value for i in x]).reduceByKey(add).sortBy(lambda (word, count): count, False).collect()
    neg_words = tokens.flatMap(lambda x: [(i, 1) if i in neg.value for i in x]).reduceByKey(add).sortBy(lambda (word, count): count, False).collect()

    # Generate Output Files
    output = open(FILENAME+'_pos-srank.csv', 'wb')
    for x in pos_words:
        output.write(str(x[0].encode('utf-8'))+','+str(x[1]).encode('utf-8')+'\n')
    output.close()

    output = open(FILENAME+'_neg-srank.csv', 'wb')
    for x in neg_words:
        output.write(str(x[0].encode('utf-8'))+','+str(x[1]).encode('utf-8')+'\n')
    output.close()
