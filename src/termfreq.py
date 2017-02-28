# -*- coding: utf-8 -*-
'''
Term Frequency Counts
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

    # Preprocessing Phase
    # tokens = json_data.map(lambda x: tk.preprocess(x['text'].encode('utf-8'), True, True, True, True, True, True, True))
    tokens = json_data.map(lambda x: tokens_re.findall(x['text']))
    tokens = tokens.map(lambda tok: [t.lower() for t in tok]) # Text Normalization
    tokens = tokens.map(lambda x: filter(None, [token.rstrip(string.punctuation) for token in x])) # Punctuation Removal
    tokens = tokens.map(lambda tok: filter(lambda x: not x.startswith('http'), tok)) # HTTP Link Removal
    tokens = tokens.map(lambda tok: filter(lambda x: not x.startswith('#'), tok)) # Hashtag Removal
    tokens = tokens.map(lambda tok: filter(lambda x: not x.startswith('@'), tok)) # Username Removal
    tokens = tokens.map(lambda tok: filter(lambda x: x.lower() != 'rt', tok)) # RT Token Removal

    # Remove English Stopwords
    stopwords_en = open('res/stopwords.txt', 'rb')
    stop_en = sc.broadcast(stopwords_en.read().split('\n')[:-1])
    tokens = tokens.map(lambda tok: [t for t in tok if t not in stop_en.value])

    # Remove Spanish Stopwords
    stopwords_es = open('res/stopwords_es.txt', 'rb')
    stop_es = sc.broadcast(stopwords_es.read().split('\n')[:-1])
    tokens = tokens.map(lambda tok: [t for t in tok if t not in stop_es.value])

    # Compute Word Frequency
    token_count = tokens.flatMap(lambda x: [(i,1) for i in x]).reduceByKey(add).sortBy(lambda (word, count): count, False).collect()

    # Generate Output Files
    output = open(FILENAME+'-termfreq.csv', 'wb')
    for x in token_count:
        output.write(str(x[0].encode('utf-8'))+','+str(x[1]).encode('utf-8')+'\n')
    output.close()
