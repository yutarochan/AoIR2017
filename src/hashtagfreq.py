# -*- coding: utf-8 -*-
'''
Hashtag Frequency Analysis
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
DATASETS = ['clinton-20160926', 'clinton-20161009', 'clinton-20161019', 'trump-20160926', 'trump-20161009']

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

    # Hashtag Extraction Count
    hashtag_count = json_data.flatMap(lambda x: [(i['text'],1) for i in x['entities']['hashtags']]).reduceByKey(add).sortBy(lambda (word, count): count, False).collect()

    # Generate Output Files
    output = open(FILENAME+'-hashtagfreq.csv', 'wb')
    for x in token_count:
        output.write(str(x[0].encode('utf-8'))+','+str(x[1]).encode('utf-8')+'\n')
    output.close()
