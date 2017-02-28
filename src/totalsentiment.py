# -*- coding: utf-8 -*-
'''
Aggregate Sentiment Analysis
Author: Yuya Jeremy Ong (yjo5006@psu.edu)
'''
import re
import sys
import json
import string
from operator import add

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext, SparkConf
sc = SparkContext()

# [Application Parameters]
DATASETS = ['clinton-20160926', 'clinton-20161009', 'clinton-20161019', 'trump-20160926', 'trump-20161009', 'trump-20161019']
