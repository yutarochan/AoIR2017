'''
Basic Data Loading Test
Author: Yuya Jeremy Ong (yjo5006@psu.edu)
'''
import json
from pyspark import SparkContext, SparkConf

# Initialize Spark
sc = SparkContext()

text_file = sc.textFile("hdfs:///user/yjo5006/AOIR2017/data/clinton-20161019.json")
print 'TOTAL: ' + str(text_file.count())
