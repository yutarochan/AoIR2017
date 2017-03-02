'''
Plot Temporal Sentiment Analysis Data
Author: Yuya Jeremy Ong (yuyajeremyong@gmail.com)
'''
from numpy import genfromtxt
import matplotlib.pyplot as plt
import seaborn as sns
import math

sns.set_style("darkgrid")

def parseData(filename):
    data = open(filename, 'rb').read().split('\n')[:-1]
    return [(int(d.split(',')[0]), float(d.split(',')[1])) for d in data]

DATASETS = ['clinton-20160926', 'clinton-20161009', 'trump-20160926', 'trump-20161009']
FILE_DIR = '../data/tempsent/'
SAVE_DIR = '../data/tempsent_plot/'

for FILENAME in DATASETS:
    # Open and Parse CSV
    pos = parseData(FILE_DIR + FILENAME + '-pos_tempsent.csv')
    neg = parseData(FILE_DIR + FILENAME + '-neg_tempsent.csv')

    # Parse X, Y Data
    pos_x = [i[0] for i in pos]
    pos_y = [math.log10(i[1]) for i in pos]
    neg_x = [i[0] for i in neg]
    neg_y = [math.log10(i[1]) for i in neg]

    # Labels
    plt.title('#'+FILENAME.split('-')[0] + ' (' + FILENAME.split('-')[1][4:6] + '/' +
        FILENAME.split('-')[1][6:8] + '/' + FILENAME.split('-')[1][0:4] + '): Temporal Sentiment')
    plt.xlabel("Hour (24-HR)")
    plt.ylabel("Log10 Normalized Frequency");

    plt.plot(pos_x, pos_y, 'o', linestyle='-', label='Positive')
    plt.plot(neg_x, neg_y, 'o', linestyle='-', label='Negative')
    plt.legend()
    plt.show()
