# -*- coding: utf-8 -*-
'''
TwitterNLP: Tokenizer
Author: Yuya Jeremy Ong (yuyajeremyong@gmail.com)
'''
import re
import string

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

def tokenize(s):
    return tokens_re.findall(s)

def preprocess(s, lowercase=False, symbols=False, punct=False, http=False):
    tokens = tokenize(s)
    if lowercase: tokens = [token.lower() for token in tokens]
    if punct: tokens = filter(None, [token.rstrip(string.punctuation) for token in tokens])
    if http: tokens = filter(lambda x: x.startswith('http') == False, tokens)
    return tokens

if __name__ == '__main__':
    tweet = 'RT @marcobonzanini: just an example! :D http://example.com #NLP'
    print(preprocess(tweet, lowercase=True, punct=True, http=True))
    # ['RT', '@marcobonzanini', ':', 'just', 'an', 'example', '!', ':D', 'http://example.com', '#NLP']
