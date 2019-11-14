import random
import pymongo
import time
import logging
from util import ProxyConf

client = pymongo.MongoClient('localhost', 27017)
database = client['film']
collection = database['movie_with_rate']
casts = set()
directors = set()
types = set()
editors = set()

movies = []

for m in collection.find({}).sort([('_id', 1)]):
    casts.update(m['casts'])
    directors.update(m['directors'])
    types.update(m['types'])
    editors.update(m['editors'])
    movies.append(m)

types_dic = {}
directors_dic = {}
casts_dic = {}
editors_dic = {}


def fill_dic(feature_dic, feature_set):
    index = 0
    for f in feature_set:
        feature_dic[f] = index
        index += 1
    del feature_set


fill_dic(types_dic, types)
fill_dic(directors_dic, directors)
fill_dic(casts_dic, casts)
fill_dic(editors_dic, editors)

with open('regression_train_data.txt', 'a+') as writer:
    for m in movies:
        str_dic = {}
        for t in m['types']:
            str_dic[types_dic[t]] = '1'
        for d in m['directors']:
            str_dic[len(types_dic) + directors_dic[d]] = '1'
        for c in m['casts']:
            str_dic[len(types_dic) + len(directors_dic) + casts_dic[c]] = '1'
        for e in m['editors']:
            str_dic[len(types_dic) + len(directors_dic) + len(casts_dic) + editors_dic[e]] = '1'
        s = m['rate']
        for k in sorted(str_dic):
            s = s + ' ' + str(k) + ':' + str_dic[k]
        s = s + '\n'
        writer.write(s)
