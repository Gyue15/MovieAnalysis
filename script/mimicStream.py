from hdfs import InsecureClient
import pymongo
import json
import time
import datetime
from dateutil.relativedelta import relativedelta
import os
from config import *
import random


collection, hdfs_client, hdfs_path = Config.get_config(conf=Config.gyue_conf, stream_type=StreamType.AREA)

pattern = '%Y-%m-%d'
beginStr = "2011-11-01"
beginTime = datetime.datetime.strptime(beginStr, pattern)
endStr = "2019-11-01"
endTime = datetime.datetime.strptime(endStr, pattern)
totalMonths = (endTime.year - beginTime.year) * 12 + endTime.month - beginTime.month
nextTime = beginTime
nextStr = nextTime.strftime(pattern)

before = time.time()

for mon in range(totalMonths):
    before0 = time.time()
    nextTime = beginTime + relativedelta(months=1)
    nextStr = nextTime.strftime(pattern)
    print(nextStr)
    items = []
    for item in collection.find({'time': {'$gte': beginStr, '$lt': nextStr}}, {"_id": 0}):
        items.append(item)
        # print(movie)
    print(len(items))
    jsonStr = json.dumps(items)
    string = beginTime.strftime('%Y-%m') + "%sr%s" % (int(time.time()), random.randint(100, 1000))
    f = open('file/' + string + ".txt", mode='w')
    f.write(jsonStr)
    f.close()
    hdfs_client.upload(hdfs_path, 'file/' + string + ".txt")
    os.remove('file/' + string + ".txt")
    beginTime = nextTime
    beginStr = nextStr
    after0 = time.time()
    print(after0 - before0)
    time.sleep(2)
    # time.sleep(0.6)
after = time.time()
# print(after-before)
