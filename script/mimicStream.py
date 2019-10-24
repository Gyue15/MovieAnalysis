from hdfs import InsecureClient
import pymongo
import json
import time
import datetime
from dateutil.relativedelta import relativedelta
import os


myclient = pymongo.MongoClient('mongodb://localhost:27017/')
# myclient = pymongo.MongoClient('mongodb://172.19.240.38:27017/')
myspider = myclient["sparkpractise"]
# movieCollection = myspider["movieCollection"]
movieCollection = myspider["filmStreamFull"]
#
#
client = InsecureClient("http://localhost:9870", user='shea')
# client.delete("streamInput",True)
# client.makedirs("streamInput")
# #
# items = []
# for movie in movieCollection.find({},{ "_id": 0 }):
#     items.append(movie)
#     # json_str = json.dumps(movie)
# json_str = json.dumps(items)
# # print(json_str)
# client.write("streamInput/"+movie['name']+".txt",json_str,True)

# beginStr = "2011-01-01"
# beginTime = time.strptime(beginStr,'%Y-%m-%d')
# print(beginTime)
# beginTime.tm_mon = beginTime.tm_mon+1
# print(time.mktime(beginTime))
# print(time.asctime(beginTime))
#
# nextTime = time.gmtime(time.mktime(beginTime)+24*60*60)
# print(time.mktime(nextTime))
# print(nextTime)
# nextStr = time.strftime('%Y-%m-%d',nextTime)
# print (nextStr)
#

pattern = '%Y-%m-%d'
beginStr = "2011-11-01"
beginTime = datetime.datetime.strptime(beginStr,pattern)
endStr = "2019-11-01"
endTime = datetime.datetime.strptime(endStr,pattern)
totalMonths = (endTime.year-beginTime.year)*12+endTime.month-beginTime.month
nextTime = beginTime
nextStr = nextTime.strftime(pattern)

before = time.time()

for mon in range(13):
    before0 = time.time()
    nextTime = beginTime + relativedelta(months=1)
    nextStr = nextTime.strftime(pattern)
    print(nextStr)
    items = []
    for movie in movieCollection.find({'time':{'$gte': beginStr,'$lt':nextStr}},{ "_id": 0 }):
        items.append(movie)
        # print(movie)
    print(len(items))
    jsonStr = json.dumps(items)
    str = beginTime.strftime('%Y-%m')
    f = open('file/'+str + ".txt", mode='w')
    f.write(jsonStr)
    f.close()
    client.upload("streamInput",'file/'+str + ".txt")
    os.remove('file/'+str + ".txt")
    beginTime = nextTime
    beginStr = nextStr
    after0 = time.time()
    print(after0-before0)
    time.sleep(1)
    # time.sleep(0.6)
after = time.time()
# print(after-before)


