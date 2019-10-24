import pymongo
from util import *

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
connection = database["film_miss_bak"]
benchmark = database['film_stream_bak']
all_film = set()
real_miss = set()
miss_times = dict()

for film in connection.find():
    all_film.add(film["name"])

for film in all_film:
    temp = film
    find = benchmark.find_one({"movie_name": trans_name(temp)})
    if not find:
        real_miss.add(film)
        miss_times[film] = connection.count_documents({"name": film})

miss_times = sorted(miss_times.items(), key=lambda d: d[1], reverse=True)
print(miss_times)
print(len(real_miss))
