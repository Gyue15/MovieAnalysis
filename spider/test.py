import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
collection = database['movie_test']

cnt = 0
total = 0
for m in collection.find({}):
    if (1 in m['features']) and (m['rate'] != ''):
        total += m['box_percent'] * 100
        cnt += 1
print(total / cnt)
