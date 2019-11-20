import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
collection = database['movie_test']
cluster = database['test_res']

clusters = []
for c in cluster.find({}):
    clusters.append(c['res'])

index = 0
for m in collection.find({}):
    if 1 in m['features']:
        collection.update_one({"_id": m['_id']}, {"$set": {"cluster": clusters[index]}})
        index += 1
    else:
        collection.update_one({"_id": m['_id']}, {"$set": {"cluster": ''}})
