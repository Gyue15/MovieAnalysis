import pymongo
from util import *

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
collection = database["area_detail_data"]
res = database["area_stream"]

count = 0
for data_list in collection.find({}):
    time = data_list["time"]
    for area in data_list["data_province"]:
        data = {
            "time": time,
            "province_id": area["provinceId"],
            "province": area["provinceName"],
            "box": area["totalBoxoffice"]
        }
        res.insert_one(data)
        count += 1
print(count)
