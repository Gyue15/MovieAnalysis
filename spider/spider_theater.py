from requests_html import HTMLSession
import pymongo
import json
from util import *


if __name__ == "__main__":
    session = HTMLSession()
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    database = client["film"]
    connection = database["area_detail_data"]
    for i in get_date_str("2011-01-01", "2019-10-18"):
        url = 'http://www.films.cn/api/top/theater/boxoffice/local?date=%s&size=30' % i
        r = session.request("GET", url).content
        raw_data = json.loads(r)
        data_theater = raw_data['data']
        print(data_theater)
        connection.update_one({'time': i}, {'$set': {'data_theater': data_theater}}, True)
        print("=======")
