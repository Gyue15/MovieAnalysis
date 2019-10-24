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
        url1 = 'http://www.films.cn/api/top/province/boxoffice?date=%s&size=50' % i
        r1 = session.request("GET", url1).content
        raw_data1 = json.loads(r1)
        data_province = raw_data1['data']
        print(data_province)

        url2 = 'http://www.films.cn/api/top/cinema/boxoffice/local?period=0&date=%s&provinceId=0&cityId=0&size=100' % i
        r2 = session.request("GET", url2).content
        raw_data2 = json.loads(r2)
        data_cinema = raw_data2['data']
        print(data_cinema)

        url3 = 'http://www.films.cn/api/top/city/boxoffice?date=%s&size=500' % i
        r3 = session.request("GET", url3).content
        raw_data3 = json.loads(r3)
        data_city = raw_data3['data']
        print(data_city)

        data = {
            "time": i,
            "data_province": data_province,
            "data_city": data_city,
            "data_cinema": data_cinema
        }

        print(data)
        connection.insert_one(data)

        print("=======")
