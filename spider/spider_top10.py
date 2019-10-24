from requests_html import HTMLSession
import datetime
import pymongo
import json
from util import *


if __name__ == "__main__":
    session = HTMLSession()
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    database = client["film"]
    connection = database["film_top10"]
    for i in get_date_str("2017-03-01", "2019-10-18"):
        url = ' https://zgdypw.cn/pors/w/webStatisticsDatas/api/%s/searchDayBoxOffice' % i
        r = session.request("GET", url).content
        raw_data = json.loads(r)
        if raw_data['status'] == 'success':
            data = raw_data["data"]
            print(data)
            connection.insert_one(data)

