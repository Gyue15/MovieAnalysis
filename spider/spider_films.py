from requests_html import HTMLSession
import pymongo
import json
from util import *


if __name__ == "__main__":
    session = HTMLSession()
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    database = client["film"]
    connection = database["film_detail_data"]
    for i in get_date_str("2011-01-01", "2019-10-18"):
        url1 = 'http://www.films.cn/api/top/movie/boxoffice?boxofficeTime=%s&movieType=all&topType=day&size=100' % i
        r1 = session.request("GET", url1).content
        raw_data1 = json.loads(r1)
        data_movie = []
        # print(raw_data1)
        for data in raw_data1['data']:
            data_movie.append({
                'rank': data['rank'],
                'id': data['id'],
                'movieName': data['movieName'],
                'boxOfficeIndexNum': data['boxOfficeIndexNum'],
                'popShowIndexNum': data['popShowIndexNum'],
                'showDays': int(data['showDays']),
                'boxOffice': data['boxofficeNum'],
                'totalBoxOffice': data['totalBoxofficeNum'],
                'boxOfficeRate':  trans_percent_num(data['boxOfficeRate']),
                'scheduleRate': trans_percent_num(data['scheduleRate']),
                'ticketSeatRate': trans_percent_num(data['ticketSeatRate']),
                'avgPerson': int(data['avgPerson']),
                'seat': data['seat'],
                'showCount': data['showCount'],
                'person': data['person'],
                'tickes': data['tickes'],
                'releaseDay': int(data['releaseDay']) if data['releaseDay'] != '点映' else 0
            })
        print(data_movie)

        url2 = 'http://www.films.cn/api/aggs/general/boxoffice/summary?boxofficeTime=%s&rankType=day' % i
        r2 = session.request("GET", url2).content
        raw_data2 = json.loads(r2)
        data_total = raw_data2['data']
        print(data_total)

        url3 = 'http://www.films.cn/api/aggs/general/moviearea/distribute?date=%s&rankType=day' % i
        r3 = session.request("GET", url3).content
        raw_data3 = json.loads(r3)
        data_location = raw_data3['data']
        print(data_location)

        url4 = 'http://www.films.cn/api/aggs/general/movietype/distribute?date=%s&rankType=day' % i
        r4 = session.request("GET", url4).content
        raw_data4 = json.loads(r4)
        data_tag = raw_data4['data']
        print(data_tag)

        print("=======")

        connection.insert_one({
            "data_total": data_total,
            "data_movie": data_movie,
            "data_location": data_location,
            "data_tag": data_tag
        })
