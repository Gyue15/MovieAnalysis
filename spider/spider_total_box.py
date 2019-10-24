from requests_html import HTMLSession
import re
import pymongo
from util import *


session = HTMLSession()
client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
connection = database["film_total_box"]
for i in range(476):
    url = "http://58921.com/content/type/film?page=%s" % str(i)
    r = session.get(url)
    film_cards = r.html.xpath('//*[@id="content"]/div[2]/div/div')
    for film_card in film_cards:
        box = film_card.xpath('div/div/p/span[2]')
        if len(box) > 0:
            unit_str = box[0].text[-1]
            box = ''.join(re.findall('[\d+.\d]*', box[0].text))
            if len(box) > 0:
                name = str(film_card.xpath('div/div/h4/a/@title')[0])
                total_box = trans_num(box + unit_str)
                data = {"name": name, "total_box": total_box}
                print(data)
                connection.insert_one(data)
