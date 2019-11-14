from requests_html import HTMLSession
from spider_douban import User_Agents
import random
import pymongo
import time
import logging
from util import ProxyConf

session = HTMLSession()
client = pymongo.MongoClient('localhost', 27017)
database = client['film']
collection = database['movie_with_rate']

proxy = '36.69.23.233:8080'
use_proxy = True
need_sleep = False
allow_repeat_time = 10

start = 0


def download(movie_list: list):
    global start
    proxies = {'http': 'http://%s' % proxy, 'https': 'http://%s' % proxy}
    repeat_time = 0
    s = start
    for i in range(s, len(movie_list)):
        movie: dict = movie_list[start]
        start += 1
        url = movie['url']
        if use_proxy:
            html = session.request("GET", url, headers={'User-Agent': random.choice(User_Agents)}, proxies=proxies, verify=False).html
        else:
            html = session.request("GET", url, headers={'User-Agent': random.choice(User_Agents)}).html
        # print(html)
        editors = html.xpath('//*[@id="info"]/span[2]/span[2]/a/text()')
        types = html.xpath('//*[@id="info"]/span[@property="v:genre"]/text()')
        directors = html.xpath('//*[@id="info"]/span[1]/span[2]/a[@rel="v:directedBy"]/text()')
        casts = html.xpath('//*[@id="info"]/span[3]/span[2]/a[@rel="v:starring"]/text()')
        movie['editors'] = editors if len(editors) > len(movie['editors']) else movie['editors']
        movie['types'] = types if len(types) > len(movie['types']) else movie['types']
        if len(directors) > len(movie['directors']):
            movie['directors'] = directors
            print('change directors!')
        if len(casts) > len(movie['casts']):
            movie['casts'] = casts
            print('change casts!')
        if html.url != movie['url']:
            i -= 1
            start -= 1
            repeat_time += 1
            if repeat_time > allow_repeat_time:
                raise Exception
            print(html)
            print("miss!!" + str(movie))
            time.sleep(float(random.randint(0, 30)) / 10)
            continue
        repeat_time = 0
        print(str(i) + " : " + str(movie))
        collection.update_one({'url': movie['url']}, {"$set": {
            'directors': movie['directors'],
            'types': movie['types'],
            'editors': movie['editors'],
            'casts': movie['casts']
        }})
        if need_sleep:
            time.sleep(float(random.randint(0, 15)) / 10)


if __name__ == '__main__':
    movies = []
    for m in collection.find({}).sort([('url', -1)]):
        movies.append(m)
    index = 0
    need_sleep = True
    use_proxy = False
    start = 0

    while True:
        try:
            download(movies)
            if len(movies) == start:
                break
        except Exception as e:
            logging.error(e)
            # need_sleep = False
            # use_proxy = True
            proxy = ProxyConf.proxy_list[index]
            print("now proxy: " + proxy)
            index += 1
            if index >= len(ProxyConf.proxy_list):
                index = 0
