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
collection = database['movie_douban']
miss = []

destination = database['movie_with_rate']

proxy = '36.69.23.233:8080'
use_proxy = True
need_sleep = False
allow_repeat_time = 5000


def get_movies():
    url_set = set()
    for m in destination.find({}):
        url_set.add(m['url'])
    movie_list = []
    for m in collection.find({}):
        if m['url'] in url_set:
            # print('already has')
            continue
        if m['rate'] != '' and m['url'] != '':
            movie_list.append({
                'title': m['title'],
                'directors': m['directors'],
                'casts': m['casts'],
                'rate': m['rate'],
                'url': m['url']
            })

    print(len(movie_list))
    return movie_list


def download(movie_list: list):
    proxies = {'http': 'http://%s' % proxy, 'https': 'http://%s' % proxy}
    repeat_time = 0
    for m in movie_list:
        if m in miss:
            continue
        url = m['url']
        find = destination.find_one({'url': url})
        if find:
            print('already has')
            continue
        if use_proxy:
            html = session.request("GET", url, headers={'User-Agent': random.choice(User_Agents)}, proxies=proxies, verify=False).html
        else:
            html = session.request("GET", url, headers={'User-Agent': random.choice(User_Agents)}).html
        # print(html)
        editors = html.xpath('//*[@id="info"]/span[2]/span[2]/a/text()')
        types = html.xpath('//*[@id="info"]/span[@property="v:genre"]/text()')
        directors = html.xpath('//*[@id="info"]/span[1]/span[2]/a[@rel="v:directedBy"]/text()')
        casts = html.xpath('//*[@id="info"]/span[3]/span[2]/a[@rel="v:starring"]/text()')
        m['editors'] = editors
        m['types'] = types
        m['directors'] = directors if len(directors) > len(m['directors']) else m['directors']
        m['casts'] = casts if len(casts) > len(m['casts']) else m['casts']
        if html.url != m['url']:
            repeat_time += 1
            if repeat_time > allow_repeat_time:
                raise Exception
            print(html)
            print("miss!!" + str(m))
            miss.append(m)
            time.sleep(float(random.randint(0, 30)) / 10)
            continue
        repeat_time = 0
        print(m)
        destination.insert_one(m)
        movie_list.remove(m)
        if need_sleep:
            time.sleep(float(random.randint(0, 15)) / 10)


if __name__ == '__main__':
    index = 0
    m_list = get_movies()
    need_sleep = True
    use_proxy = False
    while True:
        try:
            download(m_list)
            if len(m_list) == 0:
                break
        except Exception as e:
            # need_sleep = False
            # use_proxy = True
            logging.error(e)
            proxy = ProxyConf.proxy_list[index]
            print("now proxy: " + proxy)
            index += 1
            if index >= len(ProxyConf.proxy_list):
                index = 0
