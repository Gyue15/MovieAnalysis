import pymongo
import logging
import string
import requests
import time
from collections import deque
from urllib import parse
import random
from util import ProxyConf

User_Agents = [
    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
    'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11',
    'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11',
]


class MongoDBHelper:
    """数据库操作"""

    def __init__(self, collection_name=None):
        # 启动mongo
        self._client = pymongo.MongoClient('localhost', 27017)
        # 使用test数据库
        self._test = self._client['film']
        # 创建指定的集合
        self._name = self._test[collection_name]

    def insert_item(self, item):
        """插入数据"""
        self._name.insert_one(item)

    def find_item(self):
        """查询数据"""
        data = self._name.find()
        return data


class DoubanSpider(object):
    """豆瓣爬虫"""

    def __init__(self):
        # 基本的URL
        self.base_url = 'https://movie.douban.com/j/new_search_subjects?'
        self.full_url = self.base_url + '{query_params}'
        # 从User-Agents中选择一个User-Agent
        self.headers = {'User-Agent': random.choice(User_Agents)}
        self.form_tag = '电影'
        self.countries_tag = '中国'
        self.sort = 'T'  # 排序方式,默认是T,表示热度
        self.range = 0, 10  # 评分范围
        self.playable = ''
        self.unwatched = ''
        self.genres = ''
        # 连接数据库,集合名为douban_movies
        self.db = MongoDBHelper('movie_douban')

    def encode_query_data(self):
        """对输入信息进行编码处理"""
        query_param = {
            'sort': self.sort,
            'range': self.range,
            'countries': self.countries_tag,
            'tags': self.form_tag,
            'genres': self.genres
        }

        # string.printable:表示ASCII字符就不用编码了
        query_params = parse.urlencode(query_param, safe=string.printable)
        # 去除查询参数中无效的字符
        invalid_chars = ['(', ')', '[', ']', '+', '\'']
        for char in invalid_chars:
            if char in query_params:
                query_params = query_params.replace(char, '')
        # 把查询参数和base_url组合起来形成完整的url
        self.full_url = self.base_url + '{query_params}'
        self.full_url = self.full_url.format(query_params=query_params)
        print(self.genres)
        # print(self.full_url)

    def download_movies(self, offset):
        """下载电影信息
        :param offset: 控制一次请求的影视数量
        :return resp:请求得到的响应体"""
        full_url = self.full_url + '&start=' + str(offset)
        print(full_url)
        resp = None
        try:
            resp = requests.get(full_url, headers=self.headers)
        except Exception as e:
            # print(resp)
            logging.error(e)
        return resp

    def get_movies(self, resp):
        """获取电影信息
        :param resp: 响应体
        :return movies:爬取到的电影信息"""
        if resp:
            if resp.status_code == 200:
                # 获取响应文件中的电影数据
                movies = dict(resp.json()).get('data')
                if movies:
                    # 获取到电影了,
                    print(movies)
                    return movies
                else:
                    # 响应结果中没有电影了!
                    # print('已超出范围!')
                    return None
        else:
            # 没有获取到电影信息
            return None

    def save_movies(self, movies):
        """把请求到的电影保存到数据库中
        :param movies:提取到的电影信息
        :param id: 记录每部电影
        """
        if not movies:
            print('save_movies() error: movies为None!!!')
            return

        all_movies = self.find_movies()
        if len(all_movies) == 0:
            # 数据库中还没有数据,
            for movie in movies:
                # movie['_id'] = id
                self.db.insert_item(movie)
        else:
            # 保存已经存在数据库中的电影标题
            titles = []
            for existed_movie in all_movies:
                # 获取数据库中的电影标题
                titles.append(existed_movie.get('url'))

            for movie in movies:
                # 判断数据库中是否已经存在该电影了
                if movie.get('url') not in titles:
                    # 如果不存在,那就进行插入操作
                    self.db.insert_item(movie)
                    print('【save_movies():"{}"】'.format(movie.get('title')))
                else:
                    print('该电影"{}"已经在数据库了!!!'.format(movie.get('title')))

    def find_movies(self):
        """查询数据库中所有的电影数目
        :return: 返回数据库中所有的电影
        """
        all_movies = deque()
        data = self.db.find_item()
        for item in data:
            all_movies.append(item)
        return all_movies


def main():
    """豆瓣电影爬虫程序入口"""
    # 1. 初始化工作,设置请求头等
    spider = DoubanSpider()
    # 2. 与用户交互,获取用户输入的信息
    # spider.get_query_parameter()
    # ret = input('是否需要设置排序方式,评分范围...(Y/N):')
    # if ret.lower() == 'y':
    #     spider.get_default_query_parameter()
    # 3. 对信息进行编码处理,组合成有效的URL
    for t in ProxyConf.type_list:
        print("type: " + t)
        spider.genres = t
        spider.encode_query_data()
        offset = 0
        while True:
            # 4. 下载影视信息
            reps = spider.download_movies(offset)
            # 5.提取下载的信息
            movies = spider.get_movies(reps)
            if not movies:
                break
            # 6. 保存数据到MongoDB数据库
            spider.save_movies(movies)
            # print(movies)
            offset += 20
            if offset > 200:
                break
            # 控制访问速速
            time.sleep(float(random.randint(0, 30)) / 10)
            print("now offset: " + str(offset))


if __name__ == '__main__':
    main()
