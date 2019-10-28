from requests_html import HTMLSession
import datetime
import pymongo

class MovieSpider(object):
    def __init__(self):
        self.li_list = ['导演：', '主演：', '片长：', '类型：', '制作国家/地区：']
        self.li_func_dict = {
            '导演：': MovieSpider.get_director,
            '主演：': MovieSpider.get_actors,
            '片长：': MovieSpider.get_duration,
            '类型：': MovieSpider.get_tag,
            '制作国家/地区：': MovieSpider.get_location
        }
        self.li_key_dict = {
            '导演：': 'director',
            '主演：': 'actors',
            '片长：': 'duration',
            '类型：': 'tag',
            '制作国家/地区：': 'location'
        }
        self.unit_list = ["千", "万", "亿"]
        self.unit_dict = {
            "千": 1000,
            "万": 10000,
            "亿": 100000000
        }
        self.client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.database = self.client["film"]
        self.connection = self.database["film_per_day"]
        self.film_info_dict = {}

    @staticmethod
    def get_date_str(begin_date, end_date):
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        date_list_str = []
        while begin_date <= end_date:
            date_string = begin_date.strftime("%Y%m%d")
            date_list_str.append(date_string)
            begin_date += datetime.timedelta(days=1)
        return date_list_str

    def trans_num(self, number):
        unit = number[-1]
        if unit in self.unit_list:
            number = number[:-1]
            number = float(number) * self.unit_dict[unit]
        return int(number)

    @staticmethod
    def get_location(parent):
        return str(parent.xpath('li/a/@title')[0])

    @staticmethod
    def get_tag(parent):
        tags = []
        for t in parent.xpath('li/a/@title'):
            tags.append(str(t))
        return tags

    @staticmethod
    def get_duration(parent):
        duration = str(parent.text)
        duration = "".join(list(filter(str.isdigit, duration)))
        return int(duration) if len(duration) > 0 else False

    @staticmethod
    def get_actors(parent):
        actors = []
        for a in parent:
            act_name = str(a)
            act_name = act_name.replace('．', '·')
            actors.append(act_name)
        # 去重
        if len(actors) > 0:
            last = actors[-1]
            for act_name in actors[:-1]:
                if act_name in last:
                    actors = actors[:-1]
                    break
        actors = list(set(actors))
        return actors

    @staticmethod
    def get_director(parent):
        return str(parent.xpath('li/a/@title')[0])

    def get_detail(self, sub_url):
        full_url = "http://58921.com%s" % sub_url
        film_index = sub_url[6:]
        if film_index in self.film_info_dict:
            return self.film_info_dict[film_index]

        detail_html = session.get(full_url).html

        info = {}
        lu = detail_html.xpath('//*[@id="content_view_%s"]/div[2]/div/ul/li' % film_index)
        for li in lu:
            key = li.xpath('li/strong/text()')
            if len(key) > 0:
                key = str(key[0])
            else:
                continue
            if key in self.li_list:
                value = self.li_func_dict[key](li)
                if value:
                    info[self.li_key_dict[key]] = value
        self.film_info_dict[film_index] = info
        return info


if __name__ == "__main__":
    date_list = MovieSpider.get_date_str("2011-11-12", "2019-10-18")
    print(date_list)
    session = HTMLSession()
    spider = MovieSpider()

    # spider.get_detail("/film/30")
    # ss = '约瑟夫．高登-莱维特布鲁斯．威利斯艾米莉．布朗特'
    # ss = ss.replace('．', '·')
    # print(ss)

    for date_str in date_list:
        r = session.get("http://58921.com/boxoffice/wangpiao/%s" % date_str)
        data = {"time": date_str, "film_detail": []}
        table = r.html.xpath('//*[@id="content"]/div/div[2]/table/tbody/tr')
        for item in table:
            url = item.xpath('tr/td[1]/a/@href')
            url = url[0][:-10] if len(url) > 0 else -1

            name = item.xpath('tr/td[1]/a/@title')
            name = name[0] if len(name) > 0 else -1

            box = item.xpath('tr/td[2]/a/text()')
            box = box[0] if len(box) > 0 else -1

            waste = item.xpath('tr/td[5]/text()')
            waste = waste[0] if len(waste) > 0 else -1

            man_time = item.xpath('tr/td[6]/a/text()')
            man_time = man_time[0] if len(man_time) > 0 else -1

            if -1 in [url, name, box, waste, man_time]:
                continue

            film_info = {"name": str(name), "total_box": spider.trans_num(str(box)), "waste": int(str(waste)),
                         "man_time": int(str(man_time))}

            film_info.update(spider.get_detail(url))

            data["film_detail"].append(film_info)

        print(data)
        spider.connection.insert_one(data)
