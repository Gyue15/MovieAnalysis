import datetime
import re
import pymongo


class ProxyConf(object):
    type_list = ['恐怖',
                 '短片',
                 '冒险',
                 '爱情',
                 '同性',
                 '动画',
                 '犯罪',
                 '情色',
                 '鬼怪',
                 '喜剧',
                 '战争',
                 '剧情',
                 '运动',
                 '脱口秀',
                 '科幻',
                 '儿童',
                 '古装',
                 '奇幻',
                 '黑色电影', ]
    proxy_list = ['95.154.104.147:38930',
                  '189.14.193.242:53281',
                  '182.53.206.40:8080',
                  '181.191.180.110:8080',
                  '192.245.60.206:80',
                  '171.97.217.32:8213',
                  '210.12.194.39:8083',
                  '190.109.167.9:57608',
                  '203.196.32.61:45305',
                  '170.244.188.152:8080',
                  '186.194.120.72:8080',
                  '103.94.122.254:8080',
                  '168.195.229.41:9999',
                  '154.7.5.228:3129',
                  '159.192.97.181:23500',
                  '24.113.73.230:48678',
                  '183.164.238.79:9999',
                  '104.168.234.212:3128',
                  '103.36.35.145:8080',
                  '183.164.238.64:9999',
                  '103.111.31.210:80',
                  '118.31.67.240:3128',
                  '5.196.132.115:3128',
                  '36.89.191.151:38604',
                  '181.129.66.138:55679',
                  '186.23.31.186:3128',
                  '27.145.100.243:8080',
                  '91.211.107.204:41258',
                  '77.37.142.203:53281',
                  '203.177.133.148:47153',
                  '70.165.65.233:48678',
                  '182.101.207.11:8080',
                  '35.198.14.76:80',
                  '188.168.96.34:34298',
                  '103.9.188.236:30274',
                  '124.158.173.133:8080',
                  '160.119.128.62:8080',
                  '134.35.203.177:8080',
                  '187.188.182.107:58921',
                  '170.81.141.116:53281',
                  '182.52.51.5:49878',
                  '182.35.82.16:9999',
                  '83.175.166.234:8080',
                  '103.94.0.82:49241',
                  '190.221.181.10:33078',
                  '109.196.15.142:49133',
                  '193.238.98.206:53202',
                  '31.220.183.217:53281',
                  '202.169.226.230:8080',
                  '95.165.163.188:60103',
                  '45.237.182.98:8080',
                  '36.80.205.115:8000',
                  '46.225.243.65:8080',
                  '27.145.233.17:8213',
                  '154.7.5.225:3129',
                  '194.213.217.254:8080',
                  '46.34.161.100:80',
                  '31.28.23.224:8080',
                  '43.254.126.31:8080',
                  '36.67.237.147:3128',
                  '36.68.26.158:8080',
                  '49.156.35.22:8080',
                  '103.240.160.21:6666',
                  '175.100.185.151:53281',
                  '158.58.133.13:21213',
                  '191.102.70.82:9991',
                  '45.70.192.180:8080',
                  '118.127.99.93:53281',
                  '45.160.7.79:8081',
                  '190.61.55.186:9991',
                  '183.89.112.72:8080',
                  '1.9.216.226:36051',
                  '103.101.233.13:8080',
                  '123.207.218.215:1080',
                  '27.150.86.72:9999',
                  '193.176.212.195:8080',
                  '36.255.87.238:84',
                  '125.99.120.166:40390',
                  '152.232.210.96:8080',
                  '117.208.148.72:3128',
                  '167.71.222.141:3128',
                  '103.250.153.242:31382',
                  '1.0.139.172:8080',
                  '77.48.23.71:38813',
                  '195.214.222.75:8080',
                  '190.93.176.70:8080',
                  '160.19.245.61:58349',
                  '139.255.89.242:8080',
                  '178.205.254.106:8080',
                  '49.248.17.94:8080',
                  '176.120.211.176:41872',
                  '41.203.75.90:8080',
                  '118.31.74.108:3128',
                  '194.44.160.246:61465',
                  '186.216.81.21:31773',
                  '41.73.15.134:80',
                  '43.229.73.235:59648',
                  '51.158.119.88:8811',
                  '191.238.217.84:80',
                  '179.127.140.188:20183',
                  '190.60.103.178:8080',
                  '203.176.181.137:3128',
                  '183.88.238.243:8080',
                  '101.255.64.246:32663',
                  '71.183.100.76:42413',
                  '51.89.228.17:8080',
                  '36.89.240.9:8080',
                  '103.241.227.106:44825',
                  '1.10.188.42:48721',
                  '14.207.15.218:8213',
                  '59.108.125.241:8080',
                  '47.102.211.203:8118',
                  '110.164.197.99:8080',
                  '157.245.60.143:8080',
                  '45.112.57.230:61222',
                  '124.41.243.22:47894',
                  '202.5.56.71:8080',
                  '91.92.80.25:40487',
                  '89.111.105.99:41258',
                  '180.232.77.107:37686',
                  '200.216.115.10:8080',
                  '154.7.9.213:3129',
                  '201.217.55.97:8080',
                  '151.248.63.153:8080',
                  '119.82.252.1:30853',
                  '79.141.163.80:3128',
                  '170.80.156.43:8080',
                  '202.57.2.19:8080',
                  '182.35.81.184:9999',
                  '37.187.149.129:1080',
                  '190.202.22.129:3128',
                  '85.30.215.48:32946',
                  '187.16.109.209:8888',
                  '134.249.156.3:82',
                  '181.112.42.38:38264',
                  '136.243.47.220:3128',
                  '110.172.135.234:40238',
                  '188.227.195.68:8080',
                  '200.124.243.59:8080',
                  '27.109.116.106:55667',
                  '194.183.168.129:31385',
                  '190.242.98.61:8083',
                  '138.121.155.127:61932',
                  '110.5.102.238:8082',
                  '70.98.53.118:8080',
                  '118.97.46.250:39129',
                  '178.128.62.211:8080',
                  '47.96.180.209:3128',
                  '188.6.164.138:55042',
                  '195.175.209.194:8080',
                  '198.229.231.13:8080',
                  '203.189.137.107:8080',
                  '128.199.254.103:23352',
                  '47.95.249.140:8118',
                  '103.250.158.21:48484',
                  '14.232.245.83:8080',
                  '103.113.83.5:3128',
                  '151.80.199.89:3128',
                  '1.20.217.221:8080',
                  '187.19.165.167:8080',
                  '217.15.143.239:8080',
                  '110.137.15.192:8080',
                  '43.225.185.146:8080',
                  '154.7.9.164:3129',
                  '119.82.245.250:57463',
                  '2.186.247.128:8080',
                  '171.13.102.147:9999',
                  '193.106.57.37:40594',
                  '180.122.149.60:9999',
                  '124.158.167.242:8080',
                  '51.77.105.45:8080',
                  '36.92.10.95:3128',
                  '103.216.51.203:9991',
                  '213.145.137.102:39364',
                  '36.90.98.202:8080',
                  '138.97.200.59:8080',
                  '36.89.190.85:8080',
                  '149.28.245.254:8080',
                  '201.67.41.214:39849',
                  '91.191.3.114:53281',
                  '186.86.247.169:39168',
                  '182.35.82.234:9999',
                  '134.35.197.161:8080',
                  '183.89.73.93:8213',
                  '189.39.127.118:8080',
                  '79.11.197.125:8080',
                  '103.61.37.179:3128',
                  '190.122.97.42:3128',
                  '130.180.28.106:42036',
                  '203.143.7.201:8080',
                  '202.84.127.1:3128',
                  '119.18.147.111:8080',
                  '62.205.169.74:53281',
                  '27.152.91.39:9999',
                  '168.0.140.183:8080',
                  '106.122.170.4:8118',
                  '212.42.113.240:36910',
                  '81.190.208.52:34418',
                  '128.201.97.158:53281',
                  '203.113.103.54:8080',
                  '180.183.66.64:8080',
                  '192.249.53.99:8080',
                  '181.143.226.252:999',
                  '117.57.90.194:9999',
                  '36.25.40.126:9999',
                  '160.119.128.66:8080',
                  '203.80.170.120:8080',
                  '117.200.48.90:8080',
                  '185.18.64.106:53281',
                  '189.198.224.1:80',
                  '157.52.135.141:3129',
                  '157.52.230.123:3129',
                  '95.79.55.196:53281',
                  '111.92.243.154:32489',
                  '176.30.212.193:8080',
                  '200.89.178.195:8080',
                  '185.72.203.244:8080',
                  '157.119.207.35:8080',
                  '181.129.42.179:35690',
                  '146.196.108.202:80',
                  '14.207.127.27:8080',
                  '118.174.46.162:8080',
                  '163.53.206.145:8080',
                  '49.48.148.45:8080',
                  '36.91.88.166:8080',
                  '149.28.225.26:8080',
                  '89.249.254.48:8080',
                  '191.96.27.81:3129',
                  '118.174.232.243:8080',
                  '91.189.177.186:3128',
                  '36.37.114.40:41454',
                  '103.94.4.99:8080',
                  '119.42.67.171:8080',
                  '103.57.36.223:8080',
                  '182.35.83.110:9999',
                  '37.187.149.234:8080',
                  '45.77.200.34:3128',
                  '27.147.249.202:56105',
                  '200.195.148.18:3128',
                  '41.65.181.132:8080',
                  '129.205.201.133:8080',
                  '91.217.42.4:8080',
                  '159.89.195.233:80',
                  '14.207.168.133:8213',
                  '218.64.69.79:8080',
                  '5.196.132.118:3128',
                  '62.210.203.105:3128',
                  '187.1.174.94:20183',
                  '201.75.80.80:53281',
                  '125.26.99.244:33510',
                  '167.172.64.139:8080',
                  '181.113.135.254:52058',
                  '117.121.207.249:31281',
                  '173.197.169.6:8080',
                  '182.35.82.37:9999',
                  '49.204.70.139:80',
                  '109.200.175.167:8080',
                  '3.0.61.137:8118',
                  '45.5.94.150:35258',
                  '134.35.11.135:8080',
                  '45.236.104.86:8080',
                  '85.187.247.58:53281',
                  '46.20.150.226:8080',
                  '103.85.16.54:33046',
                  '157.52.230.35:3129',
                  '77.252.26.71:48622',
                  '154.73.108.49:53281',
                  '89.250.221.106:53281',
                  '36.89.228.201:56810',
                  '178.32.80.239:1080',
                  '154.7.5.189:3129',
                  '187.32.222.61:3128',
                  '27.147.180.93:8080',
                  '202.166.202.29:58794',
                  '62.162.228.66:32626',
                  '212.154.83.107:35116',
                  '202.62.39.134:8080',
                  '190.61.41.162:8080',
                  '125.27.251.24:36048',
                  '85.207.44.10:53038',
                  '119.179.180.6:8060',
                  '106.14.206.26:8118',
                  '95.65.73.200:30612',
                  '103.80.83.161:8080',
                  '89.250.17.209:8080',
                  '93.76.211.56:42818',
                  '216.189.145.240:80',
                  '178.46.159.197:3128',
                  '177.12.80.50:50556',
                  '74.83.246.125:8081',
                  '5.196.132.123:3128',
                  '203.81.91.80:8080',
                  '101.109.143.71:36127',
                  '178.128.50.159:8080',
                  '182.19.66.196:3128',
                  '103.42.162.30:8080',
                  '1.1.221.75:8080',
                  '103.87.251.146:23500',
                  '103.22.173.230:8080',
                  '5.23.103.98:33027']


class RemoveWord(object):
    word_list = ['全面反击', '大电影', '剧场', '原', '三国', '英文', '神农溪恋', '鬼吹灯', '杨贵妃', '内战', '冬日战士', '西游记',
                 '意外旅程', '意外旅', '电影', '于离别朝束起约定花', '劫后重生', '山口山战记', '兔年顶呱呱', '博人传', '博人',
                 '宗师传奇', '龙腾万里', '一日成才', '赛尔号']
    word_dict = {
        '妳': '你',
        '美人鱼公主之海盗来袭': '美人鱼之海盗来袭',
        '解剖室灵异事件之男生宿舍': '男生宿舍',
        '我是狼之火龙山大冒险': '我是狼',
        '2012\\(3D版\\)': '2012世界末日',
        '万万没想到.*': '万万没想到：西游篇',
        '三变 山变': '三变促山变',
        '赛德克·巴莱 内地版': '赛德克・巴莱',
        '神奇马戏团.*': '神奇马戏团之动物饼干',
        '大野狼与小绵羊的爱情': '大野狼和小绵羊的爱情',
        '忍者神龟2.*': '忍者神龟2：破影而出',
        "一次离别": "一次别离",
        "潜艇总动员3.*": "潜艇总动员3：彩虹宝藏",
        '移动迷宫2': '移动迷宫：烧痕审判',
        '夺宝幸运星大电影之金箍棒传奇': '金箍棒传奇',
        '谁是卧底之王牌': '王牌',
        '四大名捕大结局': '四大名捕3',
        '铁血残阳：在刺刀和藩篱下': '在刺刀和藩篱下',
        '爸爸我来救你了之吉大嘿梦': '爸爸我来救你了',
        '有5个姐姐的我就注定要单身了啊': '有五个姐姐的我就注定要单身了啊',
        '快乐大本营之快乐到家': '快乐到家',
        '十二生肖之福星高照朱小八': '福星高照朱小八',
        '锋味江湖之决战食神': '决战食神',
        '甜蜜18岁': '甜蜜十八岁',
        '守护者：世纪战元': '世纪战元',
        '我叫哀木涕之山口山战记': '我叫MT之山口山战记',
        '泰国妖医': '妖医',
        '一颗心中的许愿树': '一棵心中的许愿树',
        '霍比特人3：去而复归': '霍比特人3：五军之战',
        '酒国英雄之摆渡人': '摆渡人',
        '真心话大冒险': '真心话太冒险',
        '水族秀': '水族魂1944',
        '江南灵异录之白云桥': '白云桥',
        '星球大战：侠盗一号': '星球大战外传：侠盗一号',
        '七月半之恐怖宿舍': '七月半',
        '巴霍巴利王\\(上\\)': '巴霍巴利王：开端',
        '新大头儿子小头爸爸2一日成才': '新大头儿子和小头爸爸2一日成才',
        '遊戏规则': '游戏规则',
        '魁拔3战神崛起': '魁拔Ⅲ战神崛起',
        '前任攻略2：备胎反击战': '前任2：备胎反击战',
        '异星战场：约翰・卡特传奇': '异星战场',
        '托马斯和朋友们：多多岛之迷失': '托马斯和朋友们多多岛之迷失宝藏',
        '20123D版\\)': '2012世界末日',
        '阿里巴巴：大盗奇兵': '阿里巴巴大盗奇兵',
        '太平轮下': '太平轮：彼岸',
        '哆啦A梦：大雄的南极冰寒大冒险': '哆啦A梦：大雄的南极冰冰凉大冒险',
        '敦刻尔克大撤退': '敦刻尔克',
        '冰封：重生之门': '重生之门',
        '寄生兽真人版': '寄生兽',
        '白幽灵传奇之绝命逃亡': '绝命逃亡',
        '古曼怨灵': '古曼',
        '最长1枪': '最长的一枪',
        '胡桃夹子：魔境冒险': '胡桃夹子',
        '乔布斯传': '乔布斯',
        '奥特曼：超决战！贝利亚尔银河帝国': '超决战！贝利亚银河帝国',
        '枕边诡影\\\t': '枕边诡影',
        '极限挑战之皇家宝藏': '极限挑战',
        '亲，别怕': '亲，别怕鬼宅凶灵',
        '太平轮\\(下\\)': '太平轮：彼岸',
        '隐隐惊马槽之决战女僵尸': '隐隐惊马槽',
        '刘老庄八十二烈士': '刘老庄八十二壮士',
        '逆生·致亲爱的小孩': '逆生',
        '犀利人妻最终回：幸福男·不難': '犀利人妻',
        '冰川时代4：大陆漂移': '冰川时代4',
        '火魔高跟鞋国语2D\\)': '火魔高跟鞋',
        '\\(.*?\\)': '',
        '（.*?）': ''
    }


def find_film(film_name, film_dict):
    for f in film_dict:
        if str_clear(trans_name(f["movieName"])) == str_clear(film_name):
            return f
    return False


def trans_name(film_name):
    for k, v in RemoveWord.word_dict.items():
        film_name = re.sub(k, v, film_name)
    return film_name


def str_clear(text, new_sign='。·・！：1234567890， abcdefghijklmnopqrstuvwxyzⅡ之-“”Ⅰ？版的ⅱ——'):
    text = text.lower()
    import string  # 引入string模块
    sign_text = string.punctuation + new_sign  # 引入英文符号常量，可附加自定义字符，默认为空
    sign_repl = '@' * len(sign_text)  # 引入符号列表长度的替换字符
    sign_table = str.maketrans(sign_text, sign_repl)  # 生成替换字符表
    res = text.translate(sign_table).replace('@', '')
    res = re.sub('守护者：世纪战元', '世纪战元', res)
    for word in RemoveWord.word_list:
        res = re.sub(word, '', res)
    return res


def get_date_str(begin_date, end_date):
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    date_list_str = []
    while begin_date <= end_date:
        date_string = begin_date.strftime("%Y-%m-%d")
        date_list_str.append(date_string)
        begin_date += datetime.timedelta(days=1)
    return date_list_str


def trans_date_str(date_str):
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    return date.strftime("%Y-%m-%d")


def trans_percent_num(number):
    return round(float(number.strip("%")) / 100, 4)


def trans_num(number):
    unit_list = ["千", "万", "亿"]
    unit_dict = {
        "1": 1,
        "千": 1000,
        "万": 10000,
        "亿": 100000000
    }
    unit = number[-1]
    if unit in unit_list:
        number = number[:-1]
        number = float(number) * unit_dict[unit]
    return int(number)


if __name__ == '__main__':
    client = pymongo.MongoClient('localhost', 27017)
    database = client['film']
    collection = database['douban_movies']
    des = database['movie_douban']
    movie = []
    for m in collection.find({}):
        movie.append({
            'title': m['title'],
            'directors': m['directors'],
            'casts': m['casts'],
            'rate': m['rate'],
            'url': m['url']
        })
    des.insert_many(movie)
