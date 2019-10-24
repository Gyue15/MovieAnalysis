import datetime
import re


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


print(len(trans_name("枕边诡影\t")))
