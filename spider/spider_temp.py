from selenium import webdriver
import time
import random
import pytesseract
from PIL import Image
from selenium.webdriver.common.action_chains import ActionChains


class Spider(object):

    def __init__(self):
        self.wd = webdriver.Chrome()
        self.max_count = 10000000
        self.unit_list = ["千", "万", "亿"]
        self.unit_dict = {
            "1": 1,
            "千": 1000,
            "万": 10000,
            "亿": 100000000
        }

    def login(self):
        self.wd.get("https://maoyan.com/")
        # 登录
        self.wd.find_element_by_class_name("user-avatar").click()
        self.wd.find_element_by_id("login-email").send_keys("15951925955")
        self.wd.find_element_by_id("login-password").send_keys("960917wxy")
        self.wd.find_element_by_name("commit").click()
        time.sleep(1)

    def start_crawl(self):
        for i in range(1, 2):
            # sleep_time = random.randint(0, 2) + random.random()
            self.wd.get("https://maoyan.com/films/503342")

            self.wd.find_element_by_class_name('normal-score').find_element_by_class_name(
                'info-num').find_element_by_class_name("stonefont").screenshot("star.png")
            img_star = Image.open("star.png")
            star = pytesseract.image_to_string(img_star, lang='eng'
                                               , config='--psm 13 --oem 3 -c tessedit_char_whitelist=.0123456789')

            self.wd.find_element_by_class_name('normal-score').find_element_by_class_name(
                'score-num').find_element_by_class_name("stonefont").screenshot("cnt.png")
            img_cnt = Image.open("cnt.png")
            cnt = pytesseract.image_to_string(img_cnt, lang='eng'
                                              , config='--psm 13 --oem 3 -c tessedit_char_whitelist=.0123456789')

            count_unit = self.wd.find_element_by_class_name('normal-score').find_element_by_class_name(
                'score-num').find_element_by_class_name("stonefont").text[-1]
            if count_unit not in self.unit_list:
                count_unit = "1"

            self.wd.find_element_by_class_name('box').find_element_by_class_name("stonefont").screenshot("box.png")
            img_box = Image.open("box.png")
            box = pytesseract.image_to_string(img_box, lang='eng'
                                              , config='--psm 13 --oem 3 -c tessedit_char_whitelist=.0123456789')

            box_unit = self.wd.find_element_by_class_name('box').find_element_by_class_name("unit").text
            # 随机睡一段时间
            # time.sleep(sleep_time)

            print("得分为：" + star)
            print(cnt)
            print(box)
            print(str(float(cnt) * self.unit_dict[count_unit]) + "人评分")
            print("票房： " + str(float(box) * self.unit_dict[box_unit]))


if __name__ == '__main__':
    maoyan = Spider()
    maoyan.login()
    maoyan.start_crawl()
    # maoyan.font = TTFont('./fonts/07a6e8c2fb096ce97e4ec6c5ea33716f2276.woff')
    # string = repr('\uf804.\uf308')
    # print(maoyan.modify_data(string))
