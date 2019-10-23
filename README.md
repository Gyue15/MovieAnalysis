##Streaming目标

* 从电影票房的变化观察人们的经济水平变化
* 从热门电影类型的变化观察人们口味的变化
* 从电影线上购票与线下购票的比例变化观察互联网普及程度的变化、以及分析爆款电影的观影人群结构



##Spark streaming 生成的图

  每一秒变化都是一个月的数据

###1.热门电影线上与线下票房统计图

* 说明：柱状图。横坐标是电影名称；纵坐标是票房；同时统计总票房与网票（线上）票房，用两根重叠但不同颜色的柱子表示；统计从开始到现在的总票房；统计图随时间变化。
* 意义：分析不同热门电影的观影人群结构

### 2.不同电影类型线上与线下票房统计图

* 说明：柱状图。横坐标是电影类型；其他同上。
* 意义：分析不同类型的观影人群结构

###3.电影大盘线上与线下总票房统计图

* 说明：折线图。横坐标是时间（月）；纵坐标是当月票房
* 意义：分析电影票房的变化趋势

###4.不同产地的电影票房统计图

* 说明：折线图。横坐标是时间；纵坐标是占比；不同的线表示不同的国家。
* 意义：想看到的不同国家电影的市场占有率

###5.中国省份票房地图

* 说明：地图。不同省份用不同颜色表示；颜色越深表示票房越高；随时间变化
* 意义：想看到不同省的票房变化情况

###6.演员票房统计图

* 说明：横坐标是演员，纵坐标是票房。
* 意义：想看到的是有票房号召力的是哪些演员

---

## 输出数据格式：

* collection: total_box
  {
  “time”: 日期(string yyyy-MM)
  "total_month_box":总票房，当月(long),
  "online_month_box":线上票房，当月(long),
  }

* collection: film_box
  {
  "name": 电影名称(string),
  "total_box":总票房(long),
  "online_box":线上票房(long),
  }

* collection: type_box
  {
  “time”: 日期(string, yyyy-MM)
  "type": 电影类型(string),
  "total_month_box":总票房，当月(long),
  "online_month_box":线上票房，当月(long),
  }

* collection: location_box
  {
  “time”: 日期(string, yyyy-MM)
  "location": 电影产地(string),
  "total_month_box":总票房，当月(long),
  "online_month_box":线上票房，当月(long),
  }

* collection: actor_box
  {
  “time”: 日期(string, yyyy)
  "actor": 演员名称(string),
  "total_year_box":总票房，当年(long),
  "online_year_box":线上票房，当年(long),
  }

* collection: province_box
  {
  “time”: 日期(string, yyyy-MM)
  "province": 省份名称(string),
  "total_month_box":总票房，当月(long),
  "online_month_box":线上票房，当月(long),
  }

## 输入数据格式：

* collection: film_stream
  {
  "time": (string, yyyy-MM-dd),
  "movie_name": (string),
  "total_box": (long),
  "online_box": (long),
  "location": (string),
  "actors": [(string), ...],
  "type": [(string), ...],
  }

* collection: area_stream
  {
  "time": (string, yyyy-MM-dd),
  "province_id": (int)
  "province": (string),
  "box": (int)
  }


