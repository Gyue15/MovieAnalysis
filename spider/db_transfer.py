import pymongo
from util import *


# print(str_clear(trans_name("Hello，树先生")))
# print(str_clear(trans_name("Hello！树先生")))

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
total_box = database["film_detail_data"]
online_box = database['film_per_day']

film_stream = database["film_stream"]

miss_film = database["film_miss"]

film_box_dict = {}
miss_count = 0
accept_count = 0
for films_per_day in online_box.find({}):
    date_str = trans_date_str(films_per_day["time"])
    films_total_box = total_box.find_one({'data_total.time': date_str})

    films_total_box = films_total_box["data_movie"]
    for o_film in films_per_day["film_detail"]:
        name = trans_name(o_film["name"])
        t_film = find_film(name, films_total_box)
        if not t_film:
            miss_data = {"name": name, "time": date_str}
            print("miss: " + str(miss_data))
            miss_film.insert_one({"name": o_film["name"], "time": date_str, "online_box": o_film["total_box"]})
            miss_count += 1
            t_box = float("nan")
        else:
            t_box = t_film["boxOffice"]
        o_box = o_film['total_box'] if name not in film_box_dict else o_film['total_box'] - film_box_dict[name]
        film_box_dict[name] = o_film['total_box']
        data = {
            "time": date_str,
            "movie_name": name,
            "total_box": t_box,
            "online_box": o_box,
            "location": o_film["location"] if "location" in o_film else "",
            "actors": o_film["actors"] if "actors" in o_film else [],
            "type": o_film["tag"] if "tag" in o_film else [],
        }
        print(data)
        accept_count += 1
        film_stream.insert_one(data)

print("miss: " + str(miss_count))
print("accept: " + str(accept_count))
