from enum import Enum
from hdfs import InsecureClient
import pymongo


class StreamType(Enum):
    FILM = 1
    AREA = 2


class Config(object):
    shea_conf = {
        "mongo_client": 'mongodb://localhost:27017/',
        "database": "sparkpractise",
        "film_collection": "filmStreamFull",
        "area_collection": "area_stream",
        "hdfs": "http://localhost:9870",
        "user": "shea"
    }

    gyue_conf = {
        "mongo_client": 'mongodb://localhost:27017/',
        "database": "film",
        "film_collection": "film_stream",
        "area_collection": "area_stream",
        "hdfs": "http://localhost:9870",
        "user": "gyue"
    }

    @staticmethod
    def get_config(conf, stream_type):
        mongo_client = pymongo.MongoClient(conf['mongo_client'])
        database = mongo_client[conf['database']]
        if stream_type == StreamType.FILM:
            database.drop_collection("actor_box")
            database.drop_collection("film_box")
            database.drop_collection("location_box")
            database.drop_collection("total_box")
            database.drop_collection("type_box")
        else: 
            database.drop_collection("area_box")
        collection = database[conf['film_collection' if stream_type == StreamType.FILM else 'area_collection']]
        hdfs_client = InsecureClient(conf['hdfs'], user=conf['user'])
        return collection, hdfs_client, "streamInput/film" if stream_type == StreamType.FILM else "streamInput/area"
