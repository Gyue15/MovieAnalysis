import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
collection = database['movie_train']

with open('box_train_data.txt', 'a+') as w:
    for m in collection.find({}):
        features = m['env_features']
        if 1 in m['features']:
            s = ''
            for i in range(len(features)):
                if features[i] > 0:
                    s = s + ' ' + str(i + 1) + ":" + str(features[i] / 50)
            for i in range(len(m['features'])):
                if m['features'][i] > 0:
                    s = s + ' ' + str(i + len(features) + 1) + ":" + str(m['features'][i])
            s = str(round(m['box_percent'] * 100, 2)) + ' ' + s + '\n'
            w.write(s)
