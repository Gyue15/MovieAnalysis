import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
collection = database['movie_train']

with open('box_train_data_benchmark_other.txt', 'a+') as w:
    for m in collection.find({}):
        features = m['features']
        if (1 in features) and (m['rate'] != ''):
            s = ''
            for i in range(len(features)):
                if features[i] > 0:
                    s = s + ' ' + str(i + 1) + ":" + str(features[i])
            # s = str(round(m['box_percent'] * 100, 2)) + ' ' + '1:' + str(float(m['rate']) / 10) + s + '\n'
            s = str(round(m['box_percent'] * 100, 2)) + s + '\n'
            # print(len(features))
            w.write(s)
