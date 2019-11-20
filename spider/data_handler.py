import pymongo


if __name__ == '__main__':

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    database = client["film"]
    collection = database['movie_train']

    with open('rate_train_data.txt', 'a+') as w1:
        with open('cluster_train_data.txt', 'a+') as w2:
            index = 0
            for m in collection.find({}):
                features = m['features']
                if 1 in features:
                    s = ''
                    for i in range(len(features)):
                        if features[i] == 1:
                            s = s + ' ' + str(i + 1) + ":1"
                    s = s + "\n"
                    if m['rate'] != '':
                        w1.write(m['rate'] + ' ' + s)
                    w2.write(str(index) + ' ' + s)
                    index += 1
