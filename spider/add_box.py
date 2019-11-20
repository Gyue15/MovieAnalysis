import pymongo
import math

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
source = database['movie_with_rate']
box = database['film_stream']
dst_train = database['movie_train']
dst_test = database['movie_test']

actors = set()
directors = set()
types = set()
total_box = {}
for f in box.find({}).sort([('_id', 1)]):
    if (not f['total_box']) or math.isnan(f['total_box']):
        continue
    if f["movie_name"] in total_box:
        total_box[f["movie_name"]]['box'] += f['total_box']
    else:
        total_box[f["movie_name"]] = {
            'title': f["movie_name"],
            'box': f['total_box'],
            'actors': f['actors'],
            'directors': f['director'],
            'types': f['type'],
            'dates': []
        }
    total_box[f["movie_name"]]['dates'].append({f['time']: f['total_box']})
    actors.update(f['actors'])
    if f['director'] != '':
        directors.add(f['director'])
    types.update(f['type'])

# print(len(total_box))
# print(total_box)

features = {}
begin = 0
fs = []
for a in sorted(list(actors)):
    features['a_' + a] = begin
    fs.append('a_' + a)
    begin += 1

for d in sorted(list(directors)):
    features['d_' + d] = begin
    fs.append('d_' + d)
    begin += 1

for t in sorted(list(types)):
    features['t_' + t] = begin
    fs.append('t_' + t)
    begin += 1

print(fs)

fond = 0
for k in total_box.keys():
    m = source.find_one({'title': k})
    if m:
        total_box[k]['rate'] = m['rate']
        fond += 1
    else:
        total_box[k]['rate'] = ''
    m_feature = [0] * len(features)
    for t in total_box[k]['types']:
        m_feature[features['t_' + t]] = 1
    if total_box[k]['directors'] != '':
        m_feature[features['d_' + total_box[k]['directors']]] = 1
    for a in total_box[k]['actors']:
        m_feature[features['a_' + a]] = 1
    total_box[k]['features'] = m_feature

print(fond)

m_train = []
m_test = []
index = 0
for v in total_box.values():
    if index < len(total_box) * 0.7:
        m_train.append(v)
    else:
        m_test.append(v)
    index += 1

dst_train.insert_many(m_train)
dst_test.insert_many(m_test)
