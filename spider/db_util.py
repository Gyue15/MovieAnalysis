import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
source = database['movie_with_rate']
des = database['movie_with_rate_small']
source.drop()

casts = {}
directors = {}
types = {}
editors = {}
movies = []
for m in source.find({}).sort([('_id', 1)]):
    if m['casts'] != [] and m['directors'] != [] and m['types'] != '' and m['editors'] != []:
        movies.append(m)
        for c in m['casts']:
            casts[c] = casts.get(c, 0) + 1
        for t in m['types']:
            types[t] = types.get(t, 0) + 1
        for e in m['editors']:
            editors[e] = editors.get(e, 0) + 1
        for d in m['directors']:
            directors[d] = directors.get(d, 0) + 1

casts_filter = {k: v for k, v in casts.items() if v > 3}
directors_filter = {k: v for k, v in directors.items() if v > 3}
types_filter = {k: v for k, v in types.items() if v > 3}
editors_filter = {k: v for k, v in editors.items() if v > 3}

print(len(casts_filter))
print(len(directors_filter))
print(len(types_filter))
print(len(editors_filter))


def fill_list(l, dic):
    res = []
    for li in l:
        if li in dic.keys():
            res.append(li)
    return res


for m in movies:
    ca = fill_list(m['casts'], casts_filter)
    if not ca:
        continue

    di = fill_list(m['directors'], directors_filter)
    if not di:
        continue

    ty = fill_list(m['types'], types_filter)
    if not ty:
        continue

    ed = fill_list(m['editors'], casts_filter)
    if not ed:
        continue

    m['casts'] = ca
    m['directors'] = di
    m['types'] = ty
    m['editors'] = ed
    del m['_id']

    des.insert_one(m)
