import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client["film"]
collection = database['movie_train']

m_list = []
for m in collection.find({}):
    m_list.append(m)

for m in m_list:
    dates = m['dates']
    date_keys = set()
    for d in dates:
        date_keys.add(list(d.keys())[0])
    total_box = 0
    env_features = [0] * 50
    for m2 in m_list:
        if m2['_id'] == m['_id'] or m2['cluster'] == '':
            continue
        m2_dates = m2['dates']
        for d in m2_dates:
            dk = list(d.keys())[0]
            if dk in date_keys:
                total_box += d[dk]
                env_features[m2['cluster']] += 1
    box_percent = m['box'] / (m['box'] + total_box)
    collection.update_one({"_id": m['_id']}, {"$set": {"box_percent": box_percent, "env_features": env_features}})
