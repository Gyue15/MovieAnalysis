import numpy as np
from sklearn.decomposition import SparsePCA
from sklearn.decomposition import PCA
from data_handler import get_data_dic

types_dic, directors_dic, casts_dic, editors_dic, movies = get_data_dic()

length = 41448
mat = []
for m in movies:
    features = set()
    for t in m['types']:
        features.add(types_dic[t] - 1)
    for d in m['directors']:
        features.add(len(types_dic) + directors_dic[d] - 1)
    for c in m['casts']:
        features.add(len(types_dic) + len(directors_dic) + casts_dic[c] - 1)
    for e in m['editors']:
        features.add(len(types_dic) + len(directors_dic) + len(casts_dic) + editors_dic[e] - 1)
    row = []
    index = 0
    for i in range(length):
        if i in features:
            row.append(1)
        else:
            row.append(0)
    mat.append(row)

X = np.array(mat)
print(X)
print('start pca...')
pca = SparsePCA(n_components='mle')
pca.fit(X)
low_d = pca.transform(X)
print(low_d)
