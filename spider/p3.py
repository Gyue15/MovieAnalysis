import requests

base_url = 'https://i.nhentai.net/galleries/1465581/'
base_dir = '/Users/gyue/Downloads/[Morittokoke (Morikoke)] Jack-kun no Ecchi na Omamagoto'
for i in range(1, 21):
    jpg = requests.get('%s%s.jpg' % (base_url, str(i))).content
    with open('%s/%s.jpg' % (base_dir, str(i)), 'wb+') as w:
        w.write(jpg)
    print('finish: ' + str(i))