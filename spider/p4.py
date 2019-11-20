import requests

base_url = 'https://i.nhentai.net/galleries/1452395/'
base_dir = '/Users/gyue/Downloads/[Meido Yomi] FUTANA[RE]BELLION Ch. 1 [Chinese] [黄记汉化组]'
for i in range(2, 30):
    jpg = requests.get('%s%s.jpg' % (base_url, str(i))).content
    with open('%s/%s.jpg' % (base_dir, str(i)), 'wb+') as w:
        w.write(jpg)
    print('finish: ' + str(i))