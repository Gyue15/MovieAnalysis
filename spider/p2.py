import requests

base_url = 'https://i.nhentai.net/galleries/1505248/'
base_dir = '/Users/gyue/Downloads/(C85) [Kaguya Hime Koubou (Gekka Kaguya)] THE iDOL M@STER Hayassuka!? Sunday (THE iDOLM@STER)[Chinese] [大友同好会]'
for i in range(1, 32):
    jpg = requests.get('%s%s.jpg' % (base_url, str(i))).content
    with open('%s/%s.jpg' % (base_dir, str(i)), 'wb+') as w:
        w.write(jpg)
    print('finish: ' + str(i))