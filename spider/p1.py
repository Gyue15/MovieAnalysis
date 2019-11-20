import requests

base_url = 'https://i.nhentai.net/galleries/1484187/'
base_dir = '/Users/gyue/Downloads/asmr/hakaba/'
dir_name = '[Dairiseki (Hakaba)] Nikudorei Iwase Aiko (Bakuman.) [Chinese]'
for i in range(2, 39):
    jpg = requests.get('%s%s.jpg' % (base_url, str(i))).content
    with open('%s/%s.jpg' % (base_dir + dir_name, str(i)), 'wb+') as w:
        w.write(jpg)
    print('finish: ' + str(i))