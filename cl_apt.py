import logging
import argparse
import time
import redis
import requests
import pandas as pd
from datetime import datetime
from urllib2 import urlparse
from multiprocessing import Pool, cpu_count
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--level', default='INFO')
parser.add_argument('-u', '--url', default='http://sfbay.craigslist.org/search/sfc/apa', help='base url for your apartment search page')
parser.add_argument('-s', '--sleep', default=0.1, type=float)
parser.add_argument('-p', '--pages', default=10, type=int)
parser.add_argument('-t', '--timeout', default=60*60*24*14, type=int)

def availability_check(req):
    if req.ok and '(The title on the listings page will be removed in just a few minutes.)' not in req.text:
        return True
    return False

def parse(bs):
    head = bs.find('span', class_='postingtitletext')
    logging.debug(head.text)
    geo = bs.find('div', id='map')
    meta = bs.find('p', class_='attrgroup')
    logging.debug(meta.text)
    span = meta.find_all('span')

    body = bs.find('section', id='postingbody').text
    price = float(head.find('span', class_='price').text.lstrip('$')) if head.find('span', class_='price') else None
    title = head.text.encode('utf-8')
    nbhd = head.find('small').text.encode('utf-8').strip().lstrip('(').rstrip(')') if head.find('small') else ''
    bed, bath, sqft, type = None, None, None, None
    for s in span:
        if s.find_all('b'):
            for b in span[0].find_all('b'):
                if b.nextSibling and b.nextSibling.startswith('BR'):
                    if b.text and b.text.isdigit():
                        bed = int(b.text)
                    else:
                        logging.warn(b.text)
                elif b.nextSibling and b.nextSibling.startswith('Ba'):
                    if b.text and b.text.replace('.', '').isdigit():
                        bath = float(b.text)
                elif b.nextSibling and b.nextSibling.startswith('ft'):
                    if b.text and b.text.isdigit():
                        sqft = int(b.text)
        elif not s.attrs:
            type = s.text
    lat = float(geo.attrs.get('data-latitude')) if geo else None
    lon = float(geo.attrs.get('data-longitude')) if geo else None
    return title, price, bed, bath, sqft, lat, lon, nbhd

class Crawler(object):
    def __init__(self, base_url, pages=10, sleep=0.3, timeout=60*60*24*14):
        self.base_url = base_url
        self.sess = requests.session()
        self.pages = pages
        self.now = datetime.now()
        self.sleep = sleep
        self.cache = redis.Redis()
        self.timeout = timeout

    def get(self, page=0):
        time.sleep(self.sleep)
        if page != 0:
            return self.sess.get(self.base_url)
        else:
            return self.sess.get(self.base_url, params={'s':page})

    def parse_index(self, bs):
        url = self.base_url
        def parse_span(span):
            return [span.text.encode('utf-8'), datetime.strptime(span.find('time').attrs.get('datetime'),'%Y-%m-%d %H:%M'), urlparse.urljoin(url, span.find('a').attrs.get('href'))]
        content = bs.find('div', class_='content')
        span = content.find_all('span', class_='pl')
        return map(parse_span, span)

    def crawl(self):
        rsp = [self.get(i) for i in xrange(self.pages)]
        self.data = reduce(list.__add__, (self.parse_index(BeautifulSoup(i.text)) for i in rsp if availability_check(i)))

    def get_data(self):
        for row in self.data:
            time.sleep(self.sleep)
            try:
                req = self.sess.get(row[2])
            except Exception as e:
                logging.error(e)
                logging.error(row[2])
                continue
            if not req.ok:
                logging.error(req.reason)
                continue
            try:
                row.extend(list(parse(BeautifulSoup(req.text))))
            except Exception as e:
                logging.error(e)
                logging.error(row[2])

    def write(self):
        df = pd.DataFrame(self.data, columns=['desc', 'timestamp', 'url', 'title', 'price', 'bed', 'bath', 'sqft', 'lat', 'lon', 'nbhd'])
        key = 'cl_apt_{:%Y%m%d%H}'.format(self.now)
        try:
            self.cache.set(key, df.to_msgpack())
            self.cache.expire(key, self.timeout)
        except Exception as e:
            logging.error(str(e))
            df.to_csv('{}.csv'.format(key))

    def run(self):
        logging.info('start crawling')
        self.crawl()
        self.get_data()
        self.write()
        logging.info('finished')

if __name__ == '__main__':
    args = parser.parse_args()
    logging.basicConfig(level=args.level, format='%(asctime)s:%(levelname)s:%(module)s:%(funcName)s:%(lineno)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    crawler = Crawler(args.url, pages=args.pages, sleep=args.sleep)
    crawler.run()

        
        
