import argparse
import operator
import json
import redis
import requests
import pandas as pd
from datetime import datetime, timedelta

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--search', default='soma,hayes valley,marina,mission', help='search pattern')
parser.add_argument('-p', '--people', default=2, type=int, help='max number of people')
parser.add_argument('-z', '--z_score', default=6, type=int, help='zscore filtered by')
parser.add_argument('-n', '--nbhd', help='base nbhd')

def availability_check(url):
    req = requests.get(url)
    if req.ok and '(The title on the listings page will be removed in just a few minutes.)' not in req.text:
        return True
    return False

class OLSRecommender(object):
    pattern = tuple()
    columns = ['bed', 'bath', 'price', 'lat', 'lon']

    def __init__(self, pattern, people=1, base_nbhd=None, z_score=1, verify=False):
        self.verify = verify
        self.df = None
        self.nbhd = None
        self.ols = None
        self.z_score = z_score
        self.people = people
        self.cache = redis.Redis()
        if isinstance(pattern, (list, tuple)):
            self.pattern = pattern
        elif isinstance(pattern, str):
            self.pattern = pattern.split(',')
        self.base_nbhd = base_nbhd

    def read_cache(self):
        keys = sorted(self.cache.keys(), reverse=True)[:28]
        self.df = reduce(pd.DataFrame.append, pd.read_msgpack(''.join(self.cache.get(k) for k in keys)))
        self.df.index = range(len(self.df))

    def read_msgpack(self, filename):
        with open(filename) as fh:
            self.df = pd.read_msgpack(fh.read())

    def clean(self):
        self.df['id'] = self.df.url.str.rstrip('.html').str.split('/').str[-1]
        # figure out a better way to clean bad unicode
        self.df.nbhd = self.df.nbhd.str.lower().str.replace(u'\xe2', u'').str.replace(u'\xa0', '')
        self.df = self.df.loc[self.df[self.columns].dropna().index]
        self.df.drop_duplicates(subset=['id', 'timestamp'], take_last=True, inplace=True)

    def compute_dummy(self):
        bol = reduce(operator.or_, map(self.df.nbhd.str.match, self.pattern))
        nbhd = self.df[bol].nbhd.unique()
        self.nbhd = pd.Series(nbhd, index=nbhd)
        return self.df.nbhd.apply(lambda x: x == self.nbhd)

    def compute_ols(self):
        x = self.compute_dummy()
        x['bed'] = self.df.bed
        x['bath'] = self.df.bath
        self.ols = pd.ols(x=x, y=self.df.price)
    
    def compute_yhat(self, z_score=None):
        sigma = self.ols.beta - self.ols.std_err*(z_score or self.z_score)
        yhat = self.df.bed * sigma.bed + self.df.bath * sigma.bath + sigma.intercept
        if self.base_nbhd:
            yhat += sigma[self.base_nbhd]
        return yhat

    def filter(self):
        yhat = self.compute_yhat()
        diff = self.df.price - yhat
        fit = diff < 0
        df = self.df[fit]
        bol = df.timestamp > (datetime.now().date()-timedelta(7))
        return df[(df.bed<=self.people)&df.nbhd.isin(self.nbhd)&bol].sort('timestamp', ascending=False)

    def run(self):
        self.clean()
        self.compute_ols()
        df = self.filter()
        if not self.verify:
            return df
        bol = df.url.apply(availability_check)
        return df[bol]

def search_apartment(search, people, nbhd, z_score, verify=False):
    rec = OLSRecommender(search, people, nbhd, z_score, verify)
    rec.read_cache()
    df = rec.run()
    if len(df)>0:
        return json.dumps(json.loads(df.to_json(orient='records', date_format='iso')), indent=4)

if __name__ == '__main__':
    args = parser.parse_args()
    string = search_apartment(args.search, args.people, args.nbhd, args.z_score, True)
    if string:
        print(string)
