import argparse
import logging
import operator
import redis
import pandas as pd

class OLSRecommender(object):
    pattern = tuple()
    columns = ['bed', 'bath', 'price']
    
    def __init__(self, pattern, people=1, base_nbhd=None, z_score=1):
        self.z_score = z_score
        self.people = people
        self.cache = redis.Redis()
        if isinstance(pattern, (list, tuple)):
            self.pattern = pattern
        elif isinstance(pattern, str):
            self.pattern = pattern.split(',')
        self.base_nbhd = base_nbhd

    def read_cache(self):
        keys = sorted(self.cache.keys())
        self.df = reduce(pd.DataFrame.append, pd.read_msgpack(''.join(self.cache.get(k) for k in keys)))

    def read_msgpack(self, filename):
        with open(filename) as fh:
            self.df = pd.read_msgpack(fh.read())

    def clean(self):
        self.df['id'] = self.df.url.str.rstrip('.html').str.split('/').str[-1]
        self.df = df.drop_duplicates(['id', 'timestamp'], take_last=True)
        # figure out a better way to clean bad unicode
        self.df.nbhd = self.df.nbhd.str.lower().str.replace(u'\xe2', u'').str.replace(u'\xa0', '')
        self.df = self.df.loc[self.df[self.columns].dropna(1).index]

    def compute_dummy(self):
        bol = reduce(operator.or_, map(self.df.nbhd.str.match, self.pattern))
        nbhd = self.df[bol].nbhd.unique()
        self.nbhd = pd.Series(nbhd, index=nbhd)
        return self.df.nbhd.apply(lambda x: x==self.nbhd)

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
        yhat = self.coompute_yhat()
        diff = self.df.price - yhat
        fit = diff<0
        df = self.df[fit]
        return df[df.bed<=self.people]
        
    def run(self):
        self.read_cache()
        self.clean()
        self.compute_ols()
        df = self.filter()
        return df.to_json()

if __name__ == '__main__':
    reg = OLSRecommender([])
