#!/usr/bin/env python3
import csv
import os
import json

from time import sleep
from random import betavariate

from string import ascii_uppercase
import itertools

import requests

KEY = 'AIzaSyDQPYB_peaMAM9c7-69NRkZTuk38S6OMRI'
DIRECTORY = os.path.join('data', 'all_results')

def randomsleep():
    'Sleep between zero and 100 seconds.'
    sleep(10 * betavariate(0.7, 8))

def table_features(table_id, select, where, maxResults = 1000, pageToken = None):
    url = 'https://www.googleapis.com/mapsengine/v1/tables/%s/features/' % table_id

    params = {
        'key': KEY,
        'version': 'published',
        'maxResults': maxResults,
        'select': select,
    }
    if where:
        params['where'] = where
    if pageToken:
        params['pageToken'] = pageToken

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0',
        'Referer':  'http://maps.nyc.gov/crime/',
    }
    paramsStr= ''.join('{}{}'.format(key, val) for key, val in params.items())
    
    r = requests.get(url, headers = headers, params = params)
    print (r.url)
    randomsleep()
    return r

def mkpath(pageToken, table_id):
    filename = pageToken if pageToken else '__%s__' % table_id
    return os.path.join(DIRECTORY, filename)

def mkfp(pageToken, table_id, mode = 'xb'):
    return open(mkpath(pageToken, table_id), mode)

def page(table_id, select, where,pageToken = None):
    '''
    Args: A pageToken or None
    Returns: The next pageToken or None
    '''

    path = mkpath(pageToken, table_id)
    if os.path.exists(path):
        return json.load(open(path))
    else:
        r = table_features(table_id, select, where, maxResults = 10000, pageToken = pageToken)
        fp = mkfp(pageToken, table_id, mode = 'xb')
        fp.write(r.content)
        fp.close()
        return json.loads(r.text)

def features(table_id, select,where, startPageToken = None):
    os.makedirs(DIRECTORY, exist_ok = True)

    if startPageToken:
        pageToken = startPageToken
    else:
        print('Loading data for the initial search, without pageToken')
        results = page(table_id, select,where)
        for result in results.get('features', []):
            yield result
        pageToken = results.get('nextPageToken')

    while pageToken:
        print('Loading data for pageToken', pageToken)
        results = page(table_id, select,where, pageToken = pageToken)
        for result in results.get('features', []):
            yield result
        pageToken = results.get('nextPageToken')

def head(table_id, select,where):
    path = 'head-%s.geojson' % table_id
    if not os.path.exists(path):
        fp = open(path, 'xb')
        r = table_features(table_id, select, where, maxResults = 10000)
        fp.write(r.content)
        fp.close()

def to_geojson(table_id, select,where):
    path = os.path.join('data',table_id + '.geojson')
    data = {
        'type': 'FeatureCollection',
        'features': list(features(table_id, select,where)),
    }
    with open(path, 'w') as fp:
        json.dump(data, fp)

def to_csv(table_id, select,where):
    path = os.path.join('data',table_id + '.csv')
    fieldnames = ['longitude', 'latitude'] + select.split(',')
    fieldnames.remove('geometry')
    with open(path, 'w') as fp:
        w = csv.DictWriter(fp, fieldnames = fieldnames)
        w.writeheader()
        for feature in features(table_id, select,where):
            row = {
                'longitude': feature['geometry']['coordinates'][0],
                'latitude': feature['geometry']['coordinates'][1],
            }
            row.update(feature['properties'])
            w.writerow(row)

def main():
    for table_id, select,year in [
        #('02378420399528461352-17772055697785505571', 'YR,MO,geometry,X,Y,TOT,CR', 2015),
         ('02378420399528461352-17234028967417318364', 'YR,MO,geometry,X,Y,TOT,CR', 2014)
    ]:
        for Month in range(1,13):
            if (year > 2014 and Month > 1) : 
                break
            where = "MO=" + str(Month) + " AND " + "YR=" + str(year)
            head(table_id, select,where)
            to_csv(table_id, select,where)
            to_geojson(table_id, select,where)
        where = "MO=7" + " AND " + "YR=" + str(year)
        head(table_id, select,where)
        to_csv(table_id, select,where)
        to_geojson(table_id, select,where)

if __name__ == '__main__':
    main()