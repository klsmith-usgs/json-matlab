import json
import requests
import logging
import commons

from geo_utils import extent_from_hv


logging.getLogger("requests").setLevel(logging.WARNING)

__HOST__ = r'http://lcmap-test.cr.usgs.gov/changes/results'
__ALGORITHM__ = r'lcmap-pyccd:1.4.0rc1'


@commons.retry(10)
def fetch_results_pixel(x, y, refresh=False):
    endpoint = '/'.join([__HOST__, __ALGORITHM__, str(x), str(y)]) +\
               '?refresh={}'.format(str(refresh).lower())

    resp = requests.get(endpoint)
    
    return resp.json()


@commons.retry(10)
def fetch_results_chip(x, y, algorithm=__ALGORITHM__):
    url = ('http://lcmap-test.cr.usgs.gov/'
           'changes/'
           'results/'
           '{algorithm}/'
           'chip?x={x}&y={y}'
           .format(x=x, y=y, algorithm=algorithm))

    resp = requests.get(url)

    if resp.status_code == 200:
        return resp.json()


def queue_tile_processing(h, v, refresh=False):
    ext, _ = extent_from_hv(h, v)
    
    resps = []
    for y in xrange(ext.y_max, ext.y_min, -3000):
        for x in xrange(ext.x_min, ext.x_max, 3000):
            resps.append(fetch_results_pixel(x, y, refresh=refresh))
            
    return resps       
     

def request_results(x, y):
    resp = fetch_results_pixel(x, y)
    
    if resp.get('result_ok'):
        return json.loads(resp['result'])

    else:
        return None
