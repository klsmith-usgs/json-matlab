import json
import requests

from geo_utils import extent_from_hv

__HOST__ = r'http://lcmap-test.cr.usgs.gov/changes/results'
__ALGORITHM__ = r'lcmap-pyccd:1.1.0'


def api_request(x, y, refresh=False):
    endpoint = '/'.join([__HOST__, __ALGORITHM__, str(x), str(y)]) +\
               '?refresh={}'.format(str(refresh).lower())

    resp = requests.get(endpoint)
    
    return resp.json()


def queue_tile_processing(h, v, refresh=False):
    ext, _ = extent_from_hv(h, v)
    
    resps = []
    for y in xrange(ext.y_max, ext.y_max - 5000 * 30, -3000):
        for x in xrange(ext.x_min, ext.x_min + 5000 * 30, 3000):
            resps.append(api_request(x, y, refresh=refresh))
            
    return resps       
     

def request_results(x, y):
    resp = api_request(x, y)
    
    if 'result_ok' in resp and resp['result_ok'] is True:
        return json.loads(resp['result'])

    else:
        return None
