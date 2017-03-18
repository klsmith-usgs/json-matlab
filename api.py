import json
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from geo_utils import extent_from_hv

__HOST__ = r'http://lcmap-test.cr.usgs.gov/changes/results'
__ALGORITHM__ = r'lcmap-pyccd:1.1.0'


retries = Retry(total=5,
                backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])


def api_request(x, y, refresh=False):
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=retries))

    endpoint = '/'.join([__HOST__, __ALGORITHM__, str(x), str(y)]) +\
               '?refresh={}'.format(str(refresh).lower())

    resp = s.get(endpoint)
    
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
