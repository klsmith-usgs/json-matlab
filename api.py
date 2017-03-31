import json
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from geo_utils import extent_from_hv

__HOST__ = r'http://lcmap-test.cr.usgs.gov/changes/results'
__ALGORITHM__ = r'lcmap-pyccd:1.1.0'


retries = Retry(total=10,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504])


def api_request(x, y, refresh=False):
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=retries))

    endpoint = '/'.join([__HOST__, __ALGORITHM__, str(x), str(y)]) +\
               '?refresh={}'.format(str(refresh).lower())

    resp = s.get(endpoint)
    
    return resp.json()


def fetch_results_tile(x, y, algorithm=__ALGORITHM__):
    url = ('http://lcmap-test.cr.usgs.gov/'
           'changes/'
           'results/'
           '{algorithm}/'
           'tile?x={x}&y={y}'
           .format(x=x, y=y, algorithm=algorithm))

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=retries))

    resp = s.get(url)

    if resp.status_code == 200:
        return resp.json()


def queue_tile_processing(h, v, refresh=False):
    ext, _ = extent_from_hv(h, v)
    
    resps = []
    for y in xrange(ext.y_max, ext.y_min, -3000):
        for x in xrange(ext.x_min, ext.x_max, 3000):
            resps.append(api_request(x, y, refresh=refresh))
            
    return resps       
     

def request_results(x, y):
    resp = api_request(x, y)
    
    if 'result_ok' in resp and resp['result_ok'] is True:
        return json.loads(resp['result'])

    else:
        return None
