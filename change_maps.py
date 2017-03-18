"""
Change Maps for CCDC visualizations
"""

import os
import datetime
import logging
import multiprocessing as mp
import urllib2
import json

from osgeo import gdal, osr
import numpy as np

import geo_utils


LOGGER = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)


__HOST__ = r'http://lcmap-test.cr.usgs.gov/changes/results'
__ALGORITHM__ = r'lcmap-pyccd:1.1.0'


CONUS_ALBERS = osr.SpatialReference()
CONUS_ALBERS.ImportFromEPSG(5070)


def api_request(x, y, host=__HOST__, algorithm=__ALGORITHM__):
    endpoint = '/'.join([host, algorithm, str(x), str(y)])

    request = urllib2.Request(endpoint)

    try:
        result = urllib2.urlopen(request)
    except urllib2.HTTPError:
        raise

    return json.loads(result.read())


def create_changemap_dict(x, y):
    map_names = ('ChangeMap', 'ChangeMagMap', 'QAMap', 'NumberMap', 'LastChange')

    def add_year(year):
        for c in map_names:
            if year not in changemaps[c]:
                changemaps[c][year] = 0

    api_ret = api_request(x, y)

    changemaps = {'y': y,
                  'x': x,
                  'ChangeMap': {},
                  'ChangeMagMap': {},
                  'QAMap': {},
                  'NumberMap': {},
                  'LastChange': {}
                  }

    if api_ret['result_ok'] is False:
        return changemaps

    results = json.loads(api_ret['result'])

    for model in results['change_models']:
        pass

    return changemaps


def output_maps(data, output_dir, h, v):
    y = data.pop('y')
    x = data.pop('x')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for prod in data:
        for year in data[prod]:
            ds = get_raster_ds(output_dir, prod, year, h, v)
            ds.GetRasterBand(1).WriteArray(data[prod][year].reshape(1, 5000), 0, y_off)

            ds.FlushCache()
            ds = None


def get_raster_ds(output_dir, product, year, h, v):
    key = '{0}_{1}'.format(product, year)

    file_path = os.path.join(output_dir, key + '.tif')

    if os.path.exists(file_path):
        ds = gdal.Open(file_path, gdal.GA_Update)
    else:
        ds = create_geotif(file_path, product, h, v)

    return ds


def create_geotif(file_path, product, h, v, rows=5000, cols=5000, proj=CONUS_ALBERS.ExportToWkt()):
    data_type = prod_data_type(product)
    _, geo = geo_utils.extent_from_hv(h, v)

    ds = (gdal
          .GetDriverByName('GTiff')
          .Create(file_path, cols, rows, 1, data_type))

    ds.SetGeoTransform(geo)
    ds.SetProjection(proj)

    return ds


def prod_data_type(product):
    if product in ('ChangeMap', 'NumberMap', 'LastChange'):
        return gdal.GDT_UInt16
    elif product in ('ChangeMagMap', 'ConditionMap'):
        return gdal.GDT_Float32
    elif product in ('CoverMap', 'CoverQAMap', 'QAMap'):
        return gdal.GDT_Byte
    else:
        raise ValueError


def multi_output(output_dir, ref_image, output_q, kill_count):
    count = 0
    while True:
        if count >= kill_count:
            break

        outdata = output_q.get()

        if outdata == 'kill':
            count += 1
            continue

        LOGGER.debug('Outputting line: {0}'.format(outdata['y_off']))
        output_maps(outdata, output_dir, ref_image)


def multi_worker(input_q, output_q):
    while True:
        infile = input_q.get()

        if infile == 'kill':
            output_q.put('kill')
            break

        change = ChangeMap().create_changemap_dict(infile)

        output_q.put(change)


def single_run(input_dir, output_dir, ref_image):
    for f in os.listdir(input_dir):
        change = ChangeMap().create_changemap_dict(os.path.join(input_dir, f))
        LOGGER.debug('Outputting line: {0}'.format(change['y_off']))
        output_maps(change, output_dir, ref_image)


def multi_run(input_dir, output_dir, ref_image, num_procs):
    input_q = mp.Queue()
    output_q = mp.Queue()

    worker_count = num_procs - 1

    for f in os.listdir(input_dir):
        input_q.put(os.path.join(input_dir, f))

    for _ in range(worker_count):
        input_q.put('kill')

    for _ in range(worker_count):
        mp.Process(target=multi_worker, args=(input_q, output_q)).start()

    multi_output(output_dir, ref_image, output_q, worker_count)


if __name__ == '__main__':
    indir = r'D:\lcmap\matlab_compare\WA-08\zhe\TSFitMap'
    outdir = r'D:\lcmap\matlab_compare\WA-08\klsmith\changemaps'

    test_image = r'D:\lcmap\matlab_compare\WA-08\LT50460271990297\LT50460271990297PAC04_MTLstack'
    # single_run(indir, outdir, test_image)
    multi_run(indir, outdir, test_image, 4)
