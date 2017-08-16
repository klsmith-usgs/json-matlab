"""
Change Maps for CCDC visualizations
"""

import os
import sys
import multiprocessing as mp
import datetime as dt
import json

from osgeo import gdal, osr
import numpy as np
import scipy.io as sio

import geo_utils
import change_products as cp
from logger import log


__HOST__ = r'http://lcmap-test.cr.usgs.gov/changes/results'
__ALGORITHM__ = r'lcmap-pyccd:1.1.0'

CONUS_ALBERS = osr.SpatialReference()
CONUS_ALBERS.ImportFromEPSG(5070)
CONUS_WKT = CONUS_ALBERS.ExportToWkt()

MAP_NAMES = ('ChangeMap', 'ChangeMagMap', 'QAMap', 'SegLength', 'LastChange')
YEARS = tuple(i for i in range(1984, 2016))
QUERY_DATES = tuple(dt.date(year=i, month=7, day=1).toordinal()
                    for i in YEARS)


def map_template():
    """
    Return a new dictionary to store annual change map values

    {'product name': {1984: np.array values shape=(5000,),
                      1985: np.array values shape=(5000,),
                      ...
                      }
     'next product': {years:
                     }
    """
    ret = {}

    for m in MAP_NAMES:
        ret[m] = {}
        for yr in YEARS:
            ret[m][yr] = np.zeros(shape=(5000,))

    return ret


def get_json(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    else:
        return None


def load_jsondata(data):
    outdata = np.full(fill_value=None, shape=(100, 100), dtype=object)

    if data is not None:
        for d in data:
            result = d.get('result', 'null')

            # Could leverage geo_utils to do this
            col = int((d['x'] - d['chip_x']) / 30)
            row = int((d['chip_y'] - d['y']) / 30)

            try:
                outdata[row][col] = json.loads(result)
            except:
                outdata[row][col] = None

    return outdata


def determine_coverage(data):
    coverage = np.ones(shape=(100, 100), dtype=np.int)

    coverage[data.reshape(100, 100) == None] = 0

    return coverage


def coords_frompath(file_path):
    parts = os.path.split(file_path)[-1].split('_')
    return parts[1], parts[2][:-5]


def changemap_vals(input, query_dates=QUERY_DATES):
    data = load_jsondata(get_json(input)).flatten()
    chip_x, chip_y = coords_frompath(input)

    temp = map_template()
    temp['chip_x'] = int(chip_x)
    temp['chip_y'] = int(chip_y)

    coverage = determine_coverage(data)

    row = 0
    col = 0
    for result in data:
        models = [cp.ChangeModel(r['start_day'], r['end_day'], r['break_day'],
                                 r['qa'], r['magnitudes'], r['change_prob'])
                  for r in result]

        changedates = [cp.changedate_val(models, qd)
                       for qd in query_dates]

        changemag = [cp.changemag_val(models, qd)
                     for qd in query_dates]

        qa = [cp.qa_val(models, qd)
              for qd in query_dates]

        seglength = [cp.seglength_val(models, qd)
                     for qd in query_dates]

        lastchange = [cp.lastchange_val(models, qd)
                      for qd in query_dates]

        for idx, qdate in enumerate(query_dates):
            year = dt.date.fromordinal(qdate).year

            temp['ChangeMap'][year][row, col] = changedates[idx]
            temp['ChangeMagMap'][year][row, col] = changemag[idx]
            temp['QAMap'][year][row, col] = qa[idx]
            temp['SegLength'][year][row, col] = seglength[idx]
            temp['LastChange'][year][row, col] = lastchange[idx]

        col += 1

        if col > 99:
            row += 1
            col = 0

    return temp, coverage


def xyoff(h, v, chip_x, chip_y):
    coord = geo_utils.GeoCoordinate(x=chip_x, y=chip_y)
    _, geo = geo_utils.extent_from_hv(h, v)

    rowcol = geo_utils.geo_to_rowcol(geo, coord)
    return rowcol.column, rowcol.row


def output_chip(data, coverage, output_dir, h, v):
    chip_y = data.pop('chip_y')
    chip_x = data.pop('chip_x')

    x_off, y_off = xyoff(h, v, chip_x, chip_y)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for prod in data:
        for year in data[prod]:
            ds = get_raster_ds(output_dir, prod, year, h, v)
            ds.GetRasterBand(1).WriteArray(data[prod][year], x_off, y_off)

            ds.FlushCache()
            ds = None

    ds = get_raster_ds(output_dir, 'coverage', '', h, v)
    ds.GetRasterBand(1).WriteArray(coverage, x_off, y_off)
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


def create_geotif(file_path, product, h, v, rows=5000, cols=5000, proj=CONUS_WKT):
    data_type = prod_data_type(product)
    _, geo = geo_utils.extent_from_hv(h, v)

    ds = (gdal
          .GetDriverByName('GTiff')
          .Create(file_path, cols, rows, 1, data_type))

    ds.SetGeoTransform(geo)
    ds.SetProjection(proj)

    return ds


# MAP_NAMES = ('ChangeMap', 'ChangeMagMap', 'QAMap', 'SegLength', 'LastChange')
def prod_data_type(product):
    if product in ('ChangeMap', 'NumberMap', 'LastChange', 'SegLength'):
        return gdal.GDT_UInt16
    elif product in ('ChangeMagMap', 'ConditionMap'):
        return gdal.GDT_Float32
    elif product in ('CoverMap', 'CoverQAMap', 'QAMap', 'coverage'):
        return gdal.GDT_Byte
    else:
        raise ValueError


def multi_output(output_dir, output_q, kill_count, h, v):
    count = 0
    progress = 0
    while True:
        if count >= kill_count:
            break

        outdata = output_q.get()

        if outdata == 'kill':
            count += 1
            continue

        outdata, coverage = outdata

        log.debug('Outputting chip: {0} {1}'.format(outdata['chip_x'],
                                                    outdata['chip_y']))
        output_chip(outdata, output_dir, h, v)
        progress += 1
        log.debug('Total chips written: {}'.format(progress))

    log.debug('Finalizing Writes')


def multi_worker(input_q, output_q):
    while True:
        try:
            infile = input_q.get()

            log.debug('received {}'.format(infile))

            if infile == 'kill':
                output_q.put('kill')
                break

            map_dict, coverage = changemap_vals(infile)

            log.debug('finished {}'.format(infile))
            output_q.put((map_dict, coverage))
        except Exception as e:
            log.exception('EXCEPTION')
            continue

#
# def single_run(input_dir, output_dir, h, v):
#     for infile in os.listdir(input_dir):
#         log.debug('received {}'.format(infile))
#         filename = os.path.split(infile)[-1]
#
#         map_dict, coverage = changemap_vals(infile)
#         map_dict['y_off'] = int(filename[13:-4]) - 1
#
#         log.debug('Outputting line: {0}'.format(map_dict['y_off']))
#         output_line(map_dict, coverage, output_dir, h, v)


def multi_run(input_dir, output_dir, num_procs, h, v):
    input_q = mp.Queue()
    output_q = mp.Queue()

    worker_count = num_procs - 1

    for f in os.listdir(input_dir):
        input_q.put(os.path.join(input_dir, f))

    for _ in range(worker_count):
        input_q.put('kill')

    for _ in range(worker_count):
        mp.Process(target=multi_worker,
                   args=(input_q, output_q),
                   name='Process-{}'.format(_)).start()

    multi_output(output_dir, output_q, worker_count, h, v)
#
#
# if __name__ == '__main__':
#     if len(sys.argv) < 6:
#         indir = raw_input('Input directory: ')
#         outdir = raw_input('Output directory: ')
#         cpu = raw_input('Number of CPU\'s: ')
#         horiz = raw_input('H: ')
#         vert = raw_input('V: ')
#     else:
#         indir = sys.argv[1]
#         outdir = sys.argv[2]
#         cpu = int(sys.argv[3])
#         horiz = sys.argv[4]
#         vert = sys.argv[5]
#
#     if cpu < 2:
#         single_run(indir, outdir, horiz, vert)
#     else:
#         multi_run(indir, outdir, int(cpu), int(horiz), int(vert))
