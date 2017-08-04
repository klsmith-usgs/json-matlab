"""
Classification Maps for CCDC visualizations
"""

import os
import sys
import multiprocessing as mp
import datetime as dt
import pickle

from osgeo import gdal, osr
import numpy as np
import scipy.io as sio

import geo_utils
from class_products import ClassModel, class_primary, class_secondary, conf_primary, conf_secondary, fromto, sort_models
from logger import log


CONUS_ALBERS = osr.SpatialReference()
CONUS_ALBERS.ImportFromEPSG(5070)
CONUS_WKT = CONUS_ALBERS.ExportToWkt()

MAP_NAMES = ('CoverPrim', 'CoverSec', 'CoverConfPrim', 'CoverConfSec', 'CoverFromTo')
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
            ret[m][yr] = np.zeros(shape=(100, 100))

    return ret


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
    return gdal.GDT_Byte
    # if product in ('ChangeMap', 'NumberMap', 'LastChange', 'SegLength'):
    #     return gdal.GDT_UInt16
    # elif product in ('ChangeMagMap', 'ConditionMap'):
    #     return gdal.GDT_Float32
    # elif product in ('CoverMap', 'CoverQAMap', 'QAMap', 'coverage'):
    #     return gdal.GDT_Byte
    # else:
    #     raise ValueError


def open_classpickle(file_path):
    return pickle.load(open(file_path, 'rb'))


def coords_frompath(file_path):
    parts = os.path.split(file_path)[-1].split('_')
    return parts[1], parts[2]


def classmap_vals(input, query_dates=QUERY_DATES):
    data = open_classpickle(input)
    chip_x, chip_y = coords_frompath(input)

    temp = map_template()
    temp['chip_x'] = int(chip_x)
    temp['chip_y'] = int(chip_y)

    row = 0
    col = 0
    for result in data:
        models = [ClassModel(class_probs=r['class_probs'],
                             class_vals=r['class_vals'],
                             end_day=r['end_day'],
                             start_day=r['start_day'])
                  for r in result]

        models = sort_models(models)

        cl_pr = [class_primary(models, d) for d in query_dates]
        cl_sc = [class_secondary(models, d) for d in query_dates]
        conf_pr = [conf_primary(models, d) for d in query_dates]
        conf_sc = [conf_secondary(models, d) for d in query_dates]
        cl_fromto = [fromto(models, d) for d in query_dates]

        # ('CoverPrim', 'CoverSec', 'CoverConfPrim', 'CoverConfSec',
         # 'CoverFromTo')
        for idx, qdate in enumerate(query_dates):
            year = dt.date.fromordinal(qdate).year

            temp['CoverPrim'][year][row, col] = cl_pr[idx]
            temp['CoverSec'][year][row, col] = cl_sc[idx]
            temp['CoverConfPrim'][year][row, col] = conf_pr[idx]
            temp['CoverConfSec'][year][row, col] = conf_sc[idx]
            temp['CoverFromTo'][year][row, col] = cl_fromto[idx]

        col += 1

        if col > 99:
            row += 1
            col = 0

    return temp


def xyoff(h, v, chip_x, chip_y):
    coord = geo_utils.GeoCoordinate(x=chip_x, y=chip_y)
    _, geo = geo_utils.extent_from_hv(h, v)

    rowcol = geo_utils.geo_to_rowcol(geo, coord)
    return rowcol.column, rowcol.row


def output_chip(data, output_dir, h, v):
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

            log.debug('Received {}'.format(infile))

            if infile == 'kill':
                output_q.put('kill')
                break

            map_dict = classmap_vals(infile)

            log.debug('Finished: {0} {1}'.format(map_dict['chip_x'],
                                                 map_dict['chip_y']))
            output_q.put(map_dict)
        except Exception as e:
            log.exception('EXCEPTION')
            continue


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


def main():
    indir = r'X:\klsmith\class'
    outdir = r'C:\temp\class'
    procs = 4
    h = 5
    v = 2

    multi_run(indir, outdir, procs, h, v)

if __name__ == '__main__':
    main()
