"""
Change Maps for CCDC visualizations
"""

import os
import sys
import logging
import multiprocessing as mp
import datetime as dt

from osgeo import gdal, osr
import numpy as np
import scipy.io as sio

import geo_utils
import change_products


LOGGER = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s: %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)


__HOST__ = r'http://lcmap-test.cr.usgs.gov/changes/results'
__ALGORITHM__ = r'lcmap-pyccd:1.1.0'

CONUS_ALBERS = osr.SpatialReference()
CONUS_ALBERS.ImportFromEPSG(5070)
CONUS_WKT = CONUS_ALBERS.ExportToWkt()

MAP_NAMES = ('ChangeMap', 'ChangeMagMap', 'QAMap', 'SegLength', 'LastChange')
YEARS = tuple(i for i in range(1984, 2017))
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


def open_matlab(file):
    """
    Open a matlab formatted file and return the data structure contained
    within
    """
    return sio.loadmat(file, squeeze_me=True)


def mat_to_changemodel(t_start, t_end, t_break, category, magnitudes, change_prob):
    return change_products.ChangeModel(t_start - 366,
                                       t_end - 366,
                                       t_break - 366 if t_break > 366 else 0,
                                       category,
                                       magnitudes,
                                       change_prob)


def determine_coverage(line_num, unique_pos):
    coverage = np.ones(shape=(5000,))

    rng_min = ((line_num - 1) * 5000) + 1
    rng_max = line_num * 5000
    complete = np.arange(start=rng_min, stop=rng_max + 1)

    if not np.array_equal(complete, unique_pos):
        diff = set(complete).difference(unique_pos)

        for d in diff:
            coverage[complete == d] = 0

    return coverage


def changemap_vals(input, query_dates=QUERY_DATES):
    temp = map_template()

    data = open_matlab(input)['rec_cg']

    line = int(os.path.split(input)[-1][13:-4])

    x_locs = np.unique(data['pos'])
    for x in x_locs:
        model_locs = np.where(data['pos'] == x)[0]
        arr_pos = (x - 1) % 5000

        models = [mat_to_changemodel(data['t_start'][i],
                                     data['t_end'][i],
                                     data['t_break'][i],
                                     data['category'][i],
                                     data['magnitude'][i],
                                     data['change_prob'][i])
                  for i in model_locs]

        changedates = [change_products.changedate_val(models, qd)
                       for qd in query_dates]

        changemag = [change_products.changemag_val(models, qd)
                     for qd in query_dates]

        qa = [change_products.qa_val(models, qd)
              for qd in query_dates]

        seglength = [change_products.seglength_val(models, qd)
                     for qd in query_dates]

        lastchange = [change_products.lastchange_val(models, qd)
                      for qd in query_dates]

        for idx, qdate in enumerate(query_dates):
            year = dt.date.fromordinal(qdate).year

            temp['ChangeMap'][year][arr_pos] = changedates[idx]
            temp['ChangeMagMap'][year][arr_pos] = changemag[idx]
            temp['QAMap'][year][arr_pos] = qa[idx]
            temp['SegLength'][year][arr_pos] = seglength[idx]
            temp['LastChange'][year][arr_pos] = lastchange[idx]

    coverage = determine_coverage(line, x_locs)

    return temp, coverage


def output_line(data, coverage, output_dir, h, v):
    y_off = data.pop('y_off')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for prod in data:
        for year in data[prod]:
            ds = get_raster_ds(output_dir, prod, year, h, v)
            ds.GetRasterBand(1).WriteArray(data[prod][year].reshape(1, 5000), 0, y_off)

            ds.FlushCache()
            ds = None

    ds = get_raster_ds(output_dir, 'coverage', '', h, v)
    ds.GetRasterBand(1).WriteArray(coverage.reshape(1, 5000), 0, y_off)
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
    while True:
        if count >= kill_count:
            break

        outdata, coverage = output_q.get()

        if outdata == 'kill':
            count += 1
            continue

        LOGGER.debug('Outputting line: {0}'.format(outdata['y_off']))
        output_line(outdata, coverage, output_dir, h, v)


def multi_worker(input_q, output_q, name):
    while True:
        try:
            infile = input_q.get()

            LOGGER.debug('{} - received {}'.format(name, infile))

            if infile == 'kill':
                output_q.put(('kill', ''))
                break

            filename = os.path.split(infile)[-1]

            map_dict, coverage = changemap_vals(infile)
            map_dict['y_off'] = int(filename[13:-4]) - 1

            LOGGER.debug('{} - finished {}'.format(name, infile))
            output_q.put((map_dict, coverage))
        except Exception as e:
            LOGGER.exception('{} - exception'.format(name))
            continue


def single_run(input_dir, output_dir, h, v):
    for f in os.listdir(input_dir):
        change = changemap_vals(f)
        LOGGER.debug('Outputting line: {0}'.format(change['y_off']))
        output_line(change, output_dir, h, v)


def multi_run(input_dir, output_dir, num_procs, h, v):
    input_q = mp.Queue()
    output_q = mp.Queue()

    worker_count = num_procs - 1

    for f in os.listdir(input_dir):
        input_q.put(os.path.join(input_dir, f))

    for _ in range(worker_count):
        input_q.put('kill')

    for _ in range(worker_count):
        mp.Process(target=multi_worker, args=(input_q, output_q, _)).start()

    multi_output(output_dir, output_q, worker_count, h, v)


if __name__ == '__main__':
    if len(sys.argv) < 6:
        indir = raw_input('Input directory: ')
        outdir = raw_input('Output directory: ')
        cpu = raw_input('Number of CPU\'s: ')
        horiz = raw_input('H: ')
        vert = raw_input('V: ')
    else:
        indir = sys.argv[1]
        outdir = sys.argv[2]
        cpu = int(sys.argv[3])
        horiz = sys.argv[4]
        vert = sys.argv[5]

    if cpu < 2:
        single_run(indir, outdir, horiz, vert)
    else:
        multi_run(indir, outdir, int(cpu), int(horiz), int(vert))
