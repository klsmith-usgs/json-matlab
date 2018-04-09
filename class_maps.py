"""
Classification Maps for CCDC visualizations
"""

import os
import sys
import multiprocessing as mp
import datetime as dt
import pickle

from osgeo import gdal
import numpy as np

import geo_utils
from class_products import ClassModel, class_primary, class_secondary, conf_primary, conf_secondary, segchange, sort_models
from logger import log


CONUS_WKT = 'PROJCS["Albers",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378140,298.2569999999957,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'

MAP_NAMES = ('CoverPrim', 'CoverSec', 'CoverConfPrim', 'CoverConfSec', 'SegChange')
YEARS = tuple(i for i in range(1984, 2016))
QUERY_DATES = tuple(dt.date(year=i, month=7, day=1).toordinal()
                    for i in YEARS)


def getcolortable():
    ct = gdal.ColorTable()
    ct.SetColorEntry(0, (0, 0, 0, 0))  # Black
    ct.SetColorEntry(1, (238, 0, 0, 0))  # Red Developed
    ct.SetColorEntry(2, (171, 112, 40, 0))  # Orange Ag
    ct.SetColorEntry(3, (227, 227, 194, 0))  # Yellow Grass
    ct.SetColorEntry(4, (28, 99, 48, 0))  # Green Tree
    ct.SetColorEntry(5, (71, 107, 161, 0))  # Blue Water
    ct.SetColorEntry(6, (186, 217, 235, 0))  # Lt. Blue Wet
    ct.SetColorEntry(7, (255, 255, 255, 0))  # White Snow
    ct.SetColorEntry(8, (179, 174, 163, 0))  # Brown Barren
    ct.SetColorEntry(9, (251, 154, 153, 0))  # Pink Change

    # SegChange Values
    # Same class
    ct.SetColorEntry(11, (238, 0, 0, 0))  # Red Developed
    ct.SetColorEntry(22, (171, 112, 40, 0))  # Orange Ag
    ct.SetColorEntry(33, (227, 227, 194, 0))  # Yellow Grass
    ct.SetColorEntry(44, (28, 99, 48, 0))  # Green Tree
    ct.SetColorEntry(55, (71, 107, 161, 0))  # Blue Water
    ct.SetColorEntry(66, (186, 217, 235, 0))  # Lt. Blue Wet
    ct.SetColorEntry(77, (255, 255, 255, 0))  # White Snow
    ct.SetColorEntry(88, (179, 174, 163, 0))  # Brown Barren

    for i in range(1, 9):
        ct.SetColorEntry(i * 10, (145, 145, 145, 0))  # End of Time Series

        for j in range(1, 9):
            if i != j:
                ct.SetColorEntry(int(f'{i}{j}'), (162, 1, 255, 0))  # Different class

    return ct


COLOR_TABLE = getcolortable()


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
    key = 'h{:02d}v{:02d}_{}_{}'.format(h, v, product, year)

    file_path = os.path.join(output_dir, key + '.tif')

    if os.path.exists(file_path):
        ds = gdal.Open(file_path, gdal.GA_Update)
    else:
        ds = create_geotif(file_path, product, h, v)

        if 'Conf' not in product:
            ds.GetRasterBand(1).SetColorTable(COLOR_TABLE)

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
                             class_vals=tuple(range(0, 9)),
                             end_day=r['end_day'],
                             start_day=r['start_day'])
                  for r in result]

        models = sort_models(models)

        cl_pr = [class_primary(models, d) for d in query_dates]
        cl_sc = [class_secondary(models, d) for d in query_dates]
        conf_pr = [conf_primary(models, d) for d in query_dates]
        conf_sc = [conf_secondary(models, d) for d in query_dates]
        cl_segchg = [segchange(models, d) for d in query_dates]

        # ('CoverPrim', 'CoverSec', 'CoverConfPrim', 'CoverConfSec',
        #  'CoverFromTo')
        for idx, qdate in enumerate(query_dates):
            year = dt.date.fromordinal(qdate).year

            temp['CoverPrim'][year][row, col] = cl_pr[idx]
            temp['CoverSec'][year][row, col] = cl_sc[idx]
            temp['CoverConfPrim'][year][row, col] = conf_pr[idx]
            temp['CoverConfSec'][year][row, col] = conf_sc[idx]
            temp['SegChange'][year][row, col] = cl_segchg[idx]

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
            try:
                ds = get_raster_ds(output_dir, prod, year, h, v)
                ds.GetRasterBand(1).WriteArray(data[prod][year], x_off, y_off)

                ds = None
            except Exception as err:
                log.exception(err)


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


def main(indir, outdir, h, v, procs):
    # indir = r'C:\temp\class\results'
    # outdir = r'C:\temp\class\maps'
    # procs = 4
    # h = 5
    # v = 2

    multi_run(indir, outdir, procs, h, v)

if __name__ == '__main__':
    if len(sys.argv) < 6:
        print('Insufficient Args')

    main(sys.argv[1], sys.argv[2], int(sys.argv[3]),
         int(sys.argv[4]), int(sys.argv[5]))
