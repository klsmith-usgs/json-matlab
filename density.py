import os
import sys
import multiprocessing as mp

from osgeo import gdal
import numpy as np

import change_maps as cm
from logger import log
import geo_utils


def worker(file):
    log.debug('Reading file {}'.format(file))

    ret = np.zeros(shape=(5000, 5000), dtype=bool)
    ds = geo_utils.get_raster_ds(file)
    band = ds.GetRasterBand(8)
    arr = band.ReadAsArray()

    ret[(arr == 0) | (arr == 1)] = 1
    date = date_from_filename(os.path.split(file)[-1])

    return date, ret


def date_from_filename(filename):
    return int(filename[9:16])


def density_map(array, outdir, h, v):
    log.debug('Outputting density map')
    outfile = os.path.join(outdir, 'density.tif')
    # out_arr = np.zeros(shape=(5000, 5000), dtype=np.int)
    #
    # for arr in arrays:
    #     out_arr += arr

    ds = cm.create_geotif(outfile, 'ChangeMap', h, v)
    band = ds.GetRasterBand(1)
    band.WriteArray(array)

    band = None
    ds = None


def reduce_results(results):
    ret = np.zeros(shape=(5000, 5000), dtype=int)

    dates, arrs = zip(*results)

    dates = np.array(dates)
    arrs = np.array(arrs, dtype=bool)

    uniq, counts = np.unique(dates, return_counts=True)

    for val in uniq:
        if len(dates[dates == val]) > 1:
            blah = np.zeros(shape=(5000, 5000), dtype=bool)

            for i in np.where(dates == val)[0]:
                blah[arrs[i]] = 1

            ret += blah
        else:
            ret += arrs[dates == val]

    return ret


def run(indir, output_dir, h, v, cpus):
    queue = []

    for root, dirs, files in os.walk(indir):
        for f in files:
            if f[-8:] == 'MTLstack':
                queue.append(os.path.join(root, f))

    pool = mp.Pool(processes=cpus)

    res = pool.map(worker, queue)

    reduced = reduce_results(res)

    density_map(reduced, output_dir, h, v)


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 5:
        input_dir = raw_input('Input directory: ')
        output_dir = raw_input('Output directory: ')
        horiz = raw_input('ARD h: ')
        vert = raw_input('ARD v: ')
        cpu_count = raw_input('Number of CPU: ')
    else:
        input_dir = sys.argv[1]
        output_dir = sys.argv[2]
        horiz = int(sys.argv[3])
        vert = int(sys.argv[4])
        cpu_count = int(sys.argv[5])

    run(input_dir, output_dir, horiz, vert, cpu_count)