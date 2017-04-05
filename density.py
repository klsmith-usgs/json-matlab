import os
import sys
import multiprocessing as mp

from osgeo import gdal
import numpy as np

import change_maps as cm
from logger import log
import geo_utils


def worker(files):
    log.debug('Reading file {}'.format(files))

    ret = np.zeros(shape=(5000, 5000), dtype=bool)
    for f in files:
        ds = geo_utils.get_raster_ds(f)
        band = ds.GetRasterBand(8)
        arr = band.ReadAsArray()

        ret[(arr == 0) | (arr == 1)] = 1

    return ret


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


def input_queue(indir):
    fqueue = {}

    for root, dirs, files in os.walk(indir):
        for f in files:
            if f[-8:] == 'MTLstack':
                jdate = date_from_filename(f)

                if jdate not in fqueue:
                    fqueue[jdate] = tuple()

                fqueue[jdate] += (os.path.join(root, f),)

    return fqueue


def reduce_results(accum, inarr):
    accum = accum + inarr

    return accum


def run(indir, output_dir, h, v, cpus):

    log.debug('Queueing files')
    queue = input_queue(indir)

    log.debug('Number of files queued: {}'.format(len(queue)))

    pool = mp.Pool(processes=cpus)
    res_it = pool.map_async(worker, (q for q in queue))

    reduced = reduce(reduce_results, res_it)

    log.debug('Outputting map')
    density_map(reduced, output_dir, h, v)


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 6:
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