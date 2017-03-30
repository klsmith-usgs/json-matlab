import sys
import os
import multiprocessing as mp
import time
from functools import wraps
from logger import log

import scipy.io as sio
import numpy as np

import geo_utils
import api


BAND_NAMES = ('blue',
              'green',
              'red',
              'nir',
              'swir1',
              'swir2',
              'thermal')


def retry(func, retries=5):
    @wraps(func)
    def wrapped(*args, **kwargs):
        count = 0
        while count < retries:
            try:
                return func(*args, **kwargs)
            except Exception:
                count += 1
    return wrapped


def record_template():
    return np.zeros(1, dtype=[('t_start', 'i4'),
                              ('t_end', 'i4'),
                              ('t_break', 'i4'),
                              ('coefs', 'f4', (8, 7)),
                              ('rmse', 'f4', 7),
                              ('pos', 'i4'),
                              ('change_prob', 'i4'),
                              ('num_obs', 'i4'),
                              ('category', 'i4'),
                              ('magnitude', 'f4', 7)])


def pyordinal_to_matordinal(ord_date):
    return ord_date + 366


def save_record(outfile, record):
    sio.savemat(outfile, {'rec_cg': record})


def build_spectral(model, band_names=BAND_NAMES):
    coefs = np.zeros(shape=(8, 7))
    rmse = np.zeros(shape=(7,))
    magnitude = np.zeros(shape=(7,))

    for i, b in enumerate(band_names):
        rmse[i] = model[b]['rmse']
        magnitude[i] = model[b]['magnitude']

        coefs[0][i] = model[b]['intercept']
        for j, val in enumerate(model[b]['coefficients']):
            coefs[j + 1][i] = val

    return coefs, rmse, magnitude


@retry
def worker(args):
    try:
        output_path, h, v, line = args
        log.debug('Working on lines {} - {}'.format(line, line + 100))
        ext, affine = geo_utils.extent_from_hv(h, v)

        y = ext.y_max - line * 30

        records = []
        for x in xrange(ext.x_min, ext.x_max, 3000):
            result_chip = api.request_results(x, y)

            if result_chip is None:
                continue

            records.append(chip_to_records(result_chip, ext.x_min, ext.y_max))

        for chip in records:
            output_lines(output_path, chip)

        log.debug('Writing lines {} - {}'.format(line, line + 100))

    except Exception:
        log.exception('EXCEPTION')


def output_lines(output_path, records):
    for row in records:
        output_line(output_path, records[row], row)


def output_line(output_path, records, row):
    outfile = os.path.join(output_path, 'record_change{}.mat'.format(row))

    save_record(outfile, np.concatenate(tuple(records)))


def chip_to_records(chip, tile_ulx, tile_uly):
    ret = {}

    for result in chip:
        # + 1 for Matlab
        column = (tile_ulx + result['x']) / 30 + 1
        row = (tile_uly - result['y']) / 30 + 1

        records = result_to_records(result, column)

        if row not in ret:
            ret[row] = []

        ret[row].append(records)

    return ret


def result_to_records(result, column):
    records = []

    for model in result['change_models']:
        record = record_template()
        coefs, rmse, mags = build_spectral(model)

        record['t_start'] = pyordinal_to_matordinal(model['start_day'])
        record['t_end'] = pyordinal_to_matordinal(model['end_day'])
        record['t_break'] = pyordinal_to_matordinal(model['break_day'])
        record['coefs'] = coefs
        record['rmse'] = rmse
        record['pos'] = column
        record['change_prob'] = model['change_probability']
        record['num_obs'] = model['observation_count']
        record['category'] = model['curve_qa']
        record['magnitude'] = mags

        records.append(record)

    return records


def run(output_path, h, v, cpus, resume=True):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    pool = mp.Pool(processes=cpus)

    lines = [l for l in range(0, 5000, 100)]

    if resume is True:
        for f in os.listdir(output_path):
            line = int(f[13:-4]) - 1
            lines.remove(line)

    pool.map(worker, ((output_path, h, v, l) for l in lines))


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 5:
        output_dir = raw_input('Output directory: ')
        horiz = raw_input('ARD h: ')
        vert = raw_input('ARD v: ')
        cpu_count = raw_input('Number of CPU: ')
    else:
        output_dir = sys.argv[1]
        horiz = int(sys.argv[2])
        vert = int(sys.argv[3])
        cpu_count = int(sys.argv[4])

    run(output_dir, horiz, vert, cpu_count)
