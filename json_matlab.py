import os
import multiprocessing as mp
from logger import log
import json
from functools import partial

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
    sio.savemat(outfile, {'rec_cg': record}, do_compression=True)


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


def worker(output_path, input_path, h, v, alg, line):
    # output_path, input_path, h, v, alg, line = args
    log.debug('Received lines beginning at {}'.format(line))
    ext, affine = geo_utils.extent_from_hv(h, v)

    y = ext.y_max - line * 30

    records = tuple()
    for x in xrange(ext.x_min, ext.x_max, 3000):
        log.debug('Requesting chip x: {} y: {}'.format(x, y))

        try:
            result_chip = get_data(input_path, h, v, x, y, alg)
        except:
            log.exception('EXCEPTION')
            continue

        if result_chip is None or len(result_chip) == 0:
            log.debug('Received no results for chip x: {} y: {}'
                      .format(x, y))
            continue

        log.debug('Received {} results for chip x: {} y: {}'
                  .format(len(result_chip), x, y))

        records += (chip_to_records(result_chip, ext.x_min, ext.y_max),)
        log.debug('Record chip accumulation: {}'.format(len(records)))

    log.debug('Outputting lines starting from: {}'.format(line))
    output_lines(output_path, compress_record_chips(records))

    return True


def get_data(input_path, h, v, x, y, alg):
    """
    Return chip results from either the api, or from files. Depends on whether
    input_path is not None.
    """
    if input_path:
        return fetch_file_results(input_path, h, v, x, y)
    else:
        return api.fetch_results_chip(x, y, alg)


def fetch_file_results(dir, h, v, x, y):
    """
    Create a dictionary from a JSON file matching a certain naming convention. 
    """
    filename = 'H{:02d}V{:02d}_{}_{}.json'.format(h, v, x, y)
    filepath = os.path.join(dir, filename)

    with open(filepath, 'r') as f:
        return json.load(f)


def compress_record_chips(record_chips):
    ret = {}

    for chip in record_chips:
        for row, val in chip.items():
            if row in ret:
                ret[row].extend(val)
            else:
                ret[row] = val

    return ret


def output_lines(output_path, records):
    for row in records:
        output_line(output_path, records[row], row)


def output_line(output_path, records, row):
    outfile = os.path.join(output_path, 'record_change{}.mat'.format(row))

    save_record(outfile, np.concatenate(records))


def chip_to_records(chip, tile_ulx, tile_uly):
    """
    Move through the LCMAP results chip and change to a dictionary of tuples
    containing numpy structures.
    
    Returns dictionary keyed row.
    """
    ret = {}

    for result in chip:
        if result.get('result_ok') is True:
            models = json.loads(result['result'])
        else:
            continue

        # + 1 for Matlab
        row = (tile_uly - int(result['y'])) / 30 + 1
        # column is expected to be a continuous value, as if the extent was
        # a flattened array
        pos = ((int(result['x'] - tile_ulx)) / 30 + 1) + (row - 1) * 5000

        records = result_to_records(models, pos)

        if row not in ret:
            ret[row] = tuple()

        ret[row] += (records,)

    return ret


def result_to_records(models, pos):
    records = tuple()

    for model in models['change_models']:
        record = record_template()
        coefs, rmse, mags = build_spectral(model)

        record['t_start'] = pyordinal_to_matordinal(model['start_day'])
        record['t_end'] = pyordinal_to_matordinal(model['end_day'])
        record['t_break'] = pyordinal_to_matordinal(model['break_day'])
        record['coefs'] = coefs
        record['rmse'] = rmse
        record['pos'] = pos
        record['change_prob'] = model['change_probability']
        record['num_obs'] = model['observation_count']
        record['category'] = model['curve_qa']
        record['magnitude'] = mags

        records += (record,)

    return records


def run(output_path, h, v, alg, cpus, input_path, resume=True):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    pool = mp.Pool(processes=cpus)

    lines = [l for l in range(0, 5000, 100)]

    # Disabled for now
    if resume is True:
        pass
        # for f in os.listdir(output_path):
        #     line = int(f[13:-4]) - 1
        #     if not line % 100:
        #         lines.remove(line)

    func = partial(worker, output_path, input_path, h, v, alg)

    success = pool.map(func, lines)

    log.debug('Successful workers: {}'.format(np.sum(success)))
#
#
# if __name__ == '__main__':
#     if len(sys.argv) < 2 or len(sys.argv) > 5:
#         output_dir = raw_input('Output directory: ')
#         input_dir = raw_input('Output directory: ')
#         horiz = raw_input('ARD h: ')
#         vert = raw_input('ARD v: ')
#         cpu_count = raw_input('Number of CPU: ')
#     else:
#         output_dir = sys.argv[1]
#         horiz = int(sys.argv[2])
#         vert = int(sys.argv[3])
#         cpu_count = int(sys.argv[4])
#
#     run(output_dir, horiz, vert, cpu_count)
