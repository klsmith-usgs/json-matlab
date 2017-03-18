import sys
import os
import multiprocessing as mp

import scipy.io as sio

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
    return {'t_start': [],
            't_end': [],
            't_break': [],
            'coefs': [],
            'rmse': [],
            'pos': [],
            'change_prob': [],
            'num_obs': [],
            'category': [],
            'magnitude': []}


def pyordinal_to_matordinal(ord_date):
    return ord_date + 366


def save_record(outfile, record):
    sio.savemat(outfile, {'rec_cg': record})


def build_spectral(model, band_names=BAND_NAMES):
    coefs = [[0, 0, 0, 0, 0, 0, 0],
             [0, 0, 0, 0, 0, 0, 0],
             [0, 0, 0, 0, 0, 0, 0],
             [0, 0, 0, 0, 0, 0, 0],
             [0, 0, 0, 0, 0, 0, 0],
             [0, 0, 0, 0, 0, 0, 0],
             [0, 0, 0, 0, 0, 0, 0],
             [0, 0, 0, 0, 0, 0, 0]]
    rmse = []
    magnitude = []

    for i, b in enumerate(band_names):
        rmse.append(model[b]['rmse'])
        magnitude.append(model[b]['magnitude'])

        coefs[0][i] = model[b]['intercept']
        for j, val in enumerate(model[b]['coefficients']):
            coefs[j + 1][i] = val

    return coefs, rmse, magnitude


def worker(args):
    output_path, h, v, line = args
    ext, affine = geo_utils.extent_from_hv(h, v)

    y = ext.y_max - line * 30
    outfile = os.path.join(output_path, 'record_change{}.mat'.format(line + 1))
    record = record_template()

    sample = 5000 * line
    for x in xrange(ext.x_min, ext.x_max, 30):
        sample += 1
        results = api.request_results(x, y)

        if results is None:
            continue

        for model in results['change_models']:
            coefs, rmse, mags = build_spectral(model)

            record['t_start'].append(pyordinal_to_matordinal(model['start_day']))
            record['t_end'].append(pyordinal_to_matordinal(model['end_day']))
            record['t_break'].append(pyordinal_to_matordinal(model['break_day']))
            record['coefs'].append(coefs)
            record['rmse'].append(rmse)
            record['pos'].append(sample)
            record['change_prob'].append(model['change_probability'])
            record['num_obs'].append(model['observation_count'])
            record['category'].append(model['curve_qa'])
            record['magnitude'].append(mags)

    save_record(outfile, record)


def run(output_path, h, v, cpu_count):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    pool = mp.Pool(processes=cpu_count)

    pool.map(worker, ((output_path, h, v, x) for x in range(5000)))

    # ext, affine = geo_utils.extent_from_hv(h, v)
    #
    # line = 0
    # sample = 0
    # for y in xrange(ext.y_max, ext.y_min, -30):
    #     line += 1
    #     outfile = os.path.join(output_path, 'record_change{}.mat'.format(line))
    #     record = record_template()
    #
    #     for x in xrange(ext.x_min, ext.x_max, 30):
    #         sample += 1
    #         results = api.request_results(x, y)
    #
    #         if results is None:
    #             continue
    #
    #         for model in results['change_models']:
    #             coefs, rmse, mags = build_spectral(model)
    #
    #             record['t_start'].append(model['start_day'])
    #             record['t_end'].append(model['end_day'])
    #             record['t_break'].append(model['break_day'])
    #             record['coefs'].append(coefs)
    #             record['rmse'].append(rmse)
    #             record['pos'].append(sample)
    #             record['change_prob'].append(model['change_probability'])
    #             record['num_obs'].append(model['observation_count'])
    #             record['category'].append(model['curve_qa'])
    #             record['magnitude'].append(mags)
    #
    #     save_mat(outfile, record)


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 5:
        output_dir = raw_input('Output directory: ')
        h = raw_input('ARD h: ')
        v = raw_input('ARD v: ')
        cpu_count = raw_input('Number of CPU: ')
    else:
        output_dir = sys.argv[1]
        h = int(sys.argv[2])
        v = int(sys.argv[3])
        cpu_count = int(sys.argv[4])

    run(output_dir, h, v, cpu_count)
