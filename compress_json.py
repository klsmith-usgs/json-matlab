import json
import os
import multiprocessing as mp
from logger import log
from functools import partial
import sys

import numpy as np


def run(input_path, output_path, cpus):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    pool = mp.Pool(processes=cpus)

    files = os.listdir(input_path)

    pool.map(worker, ((f, output_path) for f in files))


def worker(file_path, output_path):
    result_chip = get_data(file_path)
    filename = os.path.split(file_path)[-1]
    log.debug('Working file: {}'.format(filename))

    if result_chip is None or len(result_chip) == 0:
        return

    outls = [simplify_mask(result) for result in result_chip]

    write_json(outls, os.path.join(output_path, filename))


def get_data(path):
    with open(path, 'r') as f:
        return json.load(f)


def simplify_mask(result):
    if result.get('result_ok') is True:
        models = json.loads(result['result'])
    else:
        return result

    models['processing_mask'] = [int(b) for b in models['processing_mask']]

    return json.dumps(models)


def write_json(data, output_path):
    with open(output_path, 'wr') as f:
        f.write(json.dumps(data))


if __name__ == '__main__':
    indir = sys.argv[1]
    outdir = int(sys.argv[2])
    cpu_count = int(sys.argv[3])

    run(indir, outdir, cpu_count)
