import argparse
import json_matlab as jm


parser = argparse.ArgumentParser(
        description='Create Matlab formatted CCD files for TSTools '
                    'consumption. By default it will attempt to pull results '
                    'from the LCMAP API. If --input is specified, then it '
                    'will attempt to read JSON files from the given '
                    'directory. These file names must be formatted '
                    'H##V##_<X coord>_<Y coord>.json')

parser.add_argument('output', help='Output location to for the Matlab files.')
parser.add_argument('h', help='ARD Grid h value.', type=int)
parser.add_argument('v', help='ARD Grid v value.', type=int)
parser.add_argument('algorithm', help='Algorithm version to request')
parser.add_argument('-i', '--input',
                    help='Directory location of JSON files to read.',
                    default=None,
                    metavar='')
parser.add_argument('-p', '--proc',
                    help='Number of child processes to use.',
                    default=1, type=int, metavar='')

args = parser.parse_args()

jm.run(args.output, args.h, args.v, args.algorithm, args.proc, args.input)
# run(output_dir, horiz, vert, cpu_count)
