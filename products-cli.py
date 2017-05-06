import argparse
import change_maps as cm


parser = argparse.ArgumentParser(
        description='Create annual change products from Matlab '
                    'formatted files.')

parser.add_argument('input', help='Input location of Matlab files.')
parser.add_argument('output', help='Output location to for the products.')
parser.add_argument('h', help='ARD Grid h value.', type=int)
parser.add_argument('v', help='ARD Grid v value.', type=int)
parser.add_argument('-p', '--proc',
                    help='Number of child processes to use.',
                    default=1, type=int, metavar='')

args = parser.parse_args()

if args.proc < 2:
    cm.single_run(args.input, args.output, args.h, args.v)
else:
    cm.multi_run(args.input, args.output, args.proc, args.h, args.v)
