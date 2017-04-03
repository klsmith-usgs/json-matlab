import logging
import sys

log = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(processName)s: %(message)s')

handler.setFormatter(formatter)

log.addHandler(handler)
log.setLevel(logging.DEBUG)
