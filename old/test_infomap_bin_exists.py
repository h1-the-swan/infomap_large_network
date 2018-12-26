import sys, os, time
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

import pandas as pd

from h1theswan_utils.treefiles import Treefile

from utils import convert_dataframe_to_pajek_and_run_infomap

INFOMAP_PATH = '/home/jporteno/code/infomap/Infomap'

bin_dir = './data/bins_infomap_runs/bin_00/'

def main(args):
    convert_dataframe_to_pajek_and_run_infomap(None, basename="PaperReferences_subset", outdir=bin_dir, path_to_infomap=INFOMAP_PATH, infomap_args=['-t', '-vvv', '--seed 999'], pajek_exists='use_existing')

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="starting with a citation edgelist and a two-level clustering, get all within-cluster citations, then run hierarchical infomap on subsets (bins)")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    else:
        logger.setLevel(logging.INFO)
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))
