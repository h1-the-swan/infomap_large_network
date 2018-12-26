import sys, os, time
import subprocess
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

# from dotenv import load_dotenv
# load_dotenv('admin.env')
#
# from mysql_connect import get_db_connection
# db = get_db_connection('mag_20180329')

from h1theswan_utils.network_data import PajekFactory

INFOMAP_PATH = '/home/jporteno/code/infomap/Infomap'

def main(args):
    start=timer()
    fname = 'data/PaperReferences_academicgraphdls_20180329_withinCluster.tsv'
    logger.debug("loading {}...".format(fname))
    refs_within_cl = pd.read_table(fname)
    logger.debug("loaded {} in {}. {} rows".format(fname, format_timespan(timer()-start), len(refs_within_cl)))

    start = timer()
    logger.debug("creating pajek...")
    pjk = PajekFactory()
    for _, row in refs_within_cl.iterrows():
        pjk.add_edge(row.Paper_ID, row.Paper_reference_ID)
    logger.debug(format_timespan(timer()-start))

    start = timer()
    outfname = 'data/PaperReferences_academicgraphdls_20180329_withinCluster.net'
    logger.debug("writing pajek to file: {}...".format(outfname))
    with open(outfname, 'w') as outf:
        pjk.write(outf, vertices_label='Vertices', edges_label='Arcs')
    logger.debug(format_timespan(timer()-start))

    logger.debug("running infomap...")
    cmd = [INFOMAP_PATH, outfname, './data', '-t', '-vvv', '--seed 999']

    with open('data/PaperReferences_academicgraphdls_20180329_withinCluster_infomap.log', 'w') as logf:
            process = subprocess.run(cmd, stdout=logf, stderr=subprocess.STDOUT)

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="convert to pajek and run infomap")
    parser.add_argument("--min-cluster-size", default=0, help="minimum cluster size to include")
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
