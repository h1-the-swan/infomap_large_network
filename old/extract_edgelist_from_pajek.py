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

# import pandas as pd


def extract_edgelist_from_pajek(fname, outfname, sep='\t'):
    outf = open(outfname, 'w')
    with open(fname, 'r') as f:
        mode = None
        rows = []
        for line in f:
            if line[0] == '*':
                line = line.lower()
                if 'vert' in line:
                    mode = 'v'
                if 'arc' in line or 'edge' in line:
                    mode = 'e'
                continue
            
            if mode == 'e':
                line = line.strip()
                d = [int(x) for x in line.split(" ")]
                # rows.append(d)
                outf.write("{}{}{}\n".format(d[0], sep, d[1]))
    outf.close()


def main(args):
    logger.debug("extracting links from {} and writing to {}...".format(args.pajek, args.out))
    extract_edgelist_from_pajek(args.pajek, args.out)

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="starting with a citation edgelist and a two-level clustering, get all within-cluster citations, then run hierarchical infomap on subsets (bins)")
    parser.add_argument("pajek", help="filename for the citation pajek (.net)")
    parser.add_argument("out", help="filename for the output (TSV)")
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

