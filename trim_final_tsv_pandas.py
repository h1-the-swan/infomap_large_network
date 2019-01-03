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

def main(args):
    if os.path.exists(args.output):
        raise RuntimeError('output path already exists! ({})'.format(args.output))
    logger.debug("reading input file: {} ...".format(args.input))
    df = pd.read_csv(args.input, sep='\t', index_col=0)
    columns_to_drop = [
        'cl',
        'hierInfomap_cl',
    ]
    logger.debug("dropping columns: {} ...".format(columns_to_drop))
    df = df.drop(columns=columns_to_drop)

    columns_rename = {
        'node_id': 'id',
        'node_name': 'UID',
        'cl_combiuned': 'cl',
    }
    logger.debug("renaming columns: {} ...".format(columns_rename))
    df = df.rename(columns=columns_rename)
    

    logger.debug("writing to {} ...".format(args.output))
    df.to_csv(args.output, sep='\t')

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="trim unnecessary columns from the final tree TSV (using pandas)")
    parser.add_argument("input", help="input TSV filename")
    parser.add_argument("output", help="output TSV filename")
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

