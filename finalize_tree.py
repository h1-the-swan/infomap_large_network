import sys, os, time
from glob import glob
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

def get_combined_infomap_cl_path(row):
    if pd.isna(row['hierInfomap_cl']):
        return row['cl']
    else:
        return "{}:{}".format(row['cl_top'], row['hierInfomap_cl'])

def main(args):
    start = timer()
    fname = os.path.abspath(args.relaxmap)
    logger.debug("loading relaxmap data from {} ...".format(fname))
    df_tree = pd.read_csv(fname, sep=' ', comment='#', quotechar='"', names=['cl', 'flow', 'node_name'], dtype={'node_name': str})
    logger.debug("done. dataframe has {} rows. took {}".format(len(df_tree), format_timespan(timer()-start)))

    start = timer()
    dirname = os.path.abspath(args.infomap)
    logger.debug("loading infomap data from {} ...".format(dirname))
    fnames = glob(os.path.join(dirname, '*.csv*'))
    df = pd.concat(pd.read_csv(fname, sep='\t') for fname in fnames)
    df = df.set_index('node_id')
    logger.debug("done. dataframe has {} rows. took {}".format(len(df), format_timespan(timer()-start)))

    start = timer()
    fname = os.path.abspath(args.vertices)
    logger.debug("loading vertices data from {} ...".format(fname))
    df_vertices = pd.read_csv(fname, sep=' ', quotechar='"', names=['id', 'node_name'], dtype={'id': int, 'node_name': str})
    logger.debug("done. dataframe has {} rows. took {}".format(len(df_vertices), format_timespan(timer()-start)))

    start = timer()
    logger.debug("joining data...")
    nodename_to_id = df_vertices.set_index('node_name')['id'].to_dict()
    df_tree['node_id'] = df_tree.node_name.map(nodename_to_id)
    df_tree = df_tree.set_index('node_id')
    df_tree['cl_top'] = df_tree.cl.apply(lambda x: x[:x.find(':')])

    df_tree = df_tree.join(df.drop(columns='cl_top'), how='left')

    df_tree['cl_combined'] = df_tree.apply(get_combined_infomap_cl_path, axis=1)
    logger.debug("done. took {}".format(format_timespan(timer()-start)))

    start = timer()
    outfname = os.path.abspath(args.out)
    logger.debug("writing to TSV file: {}".format(outfname))
    df_tree.to_csv(outfname, sep='\t')
    logger.debug("done writing output. took {}".format(format_timespan(timer()-start)))

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="put together the tree data from the relaxmap output and the spark infomap output")
    parser.add_argument("relaxmap", help="treefile with relaxmap output")
    parser.add_argument("infomap", help="directory with infomap csv files")
    parser.add_argument("vertices", help="filename with vertices")
    parser.add_argument("-o", "--out", required=True, help="output filename (TSV)")
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
