from __future__ import division
import sys, os, time
# import multiprocessing
from multiprocessing.pool import ThreadPool
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

from h1theswan_utils.treefiles import Treefile

DEFAULTS = {
    'outdir': './data/cluster_nodes/',
    'min_cluster_size': 10,
    'max_output_files': 6000000
}

def output_to_file(counts, outfname, sep='\t'):
    series_name = counts.name
    counts.name = 'num_papers'
    counts.to_csv(outfname, header=True, sep=sep, index_label=series_name)

def get_nodes_and_write_to_file(tree, cluster_name, outfname):
    nodes = tree.get_nodes_for_cluster(cluster_name=cluster_name)
    logger.debug("writing {} nodes to {}...".format(len(nodes), outfname))
    with open(outfname, 'w') as outf:
        for node in nodes:
            outf.write("{}\n".format(node))

def main(args):
    fname = os.path.abspath(args.input)
    outdir = args.outdir
    if not os.path.isdir(outdir):
        raise RuntimeError("outdir is not a valid directory")

    treefile = Treefile(fname)
    start = timer()
    logger.debug("parsing treefile: {}...".format(fname))
    treefile.parse()
    logger.debug("done parsing treefile {}. {}".format(fname, format_timespan(timer()-start)))
    
    start = timer()
    logger.debug("getting top-level cluster counts for treefile {} (already parsed)".format(fname))
    counts = treefile.get_top_cluster_counts()
    logger.debug("done getting top-level cluster counts for treefile {}. Found {} clusters. {}".format(fname, len(counts), format_timespan(timer()-start)))

    counts = counts[counts>=args.min_cluster_size]
    logger.debug("There are {} clusters with at least {} nodes. Getting these clusters...".format(len(counts), args.min_cluster_size))
    if len(counts) > args.max_output_files:
        logger.warn("There are more than {} clusters. Will stop after {} (see help for more info)".format(args.max_output_files, args.max_output_files))

    start = timer()
    num_output_files = min(len(counts), args.max_output_files)
    zero_pad_length = len(str(num_output_files))
    pool = ThreadPool(10)
    for i, (cluster_name, cluster_size) in enumerate(counts.iteritems()):
        if i > args.max_output_files:
            break
        outfname = os.path.basename(fname)
        outfname = os.path.splitext(outfname)[0]
        zero_pad_i = "{number:0{width}d}".format(width=zero_pad_length, number=i)
        outfname = zero_pad_i + "_" + outfname + "_cluster-{}.txt".format(cluster_name)
        outfname = os.path.join(outdir, outfname)
        get_nodes_args = (treefile, cluster_name, outfname)
        pool.apply_async(get_nodes_and_write_to_file, args=get_nodes_args)
    pool.close()
    pool.join()
    logger.debug("done writing the files. {}".format(format_timespan(timer()-start)))

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="for the bigger clusters, get a list of the nodes in each cluster and output to a separate text file per cluster")
    parser.add_argument("input", help="input treefile (.tree)")
    parser.add_argument("--outdir", default=DEFAULTS['outdir'], help="output directory. default: {}".format(DEFAULTS['outdir']))
    parser.add_argument("--min-cluster-size", type=int, default=DEFAULTS['min_cluster_size'], help="only clusters with at least this many nodes will be considered. default: {}".format(DEFAULTS['min_cluster_size']))
    parser.add_argument("--max-output-files", type=int, default=DEFAULTS['max_output_files'], help="stop after this many output files have been written (failsafe). default: {}".format(DEFAULTS['max_output_files']))
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

