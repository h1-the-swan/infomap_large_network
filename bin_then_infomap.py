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

def load_refs(fname, column_names=['Paper_ID', 'Paper_reference_ID']):
    refs = pd.read_table(fname, header=None, names=column_names)
    # refs[column_names[0]] = refs[column_names[0]].astype(str)
    # refs[column_names[1]] = refs[column_names[1]].astype(str)
    return refs

def get_bins(top_cluster_counts, min_cluster_size=100, bin_size=1e6):
    """TODO: Docstring for get_bins.

    :top_cluster_counts: pandas series
    :min_cluster_size: TODO
    :bin_size: TODO
    :returns: bins (list of lists of cluster names)

    """
    top_cluster_counts_subset = top_cluster_counts[top_cluster_counts>=min_cluster_size]
    bins = []
    this_bin = []
    size_this_bin = 0
    for cl, num_papers in top_cluster_counts_subset.items():
        size_this_bin += num_papers
        this_bin.append(cl)
        if size_this_bin >= bin_size:
            bins.append(this_bin)
            this_bin = []
            size_this_bin = 0
    if this_bin:
        bins.append(this_bin)
    return bins

def process_cluster_subset(cl_subset, outdir, refs, column_names=['Paper_ID', 'Paper_reference_ID'], path_to_infomap=INFOMAP_PATH, infomap_args=['-t', '-vvv', '--seed 999']):
    """TODO: Docstring for process_cluster_subset.

    :cl_subset: TODO
    :outdir: TODO
    :refs: TODO
    :column_names: TODO
    :path_to_infomap: TODO
    :infomap_args: TODO
    :returns: TODO

    """
    logger.debug("getting citations for this bin...")
    start = timer()
    refs_subset = refs[refs.cl_out.astype(str).isin(cl_subset)]
    logger.debug("identified {} citations in {}".format(len(refs_subset), format_timespan(timer()-start)))

    logger.debug("converting to pajek and running infomap...")
    start = timer()
    convert_dataframe_to_pajek_and_run_infomap(refs_subset, column_names=column_names, basename="PaperReferences_subset", outdir=outdir, path_to_infomap=path_to_infomap, infomap_args=infomap_args, pajek_exists='use_existing', infomap_exists='use_existing')
    logger.debug("converting to pajek and running infomap completed. took {}".format(format_timespan(timer()-start)))

def write_cl_subset_to_file(cl_subset, outdir):
    outfname = os.path.join(outdir, "this_bin_relaxmap_clusters.txt")
    logger.debug("writing two-level cluster names for this bin to file: {}".format(outfname))
    with open(outfname, 'w') as outf:
        for cl in cl_subset:
            outf.write("{}\n".format(cl))

def main(args):
    logger.debug("loading references from file {}...".format(args.edgelist))
    start = timer()
    refs = load_refs(args.edgelist)
    logger.debug("loaded {} references from file {}. took {}".format(len(refs), args.edgelist, format_timespan(timer()-start)))

    logger.debug("loading cluster info from file {}...".format(args.treefile))
    start = timer()
    t = Treefile(args.treefile)
    t.get_top_cluster_counts()
    cl_map = t.df.set_index('name')['top_cluster']
    logger.debug("loaded cluster info in {}".format(format_timespan(timer()-start)))

    logger.debug("assigning clusters for out-citations...")
    start = timer()
    refs['cl_out'] = refs['Paper_ID'].astype(str).map(cl_map).astype(str)
    logger.debug("assigned out-citation clusters in {}".format(format_timespan(timer()-start)))

    logger.debug("assigning clusters for in-citations...")
    start = timer()
    refs['cl_in'] = refs['Paper_reference_ID'].astype(str).map(cl_map).astype(str)
    logger.debug("assigned in-citation clusters in {}".format(format_timespan(timer()-start)))

    logger.debug("getting only the references within the same cluster (same cl_out and cl_in)...")
    start = timer()
    refs_within_cl = refs[refs['cl_out']==refs['cl_in']]
    logger.debug("identified {} within-cluster references in {}".format(len(refs_within_cl), format_timespan(timer()-start)))

    logger.debug("getting bins...")
    start = timer()
    bins = get_bins(t.top_cluster_counts, min_cluster_size=args.min_cluster_size, bin_size=args.bin_size)
    logger.debug("got {} bins ({})".format(len(bins), format_timespan(timer()-start)))

    for i, cl_subset in enumerate(bins):
        logger.debug("")
        outdir = os.path.join(args.outdir, "bin_{:02}".format(i))
        if os.path.exists(outdir):
            if args.skip_existing is True:
                logger.debug("path {} already exists. skipping this bin.".format(outdir))
                continue
            else:
                # raise RuntimeError("path {} already exists!".format(outdir))
                logger.warn("path {} already exists. continuing using this path...".format(outdir))
        else:
            os.mkdir(outdir)
        logger.debug("processing bin {}...".format(i))
        write_cl_subset_to_file(cl_subset, outdir)
        process_cluster_subset(cl_subset, outdir, refs_within_cl, path_to_infomap=INFOMAP_PATH)

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="starting with a citation edgelist and a two-level clustering, get all within-cluster citations, then run hierarchical infomap on subsets (bins)")
    parser.add_argument("edgelist", help="filename for the citation edgelist (TSV, no header)")
    parser.add_argument("treefile", help="filename for the two-level clustering of the edgelist (.tree)")
    parser.add_argument("--min-cluster-size", type=int, default=100, help="only consider clusters with this number of papers or more (default: 100 papers)")
    parser.add_argument("--bin-size", type=int, default=1e6, help="bins will be around this size (default: 1 million papers)")
    parser.add_argument("--outdir", default='./data', help="base directory for the output")
    parser.add_argument("--skip-existing", action='store_true', help="if the bin directory already exists, skip this bin")
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
