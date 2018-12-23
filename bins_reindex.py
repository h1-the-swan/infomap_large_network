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

from h1theswan_utils.treefiles import Treefile

class ConsolidateTrees(object):

    """Consolidate multiple tree files by assigning unique top-level clusters"""

    def __init__(self):
        self.d = []
        self.top_cl_increment = 0  # it will increment the first time, so it will start at 1
        self._last_top_cl = None  # the last top-level cluster encountered (before reassignment)

    def process_tree(self, tree_fname):
        """For each row in a tree file, assign a new top level cluster, and append it to self.d

        :tree_fname: tree filename
        :returns: self

        """
        t = Treefile(tree_fname)
        t.parse()
        i = 0
        for row in t.d:
            cl_split = row['path'].split(t.cluster_sep)

            # check if we should increment new top_cl
            # assumes that the rows in the tree file are in the proper order
            old_top_cl = int(cl_split[0])
            # increment if it's the first one in this tree file, or if there is a different top_cl
            if (i == 0) or (old_top_cl != self._last_top_cl):
                self.top_cl_increment += 1
            self._last_top_cl = old_top_cl

            new_cl_split = [str(self.top_cl_increment)] + cl_split[1:]
            new_cl = t.cluster_sep.join(new_cl_split)
            self.d.append((
                row['name'],
                new_cl,
                self.top_cl_increment
            ))
            i += 1
        return self


    def write_to_file(self, outfname, sep='\t'):
        """Write the cluster data to a text file (no header)

        :outfname: output filename
        :sep: delimiter for output (default: tab)
        :returns: TODO

        """
        with open(outfname, 'w') as outf:
            for row in self.d:
                row = [str(x) for x in row]
                outf.write(sep.join(row))
                outf.write("\n")


def main(args):
    # check that output file doesn't already exist
    if os.path.exists(args.out):
        raise RuntimeError("specified output file {} already exists!".format(args.out))

    # check that the base directory for inputs does exist
    if not os.path.exists(args.datadir):
        raise RuntimeError("specified data directory for input does not exist! ({})".format(args.datadir))
    g = glob(os.path.join(args.datadir, "bin_*/"))
    g.sort()
    logger.debug("{} bin directories found".format(len(g)))
    consolidate_trees = ConsolidateTrees()
    start_consolidate = timer()
    num_processed = 0
    for bin_dir in g:
        tree_fname = glob(os.path.join(bin_dir, '*.tree'))
        if len(tree_fname) == 1:
            start = timer()
            tree_fname = tree_fname[0]
            consolidate_trees.process_tree(tree_fname)
            logger.debug("processed tree file {} in {}. {} rows total so far".format(tree_fname, format_timespan(timer()-start), len(consolidate_trees.d)))
            num_processed += 1
        elif len(tree_fname) > 1:
            logger.error("more than one tree file in {}! skipping.".format(bin_dir))
        else:
            logger.debug("no tree file found in {}. skipping.".format(bin_dir))

    logger.debug("processed {} tree files in {}".format(num_processed, format_timespan(timer()-start_consolidate)))

    start = timer()
    logger.debug("writing output to file: {}".format(args.out))
    consolidate_trees.write_to_file(args.out)
    logger.debug("wrote output to file {}. took {}".format(args.out, format_timespan(timer()-start)))

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="for binned infomap runs, consolidate all of them, and assign a unique toplevel cluster name. output to TSV file")
    parser.add_argument("datadir", help="base directory for the bin data")
    parser.add_argument("out", help="output filename (TSV)")
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
