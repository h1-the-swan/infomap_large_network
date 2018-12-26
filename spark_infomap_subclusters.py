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
from utils import split_pajek
from utils import load_spark_session
import infomap
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = load_spark_session()

def same_source_and_target(r):
    cl_source = cl_top.get(r.source)
    cl_target = cl_top.get(r.target)
    if cl_source and cl_target and cl_source == cl_target:
        return cl_source
    return None

def calc_infomap(links):
    this_infomap = infomap.Infomap('-t --seed 999')
    network = this_infomap.network()
    for source, target in links:
        network.addLink(source, target)
    this_infomap.run()
    data = []
    for node in this_infomap.iterTree():
        if node.isLeaf():
            path = ":".join([str(x) for x in node.path()])
            data.append((node.physicalId, path))
    return data

@pandas_udf("cl_top string, node_id long, hierInfomap_cl string", PandasUDFType.GROUPED_MAP)
def calc_infomap_udf(pdf):
    cl_top = pdf['cl_top'].iloc[0]
    links = [(int(row.source), int(row.target)) for _, row in pdf.iterrows()]
    infomap_result = calc_infomap(links)
    return_df = pd.DataFrame(infomap_result, columns=['node_id', 'hierInfomap_cl'])
    return_df['cl_top'] = cl_top
    return return_df


def main(args):
    pjk_fname = os.path.abspath(args.pjk_file)
    fname_vertices = "{}_vertices.txt".format(os.path.splitext(pjk_fname)[0])
    fname_edges = "{}_edges.txt".format(os.path.splitext(pjk_fname)[0])
    if os.path.exists(fname_vertices) and os.path.exists(fname_edges):
        logger.debug("pajek split files {} and {} already exist. using these.".format(fname_vertices, fname_edges))
    else:
        start = timer()
        logger.debug('splitting pajek file {} into {} and {}'.format(pjk_fname, fname_vertices, fname_edges))
        split_pajek(pjk_fname)
        logger.debug("done. took {}".format(format_timespan(timer()-start)))

    start = timer()
    logger.debug("loading and preparing data...")
    sdf_edges = spark.read.csv(fname_edges, sep=' ', schema="source BIGINT, target BIGINT")
    df_vertices = pd.read_csv(fname_vertices, sep=' ', quotechar='"', names=['id', 'node_name'], dtype={'id': int, 'node_name': str})
    nodename_to_id = df_vertices.set_index('node_name')['id'].to_dict()
    df_tree = pd.read_csv(args.tree_fname, sep=' ', comment='#', quotechar='"', names=['cl', 'flow', 'node_name'], dtype={'node_name': str})
    df_tree['id'] = df_tree.node_name.map(nodename_to_id)
    df_tree['cl_top'] = df_tree.cl.apply(lambda x: x[:x.find(':')])
    logger.debug("done. took {}".format(format_timespan(timer()-start)))

    start = timer()
    threshold = args.min_size
    logger.debug("excluding clusters smaller than {}...".format(threshold))
    vc_cl_top = df_tree.cl_top.value_counts()
    cl_to_keep = vc_cl_top[vc_cl_top>=threshold].index
    df_tree = df_tree[df_tree.cl_top.isin(cl_to_keep)]

    if args.max_size:
        logger.debug("excluding clusters larger than {}...".format(args.max_size))
        cl_to_keep = vc_cl_top[vc_cl_top<=args.max_size].index
        df_tree = df_tree[df_tree.cl_top.isin(cl_to_keep)]
    logger.debug("done. took {}".format(format_timespan(timer()-start)))

    start = timer()
    logger.debug("running infomap on within-cluster links, for {} top-level clusters...".format(df_tree.cl_top.nunique()))
    global cl_top
    cl_top = df_tree.set_index('id')['cl_top']
    x = sdf_edges.rdd.map(lambda x: (same_source_and_target(x), x.source, x.target)).filter(lambda x: x[0] is not None)
    sdf_x = x.toDF(['cl_top', 'source', 'target'])
    # sdf_infomap = sdf_x.groupby('cl_top').apply(calc_infomap_udf)
    # sdf_infomap.write.csv(args.out, sep='\t', header=True, compression='gzip')
    # logger.debug("done running infomap and writing to files. took {}".format(format_timespan(timer()-start)))

    logger.debug("writing subcluster edgelists to {}".format(args.out))
    sdf_x.write.partitionBy('cl_top').csv(args.out, sep='\t', header=True, compression='gzip')
    logger.debug("done. took {}".format(format_timespan(timer()-start)))

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="run hierarchical infomap separately on relaxmap clusters, using spark")
    parser.add_argument("pjk_file", help="pajek (.net) file for the full network")
    parser.add_argument("tree_fname", help="treefile from relaxmap")
    parser.add_argument("-o", "--out", required=True, help="outdirectory for spark csv (tab-separated) (required)")
    parser.add_argument("--min-size", type=int, default=10, help="ignore clusters smaller than this size (don't run infomap)")
    parser.add_argument("--max-size", type=int, help="ignore clusters larger than this size (don't run infomap)")
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
