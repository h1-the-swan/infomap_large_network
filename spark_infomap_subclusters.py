import sys, os, time, tempfile, shutil
from glob import glob
from datetime import datetime
from timeit import default_timer as timer

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

logging.basicConfig(
    format="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
# logger = logging.getLogger(__name__)
logger = logging.getLogger("__main__").getChild(__name__)

import pandas as pd

# from utils import split_pajek

# from dotenv import load_dotenv, find_dotenv

# load_dotenv(find_dotenv())

# from utils import load_spark_session
# from config import Config
# config = Config()
# spark = config.load_spark_session(mem='100g', additional_conf=[("spark.sql.execution.arrow.pyspark.enabled", 'true')])
# https://stackoverflow.com/questions/58273063/pandasudf-and-pyarrow-0-15-0
# os.environ['ARROW_PRE_0_15_IPC_FORMAT'] = "1"
# actually, set this in .env file

from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
mem = "100g"
spark = (
    SparkSession.builder.appName("sparkApp")
    # .config("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
    # .config(
    #     "spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true"
    # )
    # .config(
    #     "spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true"
    # )
    .config("spark.executor.memory", mem)
    .config("spark.driver.memory", mem)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

import infomap
from pyspark.sql.functions import pandas_udf, PandasUDFType

# spark = load_spark_session(logLevel='INFO', additional_conf=[('spark.logConf', True), ('spark.driver.maxResultSize', '0'), ('spark.python.profile', True), ('spark.reducer.maxSizeInFlight', '5g'), ('spark.driver.supervise', True)])

# def same_source_and_target(r):
#     cl_source = cl_top.get(r.source)
#     cl_target = cl_top.get(r.target)
#     if cl_source and cl_target and cl_source == cl_target:
#         return cl_source
#     return None


def split_pajek(pjk_fname):
    pjk_fname = os.path.abspath(pjk_fname)
    outfname_vertices = "{}_vertices.txt".format(os.path.splitext(pjk_fname)[0])
    outfname_edges = "{}_edges.txt".format(os.path.splitext(pjk_fname)[0])
    outf_vertices = open(outfname_vertices, "w")
    outf_edges = open(outfname_edges, "w")
    with open(pjk_fname, "r") as f:
        mode = ""
        for line in f:
            if line:
                if line[0] == "*":
                    if line.lower().startswith("*v"):
                        mode = "v"
                    else:
                        mode = "e"
                    continue

                if mode == "v":
                    outf_vertices.write(line)
                if mode == "e":
                    outf_edges.write(line)

    outf_vertices.close()
    outf_edges.close()


def calc_infomap(nodes, links):
    this_infomap = infomap.Infomap(flow_model="undirdir", seed=999, silent=True)
    # network = this_infomap.network()
    for node in nodes:
        _node = this_infomap.add_node(int(node))
        # _node.disown()  # this seems to prevent an error message being sent out for every node
    for source, target in links:
        this_infomap.add_link(int(source), int(target))
    this_infomap.run()
    data = []
    for node in this_infomap.tree:
        if node.is_leaf:
            path = ":".join([str(x) for x in node.path])
            data.append((node.node_id, path))
    return data


def get_combined_infomap_cl_path(row):
    return "{}:{}".format(row["cl_top"], row["hierInfomap_cl"])


@pandas_udf(
    "node_id bigint, node_name string, cl string, cl_top string",
    PandasUDFType.GROUPED_MAP,
)
def calc_infomap_udf(pdf):
    cl_top = pdf["cl_top"].iloc[0]
    # links = [(int(row.source), int(row.target)) for _, row in pdf.iterrows()]
    nodes = pdf["node_id"].unique()
    links = pdf[["source", "target"]].dropna().to_records(index=False)
    try:
        infomap_result = calc_infomap(nodes, links)
    except RuntimeError:
        # nodes = set.union(set(pdf.source.values), set(pdf.target.values))
        infomap_result = [(x, "INFOMAP_FAILED") for x in nodes]
    return_df = pd.DataFrame(infomap_result, columns=["node_id", "hierInfomap_cl"])
    return_df = return_df.join(
        pdf.set_index("node_id")["node_name"].drop_duplicates(),
        on="node_id",
        how="left",
    )
    return_df["cl_top"] = cl_top
    return_df["cl"] = return_df.apply(get_combined_infomap_cl_path, axis=1)
    return_df = return_df.drop(columns="hierInfomap_cl")
    return return_df


@pandas_udf("string", PandasUDFType.SCALAR)
def get_cl_top(v):
    return v.apply(lambda x: x[: x.find(":")])


def main(args):
    logger.debug(f"SPARK_SUBMIT_OPTS: {os.environ.get('SPARK_SUBMIT_OPTS')}")
    # check if output path already exists
    if os.path.exists(args.out):
        raise RuntimeError("output path already exists! ({})".format(args.out))

    pjk_fname = os.path.abspath(args.pjk_file)
    fname_vertices = "{}_vertices.txt".format(os.path.splitext(pjk_fname)[0])
    fname_edges = "{}_edges.txt".format(os.path.splitext(pjk_fname)[0])
    if os.path.exists(fname_vertices) and os.path.exists(fname_edges):
        logger.debug(
            "pajek split files {} and {} already exist. using these.".format(
                fname_vertices, fname_edges
            )
        )
    else:
        start = timer()
        logger.debug(
            "splitting pajek file {} into {} and {}".format(
                pjk_fname, fname_vertices, fname_edges
            )
        )
        split_pajek(pjk_fname)
        logger.debug("done. took {}".format(format_timespan(timer() - start)))

    sdf_vertices = spark.read.csv(
        fname_vertices, sep=" ", quote='"', schema="node_id LONG, node_name STRING"
    )
    sdf_tree = spark.read.csv(
        args.tree_fname,
        sep=" ",
        quote='"',
        comment="#",
        schema="cl STRING, flow FLOAT, node_name STRING",
    )
    sdf_tree = sdf_tree.withColumn("cl_top", get_cl_top(sdf_tree["cl"]))
    # get `node_id` column
    sdf_tree = sdf_tree.join(sdf_vertices, on="node_name", how="inner")

    threshold = args.min_size
    logger.debug("excluding clusters smaller than {}...".format(threshold))
    to_keep = (
        sdf_tree.groupby("cl_top").count().filter("`count` >= {}".format(threshold))
    )
    if args.max_size:
        logger.debug("excluding clusters larger than {}...".format(args.max_size))
        to_keep = to_keep.filter("`count` <= {}".format(args.max_size))
    sdf_tree_subset = sdf_tree.join(to_keep.select("cl_top"), on="cl_top", how="inner")
    sdf_cl_top = sdf_tree_subset.select(["node_id", "cl_top"])

    sdf_edges = spark.read.csv(
        fname_edges, sep=" ", schema="source BIGINT, target BIGINT"
    )

    x = (
        sdf_edges.join(
            sdf_cl_top, on=sdf_edges["source"] == sdf_cl_top["node_id"], how="inner"
        )
        .drop("node_id")
        .withColumnRenamed("cl_top", "cl_top_source")
    )
    x = (
        x.join(sdf_cl_top, on=x["target"] == sdf_cl_top["node_id"], how="inner")
        .drop("node_id")
        .withColumnRenamed("cl_top", "cl_top_target")
    )
    x = x.filter(x["cl_top_source"] == x["cl_top_target"])
    # logger.debug("x.count(): {}".format(x.count()))

    x = x.drop("cl_top_target").withColumnRenamed("cl_top_source", "cl_top")

    # There may still be some nodes in this cluster that are not represented,
    # since they don't have any within-cluster links.
    # This next step will include these missing nodes as rows with null values.
    x = x.withColumnRenamed("cl_top", "_cl_top").join(
        sdf_tree_subset, on=x["source"] == sdf_tree_subset["node_id"], how="outer"
    )
    x = x.drop("_cl_top")

    # logger.debug("running infomap on within-cluster links, for {} top-level clusters...".format(df_tree.cl_top.nunique()))
    sdf_infomap = x.groupby("cl_top").apply(calc_infomap_udf)

    # get `flow` column
    sdf_infomap = sdf_infomap.join(
        sdf_tree_subset.select(["node_id", "flow"]), on="node_id", how="left"
    )

    # get the nodes in small clusters we excluded before. just use their original `cl` column for their cluster designation
    small_clusters = sdf_tree.join(sdf_tree_subset, on="node_id", how="left_anti")
    # at this point `sdf_infomap` and `small_clusters` should have the same schema
    # sdf_infomap = sdf_infomap.join(small_clusters, on=sdf_infomap.columns, how='outer')
    # reorder columns to match
    small_clusters = small_clusters.select(sdf_infomap.columns)
    # concatenate
    sdf_infomap = sdf_infomap.union(small_clusters)

    sdf_infomap = sdf_infomap.orderBy("node_id")
    # sdf_infomap.write.csv(args.out, sep='\t', header=True, compression='gzip')
    with tempfile.TemporaryDirectory() as dirpath:
        sdf_infomap.coalesce(1).write.csv(
            dirpath, sep="\t", header=True, mode="overwrite"
        )
        temp_fname = glob(os.path.join(dirpath, "*.csv"))[0]
        shutil.move(temp_fname, args.out)
    # logger.debug("done running infomap and writing to files. took {}".format(format_timespan(timer()-start)))


if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info("{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
    import argparse

    parser = argparse.ArgumentParser(
        description="run hierarchical infomap separately on relaxmap clusters, using spark"
    )
    parser.add_argument("pjk_file", help="pajek (.net) file for the full network")
    parser.add_argument("tree_fname", help="treefile from relaxmap")
    parser.add_argument(
        "-o", "--out", required=True, help="output filename (TSV) (required)"
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=10,
        help="ignore clusters smaller than this size (don't run infomap)",
    )
    parser.add_argument(
        "--max-size",
        type=int,
        help="ignore clusters larger than this size (don't run infomap)",
    )
    parser.add_argument("--debug", action="store_true", help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("debug mode is on")
    else:
        logger.setLevel(logging.INFO)
    main(args)
    total_end = timer()
    logger.info(
        "all finished. total time: {}".format(format_timespan(total_end - total_start))
    )
