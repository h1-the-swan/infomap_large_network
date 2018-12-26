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

from h1theswan_utils.network_data import PajekFactory

INFOMAP_PATH = '/home/jporteno/code/infomap/Infomap'

def create_pajek_from_dataframe(df, column_names=['Paper_ID', 'Paper_reference_ID']):
    pjk = PajekFactory()
    for _, row in df.iterrows():
        pjk.add_edge(row[column_names[0]], row[column_names[1]])
    return pjk

def write_pajek_to_file(pjk, outfname):
    with open(outfname, 'w') as outf:
        pjk.write(outf, vertices_label='Vertices', edges_label='Arcs')

def run_infomap(path_to_pajek_file, path_to_infomap=INFOMAP_PATH, outdir='./data', infomap_args=['-t', '--seed 999'], logfname=None):
    cmd = [path_to_infomap, path_to_pajek_file, outdir]
    cmd = cmd + infomap_args

    if not logfname:
        logfname = os.path.join(outdir, "run_infomap_{:%Y%m%d%H%M%S%f}.log".format(datetime.now()))

    logger.debug("outdir for infomap: {}".format(outdir))
    logger.debug("log file for infomap: {}".format(logfname))

    logger.debug("cmd=={}".format(cmd))
    with open(logfname, 'w') as logf:
        process = subprocess.run(cmd, stdout=logf, stderr=subprocess.STDOUT)

def check_if_infomap_already_complete(logfname):
    if os.path.exists(logfname):
        with open(logfname, 'r') as f:
            if "Infomap ends" in f.read():
                return True
    return False

def convert_dataframe_to_pajek_and_run_infomap(df, column_names=['Paper_ID', 'Paper_reference_ID'], basename="PaperReferences", outdir='./data', path_to_infomap=INFOMAP_PATH, infomap_args=['-t', '--seed 999'], pajek_exists='raise', infomap_exists='replace'):
    """TODO: Docstring for convert_dataframe_to_pajek_and_run_infomap.

    :df: TODO
    :outdir: TODO
    :path_to_infomap: TODO
    :infomap_args: TODO
    :pajek_exists: one of ['raise', 'use_existing', 'replace']
    :infomap_exists: one of ['raise', 'use_existing', 'replace']
    :returns: TODO

    """
    outfname = os.path.join(outdir, "{}.net".format(basename))
    skip_pajek_conversion = False
    if os.path.exists(outfname):
        if pajek_exists.lower() == 'raise':
            raise RuntimeError("file {} already exists!".format(outfname))
        elif pajek_exists.lower() == 'replace':
            logger.debug("file {} already exists and will be overwritten.".format(outfname))
        elif pajek_exists.lower() == 'use_existing':
            logger.debug("file {} already exists. skipping pajek creation and using this file".format(outfname))
            skip_pajek_conversion = True
        else:
            raise RuntimeError('argument `pajek_exists` must be one of ["raise", "use_existing", "replace"]')

    if skip_pajek_conversion is False:
        start = timer()
        logger.debug("creating pajek...")
        pjk = create_pajek_from_dataframe(df, column_names=column_names)
        logger.debug("created pajek in {}".format(format_timespan(timer()-start)))

        start = timer()
        logger.debug("writing pajek to file: {}...".format(outfname))
        write_pajek_to_file(pjk, outfname)
        logger.debug("wrote pajek to file {} in {}".format(outfname, format_timespan(timer()-start)))

    logfname = os.path.join(outdir, "{}_infomap.log".format(basename))
    if check_if_infomap_already_complete(logfname) is True:
        if infomap_exists.lower() == 'raise':
            raise RuntimeError("infomap already complete! (see {})".format(logfname))
        elif infomap_exists.lower() == 'replace':
            logger.debug("infomap already complete (see {}), but infomap will run again anyway, and files may be replaced".format(logfname))
        elif infomap_exists.lower() == 'use_existing':
            logger.debug("infomap already complete (see {}), skipping...".format(logfname))
            return
        else:
            raise RuntimeError('argument `infomap_exists` must be one of ["raise", "use_existing", "replace"]')

    start = timer()
    logger.debug("running infomap...")
    run_infomap(outfname, path_to_infomap, outdir, infomap_args, logfname)
    logger.debug("ran infomap in {}".format(format_timespan(timer()-start)))

def split_pajek(pjk_fname):
    pjk_fname = os.path.abspath(pjk_fname)
    outfname_vertices = "{}_vertices.txt".format(os.path.splitext(pjk_fname)[0])
    outfname_edges = "{}_edges.txt".format(os.path.splitext(pjk_fname)[0])
    outf_vertices = open(outfname_vertices, 'w')
    outf_edges = open(outfname_edges, 'w')
    with open(pjk_fname, 'r') as f:
        mode = ""
        for line in f:
            if line:
                if line[0] == "*":
                    if line.lower().startswith('*v'):
                        mode = 'v'
                    else:
                        mode = 'e'
                    continue

                if mode == 'v':
                    outf_vertices.write(line)
                if mode == 'e':
                    outf_edges.write(line)

    outf_vertices.close()
    outf_edges.close()

