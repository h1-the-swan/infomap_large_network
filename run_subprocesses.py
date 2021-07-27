# -*- coding: utf-8 -*-

DESCRIPTION = """Run both steps of the clustering. First run RelaxMap in a subprocess. Then run hierarchical (Spark) clustring in another subprocess."""

import sys, os, time, subprocess
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
from typing import Union

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)


def subprocess_relaxmap(
    fpath: Union[str, Path], outfp: Union[str, Path], processes: int
) -> subprocess.CompletedProcess:
    relaxmap_executable = "/RelaxMap/ompRelaxmap"
    # RelaxMap command line arguments. See RelaxMap README
    relaxmap_args = [
        "1",
        str(fpath),
        str(processes),
        "1",
        "1e-4",
        "0.0",
        "10",
        str(outfp),
        "prior",
    ]
    logger.debug([relaxmap_executable] + relaxmap_args)
    p = subprocess.run([relaxmap_executable] + relaxmap_args)
    return p


def main(args):
    logger.debug("running RelaxMap")
    logger.debug(f"input: {args.input}")
    logger.debug(f"output: {args.output}")
    logger.debug(f"processes: {args.processes}")
    p = subprocess_relaxmap(args.input, args.output, args.processes)
    logger.debug(f"done. returncode: {p.returncode}")


if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info("{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
    logger.info("pid: {}".format(os.getpid()))
    import argparse

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("input", help="path to input pajek (.net) file")
    parser.add_argument("output", help="path to output directory")
    parser.add_argument(
        "processes", type=int, help="number of processes to use in parallel"
    )
    parser.add_argument("--debug", action="store_true", help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug("debug mode is on")
    main(args)
    total_end = timer()
    logger.info(
        "all finished. total time: {}".format(format_timespan(total_end - total_start))
    )
