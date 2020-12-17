"""Main module."""

from io import BytesIO
import logging
import multiprocessing as mp
import os
import time
from typing import Any, Dict, Optional, Sequence
import urllib

import numpy as np
import pandas as pd
from pyproj import Geod
import requests
import xarray as xr

from .cmr import bloom_filter
from .inrange import inrange
from .storage import OceanColorDB, FileSystem


module_logger = logging.getLogger("OceanColor")


class InRange(object):
    """Search Ocean Color DB for pixels in range of defined positions

       The satellite files are scanned in parallel in the background and
       checked against the given waypoints, so that it searches for the next
       matchup in advance before it is actually requested.
    """
    def __init__(self, username, password, npes=None):
        if npes is None:
            npes = int(2 * mp.cpu_count())
        self.npes = npes
        self.manager = mp.Manager()
        self.queue = self.manager.Queue(int(3 * npes))

        self.db = OceanColorDB(username, password)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        output = self.queue.get()
        if isinstance(output, str) and (output == "END"):
            raise StopIteration
        return output

    def search(self, track, sensor, dtype, dt_tol, dL_tol):
        """Initiate a new search

        Parameters
        ----------
        track:
        sensor:
        dtype:
        dt_tol:
        dL_tol:
        """
        module_logger.debug("Searching for matchups.")
        self.worker = mp.Process(
            target=self.scanner,
            args=(self.queue, self.npes, track, sensor, dtype, dt_tol, dL_tol),
        )
        module_logger.debug("Starting scanner worker.")
        self.worker.start()

    def scanner(self, queue, npes, track, sensor, dtype, dt_tol, dL_tol):
        timeout = 900
        module_logger.debug("Starting scanner, pid: {}".format(os.getpid()))

        filenames = bloom_filter(track, sensor, dtype, dt_tol)
        module_logger.debug("Finished bloom filter")

        with mp.Pool(processes=npes) as pool:
            results = []
            for f in filenames:
                module_logger.debug("New target: {}".format(f))
                if len(results) > npes:
                    idx = [r.ready() for r in results]
                    while not np.any(idx):
                        time.sleep(1)
                        idx = [r.ready() for r in results]
                    tmp = results.pop(idx.index(True)).get()
                    module_logger.debug("Finished reading another file")
                    if not tmp.empty:
                        module_logger.info("Found {} matchs".format(len(tmp)))
                        queue.put(tmp)
                module_logger.debug("Getting {}".format(f["filename"]))
                ds = self.db[f["filename"]].compute()
                results.append(pool.apply_async(inrange, (track, ds, dL_tol, dt_tol)))
            for tmp in (r.get(timeout) for r in results):
                module_logger.debug("Finished reading another file")
                if not tmp.empty:
                    module_logger.info("Found {} matchs".format(len(tmp)))
                    queue.put(tmp)
        module_logger.debug("Finished scanning all potential matchups.")
        queue.put("END")
