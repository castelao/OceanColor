"""Main module."""

from io import BytesIO
import logging
import threading, queue
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
    def __init__(self, username, password, path="./", npes=None):
        if npes is None:
            npes = int(2 * mp.cpu_count())
        self.npes = npes
        n_queue = int(3 * npes)
        self.queue = queue.Queue(int(3 * npes))

        self.db = OceanColorDB(username, password)
        self.db.backend = FileSystem(path)

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
        self.worker = threading.Thread(
            target=self.scanner,
            args=(self.queue, self.npes, track, sensor, dtype, dt_tol, dL_tol),
        )
        module_logger.debug("Starting scanner worker.")
        self.worker.start()

    def scanner(self, queue, npes, track, sensor, dtype, dt_tol, dL_tol):
        timeout = 900
        module_logger.debug("Starting scanner, pid: {}".format(os.getpid()))

        filenames = bloom_filter(track, sensor, dtype, dt_tol, dL_tol)
        module_logger.debug("Finished bloom filter")

        results = []
        for f in filenames:
            module_logger.info("Scanning: {}".format(f))
            if len(results) > 2:
                idx = [r.is_alive() for r in results]
                if np.all(idx):
                    r = results.pop(0)
                    module_logger.debug("Waiting for {}".format(r.name))
                else:
                    r =  results.pop(idx.index(False))
                r.join()
                module_logger.debug("Finished {}".format(r.name))
            module_logger.debug("Getting {}".format(f))
            ds = self.db[f].compute()
            module_logger.debug("Launching search on {}".format(f))
            results.append(
                threading.Thread(
                    target=inrange,
                    args=(track, ds, dL_tol, dt_tol, queue)))
            results[-1].start()
        for r in results:
            r.join()
            module_logger.debug("Finished {}".format(r.name))

        module_logger.debug("Finished scanning all potential matchups.")
        queue.put("END")
