"""Main module."""

from datetime import datetime, timedelta
from io import BytesIO
import json
import logging
import multiprocessing as mp
import os
import random
import re
import tempfile
import time
from typing import Any, Dict, Optional, Sequence
import urllib

import numpy as np
import pandas as pd
from pyproj import Geod
import requests
import xarray as xr

from .gsfc import read_remote_file
from .cmr import bloom_filter
from .inrange import inrange
from .storage import FileSystem


module_logger = logging.getLogger("OceanColor")


class OceanColorDB(object):
    """An abstraction of NASA's Ocean Color database

    In the future develop a local cache so it wouldn't need to download more
    than once the same file.

    Examples
    --------
    >>> db = OceanColorDB(username, password)
    >>> db.backend = FileSystem('./')
    >>> ds = db['T2004006.L3m_DAY_CHL_chlor_a_4km.nc']
    >>> ds.attrs

    ToDo
    ----
    - Generalize the backend entry. The idea in the future is to create other
      backends like S3.
    - Think about the best way to define the backend. Maybe add an optional
      parameter path, which if available is used to define the backend as a
      FileSystem.
    """

    lock = mp.Lock()
    time_last_download = datetime(1970, 1, 1)

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __getitem__(self, key):
        """

        Maybe use BytesIO?? or ds.compute()?
        """
        try:
            ds = self.backend[key]
            module_logger.debug("Reading from backend: {}".format(key))
        except KeyError:
            module_logger.debug("Reading from Ocean Color: {}".format(key))
            # Probably move this reading from remote to another function
            content = self.remote_content(key)
            # ds = xr.open_dataset(BytesIO(content))
            # Seems like it can't read groups using BytesIO
            with tempfile.NamedTemporaryFile(mode="w+b", delete=True) as tmp:
                tmp.write(content)
                tmp.flush()

                ds = xr.open_dataset(tmp.name)

                assert ds.processing_level in (
                    "L2",
                    "L3 Mapped",
                ), "I only handle L2 or L3 Mapped"
                if ds.processing_level == "L2":
                    geo = xr.open_dataset(tmp.name, group="geophysical_data")
                    ds = ds.merge(geo)
                    nav = xr.open_dataset(tmp.name, group="navigation_data")
                    ds = ds.merge(nav)
                    # Maybe include full scan line into ds
                    sline = xr.open_dataset(tmp.name, group="scan_line_attributes")
                    ds["time"] = (
                        (sline - 1970).year.astype("datetime64[Y]")
                        + sline.day
                        - np.timedelta64(1, "D")
                        + sline.msec
                    )
                    ds = ds.rename({"latitude": "lat", "longitude": "lon"})
            self.backend[key] = ds
        return ds


    def remote_content(self, filename, t_min=4, t_random=4):
        """Read a remote file with minimum time between downloads

        NASA monitors the downloads and excessive activity is temporarily
        banned, so this function guarantees a minimum time between downloads
        to avoid ovoerloading NASA servers.
        """
        self.lock.acquire()
        module_logger.debug("remote_content aquired lock")
        dt = t_min + round(random.random() * t_random, 2)
        next_time = self.time_last_download + timedelta(seconds=(dt))
        waiting_time = max((next_time - datetime.now()).total_seconds(), 0)
        module_logger.debug(
            "Waiting {} seconds before downloading.".format(waiting_time)
        )
        time.sleep(waiting_time)
        try:
            module_logger.info("Downloading: {}".format(filename))
            content = read_remote_file(filename, self.username, self.password)
        finally:
            self.time_last_download = datetime.now()
            module_logger.debug("remote_content releasing lock")
            self.lock.release()

        return content


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
