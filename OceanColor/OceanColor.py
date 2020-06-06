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

from .inrange import inrange
from .storage import FileSystem


module_logger = logging.getLogger('OceanColor')


def nasa_file_search(sensor: str,
                     dtype: str,
                     sdate: Any,
                     edate: Optional[Any] = None,
                     search: Optional[Any] = None
                     ) -> Sequence:
    """Search available files with NASA API

    Parameters
    ----------
    sensor:
    dtype:
    sdate: np.datetime64, optional
    edate: np.datettime64, optional
    search: str or list with pattern to search for

    Yields
    ------
    dict
        Dictionary including filename

    Notes
    -----
    There is an issue encoding the search. There is certainly a more elegant
    solution for this. An example to work on:
    
    data = urllib.parse.urlencode({"sensor": "aqua", "sdate": "2020-01-01",
    "edate": "2020-03-01", "dtype": "L3b", "addurl": 0, "results_as_file": 1,
    "search": "*DAY_CHL*"})

    Equivalent result can be achieved with wget:

    wget -q --post-data="sensor=octs&sdate=1996-11-01&edate=1997-01-01&dtype=L3b&addurl=1&results_as_file=1&search=*DAY_CHL*" -O - https://oceandata.sci.gsfc.nasa.gov/api/file_search

    Example
    -------
    >>> filenames = nasa_file_search(
            'aqua', 'L3m', np.datetime64('2019-06-01'),
            np.datetime64('2019-06-15'),
            ['*DAY_CHL_chlor_a_4km*', '*DAY_ZLEE_Zeu_lee_4km*'])   

    >>> for f in filenames:
    >>>     print(f)
    """
    if sdate is None:
        sdate = np.datetime64('1997-06-01') 
    if edate is None:
        edate = np.datetime64('now')

    # Split in blocks if the range is too long.
    if dtype in ('L0', 'L1', 'L2'):
        max_dt = 60
    else:
        max_dt = 200
    block = np.timedelta64(max_dt, 'D')
    if (edate - sdate) > block:
        for start in np.arange(sdate, edate, block):
            end = start + block - np.timedelta64(1, 'D')
            filenames = nasa_file_search(sensor, dtype, start, end, search)
            yield from filenames
        return

    if isinstance(search, list):
        for s in search:
            filenames = nasa_file_search(sensor, dtype, sdate, edate, s)
            yield from filenames
        return

    search_url = "https://oceandata.sci.gsfc.nasa.gov/api/file_search"

    possible_sensors = (
            "aquarius", "seawifs", "aqua", "terra", "meris", "octs", "czcs",
            "hico", "viirs", "snpp", "viirsj1", "s3olci", "s3bolci")
    possible_dtypes = ("L0", "L1", "L2", "L3b", "L3m", "MET", "misc")

    assert sensor in possible_sensors
    if sensor == "snpp":
        sensor = "viirs"

    cfg = {"sensor": sensor,
            "sdate": np.datetime_as_string(sdate, unit='D'),
            "edate": np.datetime_as_string(edate, unit='D'),
            "dtype": dtype,
            "addurl": 0,
            "results_as_file": 1,
            "cksum": 1,
            "format": "json"}

    # if search is not None:
    #     cfg["search"] = search

    data = urllib.parse.urlencode(cfg).encode('ascii')

    with urllib.request.urlopen(search_url, data) as f:
        filenames = f.read()

    filenames = json.loads(filenames.decode('utf-8'))

    # Temporary solution while search is not working with the request
    if search is not None:
        search = search.replace("*","")
        for f in [f for f in filenames if search not in f]:
            del(filenames[f])

    for f in sorted(filenames):
        output = filenames[f]
        output['filename'] = f
        yield output


def search_criteria(**kwargs):
    """Build a searching criteria

    Dummy function, just a place holder for now.
    """
    assert kwargs['sensor'] in ['seawifs', 'aqua', 'terra', 'snpp']
    assert kwargs['dtype'] in ('L2', 'L3m')
    if kwargs['sensor'] == 'seawifs':
        if kwargs['dtype'] == 'L2':
            criteria = ['*L2_GAC_OC.nc']
        elif kwargs['dtype'] == 'L3m':
            criteria = ['*DAY_CHL_chlor_a_9km*']
    elif kwargs['sensor'] == 'snpp':
        if kwargs['dtype'] == 'L2':
            criteria = ['*JPSS1_OC.nc']
        elif kwargs['dtype'] == 'L3m':
            criteria = ['*DAY_SNPP_CHL_chlor_a_4km.nc']
    elif kwargs['dtype'] == 'L2':
        criteria = ['*L2_LAC_OC.nc']
    elif kwargs['dtype'] == 'L3m':
        criteria = ['*DAY_CHL_chlor_a_4km*']
    return criteria


def bloom_filter(track: Sequence[Dict],
                 sensor: [Sequence[str], str],
                 dtype:str,
                 dt_tol: Optional[Any] = None):
    """Filter only satellite files with potential matches

    It can contain false positives, but must not allow false negatives, i.e.
    files excluded here are guarantee to not contain any mathcup, so it
    reduces the searching space for any potential matchup.

    Note
    ----
    - Include time and space resolution (like daily, and 4km or higest)
    - It should have the option to run with a local cache. Somehow download
      once a full list (or update a previous list) and work on that.
    - Later include criteria by geolimits.
    """
    if isinstance(sensor, list):
        for s in sensor:
            filenames = bloom_filter(track, s, dtype, dt_tol)
            yield from filenames
        return

    sdate = np.datetime64(track.time.min() - dt_tol)
    edate = np.datetime64(track.time.max() + dt_tol)
    search = search_criteria(sensor=sensor, dtype=dtype)
    filenames = nasa_file_search(sensor, dtype, sdate, edate, search)
    for f in filenames:
        yield f


def read_remote_file(filename, username, password):
    """Return the binary content of a NASA data file

    NASA now requires authentication to access its data files, thus a username
    and a password.
    """
    url_base = "https://oceandata.sci.gsfc.nasa.gov/ob/getfile/"
    url = urllib.request.urljoin(url_base, filename)

    with requests.Session() as session:
        session.auth = (username, password)
        r1 = session.request('get', url)
        r = session.get(r1.url, auth=(username, password))
        assert r.ok
        return r.content


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
            with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as tmp:
                tmp.write(content)
                tmp.flush()

                ds = xr.open_dataset(tmp.name)

                assert ds.processing_level in ('L2', 'L3 Mapped'), "I only handle L2 or L3 Mapped"
                if ds.processing_level == 'L2':
                    geo = xr.open_dataset(tmp.name, group='geophysical_data')
                    ds = ds.merge(geo)
                    nav = xr.open_dataset(tmp.name, group='navigation_data')
                    ds = ds.merge(nav)
                    # Maybe include full scan line into ds
                    sline = xr.open_dataset(tmp.name, group='scan_line_attributes')
                    ds['time'] = (sline - 1970).year.astype('datetime64[Y]') + sline.day -np.timedelta64(1, 'D') + sline.msec
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
        module_logger.debug('remote_content aquired lock')
        dt = t_min + round(random.random() * t_random, 2)
        next_time = self.time_last_download + timedelta(seconds=(dt))
        waiting_time = max((next_time - datetime.now()).total_seconds(), 0)
        module_logger.debug("Waiting {} seconds before downloading.".format(
            waiting_time))
        time.sleep(waiting_time)
        try:
            module_logger.debug("Downloading: {}".format(filename))
            content = read_remote_file(filename, self.username, self.password)
        finally:
            self.time_last_download = datetime.now()
            module_logger.debug('remote_content releasing lock')
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
        if isinstance(output, str) and (output == 'END'):
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
        self.worker = mp.Process(target=self.scanner,
                                 args=(self.queue, self.npes, track, sensor, dtype, dt_tol, dL_tol))
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
                        module_logger.debug("Found {} matchs".format(len(tmp)))
                        queue.put(tmp)
                module_logger.debug("Getting {}".format(f['filename']))
                ds = self.db[f['filename']].compute()
                results.append(pool.apply_async(inrange, (track, ds, dL_tol, dt_tol)))
            for tmp in (r.get(timeout) for r in results):
                module_logger.debug("Finished reading another file")
                if not tmp.empty:
                    module_logger.debug("Found {} matchs".format(len(tmp)))
                    queue.put(tmp)
        module_logger.debug("Finished scanning all potential matchups.")
        queue.put('END')
