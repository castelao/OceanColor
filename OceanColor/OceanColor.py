"""Main module."""

from datetime import datetime, timedelta
from io import BytesIO
import json
import logging
import multiprocessing as mp
import os
import random
import time
from typing import Any, Dict, Optional, Sequence
import urllib

import numpy as np
import pandas as pd
from pyproj import Geod
import requests
import xarray as xr


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
    block = np.timedelta64(200, 'D')
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
    assert kwargs['sensor'] in ['aqua', 'terra']
    assert kwargs['dtype'] == 'L3m'
    return ['*DAY_CHL_chlor_a_4km*']


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
        content = self.remote_content(key)
        ds = xr.open_dataset(BytesIO(content))

        assert ds.processing_level in ('L2', 'L3 Mapped'), \
                "I only handle L2 or L3 Mapped"
        if ds.processing_level == 'L2':
            # Seems like it can't read groups using BytesIO
            with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as tmp:
                tmp.write(content)
                tmp.flush()
                geo = xr.open_dataset(tmp.name, group='geophysical_data')
                nav = xr.open_dataset(tmp.name, group='navigation_data')
                ds = ds.merge(geo).merge(nav)
                ds = ds
        return ds


    def remote_content(self, filename, t_min=3, t_random=4):
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


def inrange(track, ds, dL_tol, dt_tol):
    """Search for all pixels in a time/space range of a track

    This is the general function that will choose which procedure to apply
    according to the processing level of the image, since they are structured
    in different projections.
    """
    assert ds.processing_level in ('L2', 'L3 Mapped')
    if ds.processing_level == 'L2':
        return inrange_L2(track, ds, dL_tol, dt_tol)
    elif ds.processing_level == 'L3 Mapped':
        return inrange_L3m(track, ds, dL_tol, dt_tol)


def inrange_L2(track: Any, ds: Any, dL_tol: Any, dt_tol: Any):
    """All satellite L2 pixels in range of a track

    For a given data frame of waypoints, return all satellite data, including
    lat, lon, dL (distance), and dt (difference in time) in respect of all
    waypoints.

    Parameters
    ----------
    track: pd.DataFrame
        A collection of waypoints containing {time, lat, lon}. The index on
        this DataFrame will be used as the reference on for the output.

    ds: xr.Dataset
        L3m composite.

    dL_tol: float
        Distance in meters around a waypoint to be considered a matchup.

    dt_tol: np.timedelta64
        Time difference to be considered a matchup.

    Returns
    -------
    matchup: pd.DataFrame
    """
    assert ds.processing_level == 'L2', "inrange_L2() requires L2 satellite data"
    output = pd.DataFrame()

    # Removing the Zulu part of the date definition. Better double
    #   check if it is UTC and then remove the tz.
    time_coverage_start = pd.to_datetime(
            ds.time_coverage_start.replace('Z','',))
    time_coverage_end = pd.to_datetime(
            ds.time_coverage_end.replace('Z','',))

    idx = (track.time >= (time_coverage_start - dt_tol)) & \
            (track.time <= (time_coverage_end + dt_tol))
    # Profiles possibly in the time window covered by the file
    subset = track[idx].copy()

    assert ds.pixel_control_points.shape == ds.pixels_per_line.shape
    ds = ds.swap_dims({'pixel_control_points': 'pixels_per_line'})

    # ==== Restrict to lines and columns within the latitude range ====
    # Using 100 to get a larger deg_tol
    deg_tol = dL_tol / 100e3
    idx = ((ds.latitude >= (subset.lat.min() - deg_tol))
            & (ds.latitude <= (subset.lat.max() + deg_tol)))
    if not idx.any():
        return output
    ds = ds.isel(pixels_per_line=idx.any(dim='number_of_lines'))
    ds = ds.isel(number_of_lines=idx.any(dim='pixels_per_line'))

    varnames = [v for v in ds.variables.keys()
            if ds.variables[v].dims == ('number_of_lines', 'pixels_per_line')]
    varnames = [v for v in varnames if v not in ('latitude', 'longitude')]
    #ds = ds[varnames]

    g = Geod(ellps='WGS84') # Use Clarke 1966 ellipsoid.
    assert ds.time.dims == ('number_of_lines',), "Assume time by n of lines"
    for i, p in subset.iterrows():
        for l, grp in ds.groupby('number_of_lines'):
            # Only sat. Chl within a certain distance.
            dL = g.inv(grp.longitude.data,
                       grp.latitude.data,
                       np.ones(grp.longitude.shape) * p.lon,
                       np.ones(grp.latitude.shape) * p.lat
                       )[2]
            idx = dL <= dL_tol
            if idx.any():
                tmp = {'waypoint_id': i,
                       'lon': grp.longitude.data[idx],
                       'lat': grp.latitude.data[idx],
                       'dL': dL[idx],
                       'dt': pd.to_datetime(grp.time.data) - p.time
                       }

                for v in varnames:
                    tmp[v] = grp[v].data[idx]

                tmp = pd.DataFrame(tmp)
                # Remove rows where all varnames are NaN
                tmp = tmp[(~tmp[varnames].isna()).any(axis='columns')]
                output = output.append(tmp, ignore_index=True)

    return output


def inrange_L3m(track: Any,
                ds: Any,
                dL_tol: Any,
                dt_tol: Any):
    """All satellite pixels within a tolerance around profiles from a track

    For a given data frame of profiles, return all satellite data, including
    lat, lon, dL (distance), and dt (difference in time) in respect of all
    profiles.

    Parameters
    ----------
    track: pd.DataFrame
        A collection of waypoints containing {time, lat, lon}. The index on
        this DataFrame will be used as the reference on for the output.

    ds: xr.Dataset
        L3m composite.

    dL_tol: float
        Distance in meters around a waypoint to be considered a matchup.

    dt_tol: np.timedelta64
        Time difference to be considered a matchup.


    Returns
    -------
    matchup: pd.DataFrame

    Notes
    -----
    Since L3M product is daily means, dt=0 means a profile on the same day of
    the satellite measurement, while + 3hrs means the hour 3 of the following
    day. Further, dt_tol=0 limits to satellite mean for the same day of the
    profile, while dt_tol=12 limits to the day of the profile plus the previous
    day if spray measured in the morning or following day if measurement was
    done in the afternoon/evening.

    IDEA: Maybe crop nc by min/max lat and lon before estimate the distances.
    """

    assert ds.processing_level == 'L3 Mapped', "inrange_L3M() requires L3 Mapped satellite data"

    # Removing the Zulu part of the date definition. Better double
    #   check if it is UTC and then remove the tz.
    time_coverage_start = pd.to_datetime(
            ds.time_coverage_start.replace('Z','',))
    time_coverage_end = pd.to_datetime(
            ds.time_coverage_end.replace('Z','',))

    time_reference = (time_coverage_start + \
            (time_coverage_end - time_coverage_start)/2.)

    idx = (track.time >= (time_coverage_start - dt_tol)) & \
            (track.time <= (time_coverage_end + dt_tol))
    # Profiles possibly in the time window covered by the file
    subset = track[idx].copy()

    # Using 110 to get a slightly larger deg_tol
    deg_tol = dL_tol / 110e3
    ds = ds.isel(lat = (ds.lat >= subset.lat.min() - deg_tol))
    ds = ds.isel(lat = (ds.lat <= subset.lat.max() + deg_tol))

    Lon, Lat = np.meshgrid(ds.lon[:], ds.lat[:])

    varnames = [v for v in ds.variables.keys()
            if ds.variables[v].dims == ('lat', 'lon')]
    ds = ds[varnames]

    output = pd.DataFrame()
    g = Geod(ellps='WGS84') # Use Clarke 1966 ellipsoid.
    # Maybe filter
    for i, p in subset.iterrows():
        # Only sat. Chl within a certain distance.
        dL = g.inv(Lon,
                   Lat,
                   np.ones(Lon.shape) * p.lon,
                   np.ones(Lat.shape) * p.lat
                   )[2]
        idx = dL <= dL_tol
        tmp = {'profile_id': i,
               'lon': Lon[idx],
               'lat': Lat[idx],
               'dL': dL[idx]}

        # Overlap between daily averages can result in more than 2 images
        # Any time inside the coverage range is considered dt=0
        # if p.datetime < time_coverage_start:
        #     tmp['dt'] = p.datetime - time_coverage_start
        # elif p.datetime > time_coverage_end:
        #     tmp['dt'] = p.datetime - time_coverage_end
        # else:
        #     tmp['dt'] = pd.Timedelta(0)
        tmp['dt'] = time_reference - p.time

        for v in varnames:
            tmp[v] = ds[v].data[idx]

        tmp = pd.DataFrame(tmp)
        # Remove rows where all varnames are NaN
        tmp = tmp[(~tmp[varnames].isna()).any(axis='columns')]
        output = output.append(tmp, ignore_index=True)

    return output


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
