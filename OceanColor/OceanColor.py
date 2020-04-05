"""Main module."""

import json
import logging
from typing import Any, Dict, Optional, Sequence
import urllib

import numpy as np
import pandas as pd
import requests


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
