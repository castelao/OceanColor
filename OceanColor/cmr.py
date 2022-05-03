"""Support to NASA's Common Metadata Repository
"""

import logging
from typing import Any, Dict, Optional, Sequence

from numpy import datetime64, datetime_as_string
import re
import requests

module_logger = logging.getLogger(__name__)


def api_walk(url, page_size=25, offset=0, **kwargs):
    """Walk through outputs from CMR API

    Iterate on NASA's Common Metadata Repository API output.

    Parameters
    ----------
    url : str
        CMR's API endpoint
    page_size : int, optional
        Number of results per page
    offset : int, optional
        Skip the offset number of results. Useful when rolling between pages

    Examples
    --------
    >>> src = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    >>> params = {"sort_key": "start_date", "short_name": "MODISA_L2_OC",
    ...           "provider": "OB_DAAC", "circle": "-126.81,35.6,10000",
    ...           "temporal": "2019-05-02,2019-05-03"}
    >>> for r in api_walk(src, **params):
    >>>     print(r)

    If the resulting list is longer than page_size, it still yields all
    responses, one by one.

    >>> params["temporal"] = "2019-05-02,2019-05-05"
    >>> for r in api_walk(src, page_size=2, **params):
    >>>     print(r)

    """
    kwargs["page_size"] = page_size
    kwargs["offset"] = offset
    module_logger.debug("api_walk() with kwargs: {}".format(kwargs))
    r = requests.get(url, params=kwargs)
    if r.status_code != 200:
        module_logger.warning("Failed {}".format(r.status_code))
    assert r.status_code == 200
    content = r.json()
    for item in content["items"]:
        yield item

    kwargs["offset"] += len(content["items"])
    if kwargs["offset"] < content["hits"]:
        yield from api_walk(url, **kwargs)


def granules_search(
    short_name, provider, temporal, circle, sort_key="start_date"
):
    """

    Maybe rename to filename_search

    Examples
    --------
    params = {
    >>> for g in granules_search(short_name="MODISA_L2_OC",
    ...                          provider="OB_DAAC",
    ...                          temporal="2008-01-03,2008-01-05",
    ...                          circle="-126.9,34.48,10000"):
    >>>     print(g)


    profile_time  (trajectory) datetime64[ns] 2008-08-05T23:30:52.500000
    profile_lat   (trajectory) float64 34.48
    profile_lon   (trajectory) float64 -126.9

    Notes
    -----
    - Should I use 'DIRECT DOWNLOAD' or 'GET DATA' fields and yield URL
      instead?
    """
    url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    params = {
        "short_name": short_name,
        "provider": provider,
        "sort_key": sort_key,
        "temporal": temporal,
        "circle": circle,
    }
    for result in api_walk(url, **params):
        # for r in result['umm']['RelatedUrls']:
        for r in result["umm"]["DataGranule"]["Identifiers"]:
            if r["IdentifierType"] == "ProducerGranuleId":
                yield r["Identifier"]


def search_criteria(**kwargs):
    """Build a searching criteria

    Examples
    --------
    search = search_criteria(sensor="aqua", dtype="L2")

    Notes
    -----
    - To include L3m it needs some sort of further filtering or it would return
      day, 8D, and monthly means; 4 & 9 km resolution; and other variations,
      while we would probably be interested in only one.

    """
    assert kwargs["sensor"] in ["seawifs", "aqua", "terra", "snpp"]
    assert kwargs["dtype"] in ("L2", "L3m")

    if kwargs["sensor"] == "seawifs":
        if kwargs["dtype"] == "L2":
            criteria = {"short_name": "SEAWIFS_L2_OC", "provider": "OB_DAAC"}
    elif kwargs["sensor"] == "snpp":
        if kwargs["dtype"] == "L2":
            criteria = {"short_name": "VIIRSN_L2_OC", "provider": "OB_DAAC"}
    elif kwargs["sensor"] == "aqua":
        if kwargs["dtype"] == "L2":
            criteria = {"short_name": "MODISA_L2_OC", "provider": "OB_DAAC"}
        elif kwargs["dtype"] == "L3m":
            criteria = {
                "short_name": "MODISA_L3m_CHL",
                "provider": "OB_DAAC",
                "search": "DAY_CHL_chlor_a_4km",
            }
    elif kwargs["sensor"] == "terra":
        if kwargs["dtype"] == "L2":
            criteria = {"short_name": "MODIST_L2_OC", "provider": "OB_DAAC"}
        elif kwargs["dtype"] == "L3m":
            criteria = {"short_name": "MODIST_L3m_CHL", "provider": "OB_DAAC"}
    else:
        raise ValueError
    module_logger.debug("searching criteria: {}".format(criteria))
    return criteria


def bloom_filter(
    track: Sequence[Dict],
    sensor: [Sequence[str], str],
    dtype: str,
    dt_tol: float,
    dL_tol: float,
):
    """Determine granules (filenames) that might have data of interest

    This function returns the granules that might be within the searching
    window around the given waypoint or track.

    A bloom filter returns False or maybe True, and it is typically an
    approximation significantly lighter than the the precise solution.
    Therefore, it can be used to eliminate what is a guarantee to be False
    with a reduced cost. For the OceanColor package this is useful since
    it reduces the searching space before applying a more precise searching
    criterion that is computationally or I/O intensive.

    Parameters
    ----------
    track : sequence of dict_like
        A sequence of waypoints, where each waypoint contains time, lat, and
        lon.
    sensor : str or sequence of str
        Instrument(s) used, such as snpp or aqua. For more than one instrument
        at once, use a list of.
    dtype : str
        Data processing level currently limited to L2 or L3m.
    dt_tol : float
        Time tolerance around the given waypoint(s).
    dL_tol : float
        Distance tolerance around the given waypoint(s).

    Yields
    -------
    str
        Granule name

    Notes
    -----
    This generator has a memory to avoid duplicates.  When running for a track
    composed of multiple waypoints, it is possible for a target to match
    multiple waypoints, but each target (granule) is returned only once.

    The search is around the waypoints, not the tracks. In the case of spaced
    tracks, let us say one fixed location every Sunday, with a small dt_tol of
    12 hours, the middle of the week is ignored as potential targets. For
    instance, the full history of an Argo profiler would result in a huge list
    of granules, where the end of the time series should not include the region
    where it started.
      
    Examples
    --------
    >>> track = [{"time": np.datetime64('2019-05-01'), "lat": 18, "lon": 38}]
    >>> for f in bloom_filter(track, sensor='snpp', dtype='L2', dt_tol=np.timedelta64(36, 'h'), dL_tol=10e3):
            print(f)
    V2019120103000.L2_SNPP_OC.nc
    V2019121100600.L2_SNPP_OC.nc
    V2019121101200.L2_SNPP_OC.nc
    V2019122094800.L2_SNPP_OC.nc
    V2019122113000.L2_SNPP_OC.nc

    """
    if isinstance(sensor, list):
        for s in sensor:
            module_logger.info(f"Processing a list of sensors. Now: {s}")
            filenames = bloom_filter(track, s, dtype, dt_tol, dL_tol)
            yield from filenames
        return

    # For a spaced track, break it in parts to avoid results in the middle
    # between valid waypoints.
    chrono = track.time.sort_values()
    dt = chrono.diff().abs()
    if (len(dt) > 1) and (dt.max() > 2 * dt_tol):
        time_split = chrono.iloc[dt.argmax()]
        module_logger.debug(
            "Sparse track. bloom_filter() will split search at: {}".format(time_split)
        )
        yield from bloom_filter(
            track=track[track.time < time_split],
            sensor=sensor,
            dtype=dtype,
            dt_tol=dt_tol,
            dL_tol=dL_tol,
        )
        yield from bloom_filter(
            track=track[track.time >= time_split],
            sensor=sensor,
            dtype=dtype,
            dt_tol=dt_tol,
            dL_tol=dL_tol,
        )
        return

    stime = datetime64(track.time.min() - dt_tol)
    etime = datetime64(track.time.max() + dt_tol)

    criteria = search_criteria(sensor=sensor, dtype=dtype)

    rule = criteria.pop("search", None)
    if rule is not None:
        rule = re.compile(rule)

    memory = []
    # Temporary solution. Scan each waypoint. To work with a track at once it
    # would require to define a buffer around it, then creating a polygon.
    # Plus it would require to split on space such as it is done on time.
    for _, p in track.iterrows():
        temporal = "{},{}".format(
            datetime_as_string(stime, unit="s"),
            datetime_as_string(etime, unit="s"),
        )
        circle = "{},{},{}".format(p.lon, p.lat, dL_tol)
        for g in granules_search(temporal=temporal, circle=circle, **criteria):
            if (rule is None) or rule.search(g):
                if g not in memory:
                    memory.append(g)
                    module_logger.debug("New result from bloom_filter: {}".format(g))
                    yield g


"""
    url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json?page_size=30&sort_key=short_name&sort_key=start_date&short_name=MODISA_L2_OC&provider=OB_DAAC&&bounding_box=-10,-5,10,5&temporal=2020-01-03,2020-01-10"



Aqua
L2
dt
dL
track
  -> split by time: 24hrs blocks
      -> split by dL
"""
