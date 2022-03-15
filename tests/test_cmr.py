#!/usr/bin/env python

"""Tests for `OceanColor` package."""

from numpy import datetime64, timedelta64
import pandas as pd

from OceanColor.cmr import api_walk, bloom_filter, granules_search, search_criteria


def test_api_walk():
    src = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    params = {
        "sort_key": "start_date",
        "short_name": "MODISA_L2_OC",
        "provider": "OB_DAAC",
        "circle": "-126.81,35.6,10000",
        "temporal": "2019-05-02,2019-05-03",
    }

    for r in api_walk(src, **params):
        r


def test_granules_search():
    for g in granules_search(
        short_name="MODISA_L2_OC",
        provider="OB_DAAC",
        temporal="2008-01-03,2008-01-05",
        circle="-126.9,34.48,10000",
    ):
        g


def test_bloom_filter():
    track = [{"time": datetime64("2019-05-01"), "lat": 18, "lon": 38}]
    track = pd.DataFrame(track)
    for f in bloom_filter(
        track, sensor="aqua", dtype="L2", dt_tol=timedelta64(36, "h"), dL_tol=10e3
    ):
        f

    for f in bloom_filter(
        track, sensor="aqua", dtype="L3m", dt_tol=timedelta64(36, "h"), dL_tol=10e3
    ):
        f


def test_bloom_filter_unique():
    """bloom_filter() should return unique potential matchs

    When searching for a track with multiple points, it is possible to have an
    overlap of the potential matchups, such as two close by waypoints in the
    same track. To avoid that, bloom_filter keeps a memory of the results to
    ignore duplicates.
    """
    track = [
        {"time": datetime64("2019-05-01"), "lat": 18, "lon": 38},
        {"time": datetime64("2019-05-01"), "lat": 18.001, "lon": 38.001},
        {"time": datetime64("2019-05-02"), "lat": 18, "lon": 38},
    ]
    track = pd.DataFrame(track)
    search = bloom_filter(track, sensor="aqua", dtype="L3m", dt_tol=timedelta64(24, "h"), dL_tol=10e3)
    results = [r for r in search]
    assert len(results) == len(set(results)), "Duplicates from bloom_filter"


def test_bloom_filter_spaced_target():
    track = [
        {"time": datetime64("2019-05-01 12:00:00"), "lat": 18, "lon": 38},
        {"time": datetime64("2019-05-05 12:00:00"), "lat": 18, "lon": 38},
        {"time": datetime64("2019-05-15 12:00:00"), "lat": 18, "lon": 38},
    ]
    track = pd.DataFrame(track, index=[0, 10, 100])
    search = bloom_filter(track, sensor="aqua", dtype="L3m", dt_tol=timedelta64(6, "h"), dL_tol=5e3)
    results = [r for r in search]
    print(results)
    assert len(results) < 5
    assert len(results) == 3


def test_bloom_multiple_sensors():
    track = [{"time": datetime64("2019-05-01"), "lat": 18, "lon": 38}]
    filter = bloom_filter(
        pd.DataFrame(track),
        sensor=["aqua", "terra", "snpp"],
        dtype="L2",
        dt_tol=timedelta64(36, "h"),
        dL_tol=10e3
    )
    assert len([f for f in filter]) > 0

def test_search_criteria():
    search = search_criteria(sensor="aqua", dtype="L2")
    assert search["short_name"] == "MODISA_L2_OC"
    assert search["provider"] == "OB_DAAC"

    search = search_criteria(sensor="aqua", dtype="L3m")
    assert search["short_name"] == "MODISA_L3m_CHL"
    assert search["provider"] == "OB_DAAC"


"""
{'cdate': '2019-08-05 21:23:16', 'checksum': 'sha1:97b97ec2bc5c59255fd8e5ec8551f7bebb6f8be5', 'getfile': 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile', 'size': 9881732, 'filename': 'A2019152.L3m_DAY_CHL_chlor_a_4km.nc'}
{'cdate': '2019-08-05 20:56:42', 'checksum': 'sha1:de47941eb5c5454ac7b629c54643ccdbb482988b', 'getfile': 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile', 'size': 9116814, 'filename': 'A2019153.L3m_DAY_CHL_chlor_a_4km.nc'}
{'cdate': '2019-08-05 20:52:19', 'checksum': 'sha1:7dddb1e4c8ed1edc75bf5b98ed813ad53382ca90', 'getfile': 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile', 'size': 8982649, 'filename': 'A2019154.L3m_DAY_CHL_chlor_a_4km.nc'}
{'cdate': '2019-08-06 02:02:12', 'checksum': 'sha1:cfee7831f71551c8679068110f91fb6b20e325a1', 'getfile': 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile', 'size': 5644739, 'filename': 'A2019152.L3m_DAY_ZLEE_Zeu_lee_4km.nc'}
{'cdate': '2019-08-06 02:01:38', 'checksum': 'sha1:4ed7bddfc1dbe5d0be1616a40953bfba07adc98e', 'getfile': 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile', 'size': 5225433, 'filename': 'A2019153.L3m_DAY_ZLEE_Zeu_lee_4km.nc'}
{'cdate': '2019-08-06 02:01:32', 'checksum': 'sha1:4ba5c21492edfccc116a7a14135ec2cf4cce7e34', 'getfile': 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile', 'size': 5164016, 'filename': 'A2019154.L3m_DAY_ZLEE_Zeu_lee_4km.nc'}
"""
