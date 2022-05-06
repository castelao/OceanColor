#!/usr/bin/env python

"""Tests for `OceanColor` package."""

import os
import pickle

import pytest
import xarray as xr
try:
    import s3fs
    S3FS_AVAILABLE = True
except:
    S3FS_AVAILABLE = False

from OceanColor.backend.common import parse_filename
from OceanColor.backend import FileSystem, S3Storage, InMemory
from OceanColor.storage import OceanColorDB


def test_parse_filename_AL2():
    filename = "A2011010000000.L2_LAC_OC.nc"
    descriptors = parse_filename(filename)

    ans = {
        "platform": "A",
        "year": "2011",
        "doy": "010",
        "time": "000000",
        "mode": "L2",
        "instrument": None,
    }
    for a in ans:
        assert descriptors[a] == ans[a]


def test_parse_filename_AL3m():
    filename = "T2004006.L3m_DAY_CHL_chlor_a_4km.nc"
    descriptors = parse_filename(filename)

    ans = {
        "platform": "T",
        "year": "2004",
        "doy": "006",
        "time": None,
        "mode": "L3m",
        "instrument": None,
    }
    for a in ans:
        assert descriptors[a] == ans[a]


def test_OceanColorDB():
    db = OceanColorDB(os.getenv("NASA_USERNAME"), os.getenv("NASA_PASSWORD"))
    db.backend = FileSystem("./")
    ds = db["T2004006.L3m_DAY_CHL_chlor_a_4km.nc"]
    ds.attrs


def test_contains():
    """Contain check for FileSystem

    Allows to check if a granule is available in the FileSystem.
    """
    db = OceanColorDB(os.getenv("NASA_USERNAME"), os.getenv("NASA_PASSWORD"))
    db.backend = FileSystem("./")

    # Confirm that inexistent is not available
    assert not "inexistent_granule" in db

    # Check something that exists
    # Be sure that is was available or download it first
    filename = "T2004006.L3m_DAY_CHL_chlor_a_4km.nc"
    ds = db[filename]
    # then check (confirm) that it is available
    assert filename in db


def test_serialize_OceanColorDB():
    """Test if a OceanColorDB item is serializeable

    This is required to transport between processes, threads and queues.
    """
    db = OceanColorDB(os.getenv("NASA_USERNAME"), os.getenv("NASA_PASSWORD"))
    db.backend = FileSystem("./")
    ds = db["T2004006.L3m_DAY_CHL_chlor_a_4km.nc"]
    ds2 = pickle.loads(pickle.dumps(ds.compute()))

    assert ds == ds2


def test_no_download():
    """

    The SNPP was not available on 2000, thus the used target here is guarantee
    to be missing from the current db
    """
    db = OceanColorDB(
        username=os.getenv("NASA_USERNAME"),
        password=os.getenv("NASA_PASSWORD"),
        download=False,
    )
    db.backend = FileSystem("./")

    filename = "V2000009.L3m_DAY_SNPP_CHL_chlor_a_4km.nc"
    # Confirm that it is not available, otherwise this test doesn't make sense
    assert filename not in db

    try:
        db[filename]
    except KeyError:
        return
    # It was not supposed to reach here
    raise

@pytest.mark.skipif(not S3FS_AVAILABLE, reason="S3Storage is not available without s3fs")
def test_S3Storage_path():
    backend = S3Storage("s3://mybucket/datadir")
    assert backend.path("A2019109.L3m_DAY_CHL_chlor_a_4km.nc") == 's3://mybucket/datadir/MODIS-Aqua/L3m/2019/109/A2019109.L3m_DAY_CHL_chlor_a_4km.zarr'


def test_inmemory():
    """Silly test. Improve this

    ToDo:
    - Create test data with @pytest.fixture
    """
    db = InMemory()
    ds = xr.Dataset({"x": [1,2,3]})
    db["test-1"] = ds

    assert "test-1" in db


def test_inmemory_quota():
    """Independent of the quota, at least one item
    """
    ds = xr.Dataset({"x": [1,2,3]})
    db = InMemory(quota=ds.nbytes + 1)
    db["test-1"] = ds
    db["test-2"] = ds

    assert "test-1" not in db
    assert "test-2" in db


def test_inmemory_at_least_one():
    """Independent of the quota, at least one item
    """
    db = InMemory(quota=0)
    ds = xr.Dataset({"x": [1,2,3]})
    db["test-1"] = ds

    assert "test-1" in db
