"""Test module inrange
"""

from numpy import datetime64, timedelta64
import os
from pandas import DataFrame

from OceanColor.inrange import inrange_L2, inrange_L3m, inrange
from OceanColor.storage import OceanColorDB, FileSystem
from OceanColor.OceanColor import InRange

db = OceanColorDB(os.getenv("NASA_USERNAME"), os.getenv("NASA_PASSWORD"))
db.backend = FileSystem('./')


def test_inrange_L2():
    ds = db["A2017012213500.L2_LAC_OC.nc"]
    dL_tol = 12e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 34, "lon": -126}])
    data = inrange_L2(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.size == 12096


def test_inrange_L2_day_line():
    """Test nearby the international day line from both sides
    """
    ds = db["V2017013002400.L2_SNPP_OC.nc"]
    dL_tol = 6e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 60, "lon": 179.99}])
    data = inrange_L2(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.index.size == 194
    assert data.lon.min() < 0
    assert data.lon.max() > 0

    # From the other side
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 60, "lon": -179.99}])
    data = inrange_L2(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.index.size == 197
    assert data.lon.min() < 0
    assert data.lon.max() > 0


def test_inrange_L3m():
    ds = db["A2017012.L3m_DAY_CHL_chlor_a_4km.nc"]
    dL_tol = 12e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 34, "lon": -126}])
    data = inrange_L3m(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.size == 42


def test_inrange():
    ds = db["A2017012.L3m_DAY_CHL_chlor_a_4km.nc"]
    dL_tol = 12e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 34, "lon": -126}])
    data = inrange(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.size == 42


def test_InRange():
  sensor = 'aqua'
  dtype = 'L3m'
  # dtype = 'L2'
  dL_tol = 12e3
  dt_tol = timedelta64(12, 'h')
  track = DataFrame([
      {"time": datetime64("2016-09-01 10:00:00"), "lat": 35.6, "lon": -126.81},
      {"time": datetime64("2016-09-01 22:00:00"), "lat": 34, "lon": -126}])

  matchup = InRange(os.getenv("NASA_USERNAME"), os.getenv("NASA_PASSWORD"), './', npes=3)
  matchup.search(track, sensor, dtype, dt_tol, dL_tol)
