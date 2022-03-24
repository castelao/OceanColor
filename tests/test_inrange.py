"""Test module inrange
"""

from datetime import datetime
import tempfile

from numpy import datetime64, timedelta64
import os

import pandas as pd
from pandas import DataFrame
import pytest

from OceanColor.inrange import matchup_L2, matchup_L3m, matchup
from OceanColor.storage import OceanColorDB, FileSystem
from OceanColor.OceanColor import InRange

username = os.getenv("NASA_USERNAME")
password = os.getenv("NASA_PASSWORD")

db = OceanColorDB(username, password)
db.backend = FileSystem('./')


def test_matchup_L2():
    ds = db["A2017012213500.L2_LAC_OC.nc"]
    dL_tol = 12e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 34, "lon": -126}])
    data = matchup_L2(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.index.size == 448


def test_matchup_L2_day_line():
    """Test nearby the international day line from both sides
    """
    ds = db["V2017013002400.L2_SNPP_OC.nc"]
    dL_tol = 6e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 60, "lon": 179.99}])
    data = matchup_L2(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.index.size == 194
    assert data.lon.min() < 0
    assert data.lon.max() > 0

    # From the other side
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 60, "lon": -179.99}])
    data = matchup_L2(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.index.size == 197
    assert data.lon.min() < 0
    assert data.lon.max() > 0


def test_matchup_L3m():
    ds = db["A2017012.L3m_DAY_CHL_chlor_a_4km.nc"]
    dL_tol = 12e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 34, "lon": -126}])
    data = matchup_L3m(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.index.size == 7
    assert data.size == 42


def test_matchup():
    ds = db["A2017012.L3m_DAY_CHL_chlor_a_4km.nc"]
    dL_tol = 12e3
    dt_tol = timedelta64(6, 'h')
    track = DataFrame([
        {"time": datetime64("2017-01-12 20:00:00"), "lat": 34, "lon": -126}])
    data = matchup(track, ds, dL_tol, dt_tol)

    # Dummy check
    assert data.index.size == 7
    assert data.size == 42


@pytest.mark.skip()
def test_InRange_recent():
    """Find recent in range

    By using FileSystem in a temporary directory guarantees that the cache
    is not been used. At some point a typo in the InRange's scanner was
    missed by the tests since it was acessing the cache without actually
    downloading it.
    """
    track = DataFrame(
        [
            {
                "time": datetime64(datetime.utcnow()) - timedelta64(15, "D"),
                "lat": 35.6,
                "lon": -126.81,
            }
        ]
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        matchup = InRange(username, password, tmpdirname, npes=3)
        matchup.search(
            track, sensor="snpp", dtype="L2", dt_tol=timedelta64(12, "h"), dL_tol=10e3
        )
        output = pd.concat([m for m in matchup])

    assert len(output) > 0


def test_InRange_early_termination():
  """Terminate before consuming or even finished searching
  """
  sensor = 'aqua'
  dtype = 'L3m'
  # dtype = 'L2'
  dL_tol = 12e3
  dt_tol = timedelta64(12, 'h')
  track = DataFrame([
      {"time": datetime64("2016-09-01 10:00:00"), "lat": 35.6, "lon": -126.81},
      {"time": datetime64("2016-09-01 22:00:00"), "lat": 34, "lon": -126}])

  matchup = InRange(username, password, './', npes=3)
  matchup.search(track, sensor, dtype, dt_tol, dL_tol)
  del(matchup)

  matchup = InRange(username, password, './', npes=3)
  matchup.search(track, sensor, dtype, dt_tol, dL_tol)
  # End environment without ever using it
