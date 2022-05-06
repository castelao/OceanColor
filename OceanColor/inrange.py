"""Search for data in range (time and space)

Resources to extract the satellite pixels that are in range (time and space)
of given waypoints.
"""

import logging
import threading, queue
import os
import time
from typing import Any, Dict, Optional, Sequence

import numpy as np
import pandas as pd
from pyproj import Geod

from .cmr import bloom_filter
from . import OceanColorDB
from .backend import FileSystem

module_logger = logging.getLogger("OceanColor.inrange")

try:
    from loky import ProcessPoolExecutor

    LOKY_AVAILABLE = True
    module_logger.debug("Will use package loky to search in parallel.")
except:
    LOKY_AVAILABLE = False
    module_logger.info("Missing package loky. Falling back to threading.")


class InRange(object):
    """Search and fetch Ocean Color pixels within range of given waypoints

    The satellite files are scanned in parallel in the background and checked
    against the given waypoints, so that it searches for the next matchup in
    advance before it is actually requested.

    Examples
    --------
    >>> track = DataFrame([
            {"time": datetime64("2016-09-01 10:00:00"), "lat": 35.6, "lon": -126.81},
            {"time": datetime64("2016-09-01 22:00:00"), "lat": 34, "lon": -126}
            ])

    >>> engine = InRange(os.getenv("NASA_USERNAME"),
                          os.getenv("NASA_PASSWORD"),
                          './',
                          npes=3)

    >>> engine.search(track,
                       sensor="aqua",
                       dtype="L3m",
                       dt_tol=timedelta64(12, 'h'),
                       dL_tol=12e3)
    >>> for m in engine:
    >>>     print(m)
    """

    logger = logging.getLogger("OceanColorDB.inrange.InRange")

    def __init__(self, username, password, path="./", npes=None):
        """
        Parameters
        ----------
        username : str
            NASA's EarthData username
        password : str
            NASA's EarthData password
        path : str, optional
            Path to save locally NASA's data files
        npes : int, optional
            Number of maximum parallel jobs
        """
        self.logger.info("Initializing inrange.InRange searching engine")
        if npes is None:
            npes = 3
        self.logger.debug(f"npes: {npes}")
        self.npes = npes
        queue_size = int(3 * npes)
        self.logger.debug(f"Using queue size: {queue_size}")
        self.queue = queue.Queue(queue_size)

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
        """Initiate a new search in the background

        Parameters
        ----------
        track:
        sensor:
        dtype:
        dt_tol:
        dL_tol:
        """
        self.logger.debug("Searching for matchups.")
        if LOKY_AVAILABLE:
            scanner = self.scanner
            self.logger.debug("Scanning in parallel with loky.")
        else:
            scanner = self.scanner_threading
            self.logger.debug("Scanning with threading.")

        parent = threading.current_thread()
        self.worker = threading.Thread(
            target=scanner,
            args=(self.queue, parent, self.npes, track, sensor, dtype, dt_tol, dL_tol),
        )
        self.logger.debug("Starting scanner worker.")
        self.worker.start()

    def download_only(self, track, sensor, dtype, dt_tol, dL_tol):
        filenames = bloom_filter(track, sensor, dtype, dt_tol, dL_tol)
        for f in filenames:
            self.db.check(f)

    def scanner_threading(self, queue, parent, npes, track, sensor, dtype, dt_tol, dL_tol):
        timeout = 900
        self.logger.debug("Scanner, pid: {}".format(os.getpid()))

        filenames = bloom_filter(track, sensor, dtype, dt_tol, dL_tol)
        self.logger.debug("Finished bloom filter")

        results = []
        for f in filenames:
            self.logger.info("Scanning: {}".format(f))
            if (len(results) >= npes) and parent.is_alive():
                idx = [r.is_alive() for r in results]
                if np.all(idx):
                    r = results.pop(0)
                    self.logger.debug("Waiting for {}".format(r.name))
                else:
                    r = results.pop(idx.index(False))
                r.join()
                self.logger.debug("Finished {}".format(r.name))
            self.logger.debug("Getting {}".format(f))
            ds = self.db[f].compute()
            self.logger.debug("Launching search on {}".format(f))
            if not parent.is_alive():
                return
            results.append(
                threading.Thread(
                    target=matchup, args=(track, ds, dL_tol, dt_tol, queue)
                )
            )
            results[-1].start()
        for r in results:
            if not parent.is_alive():
                return
            r.join()
            self.logger.debug("Finished {}".format(r.name))

        self.logger.debug("Finished scanning all potential matchups.")
        queue.put("END")


    def scanner(self, queue, parent, npes, track, sensor, dtype, dt_tol, dL_tol):
        timeout = 900
        self.logger.debug("Scanner, pid: {}".format(os.getpid()))

        filenames = bloom_filter(track, sensor, dtype, dt_tol, dL_tol)
        self.logger.debug("Finished bloom filter")

        with ProcessPoolExecutor(max_workers=npes, timeout=timeout) as executor:
            results = []
            for f in filenames:
                self.logger.info("Scanning: {}".format(f))
                if (len(results) >= npes) and parent.is_alive():
                    idx = [r.done() for r in results]
                    while not np.any(idx):
                        time.sleep(1)
                        idx = [r.done() for r in results]
                    tmp = results.pop(idx.index(True)).result()
                    self.logger.debug("Finished reading another file")
                    if not tmp.empty:
                        self.logger.warning("Found {} matchs".format(len(tmp)))
                        queue.put(tmp)
                self.logger.debug("Getting {}".format(f))
                ds = self.db[f].compute()
                if not parent.is_alive():
                    return
                self.logger.debug("Submitting a new inrange process")
                results.append(executor.submit(matchup, track, ds, dL_tol, dt_tol))

            for tmp in (r.result(timeout) for r in results):
                if not parent.is_alive():
                    return
                self.logger.debug("Finished reading another file")
                if not tmp.empty:
                    self.logger.warning("Found {} matchs".format(len(tmp)))
                    queue.put(tmp)

        self.logger.debug("Finished scanning all potential matchups.")
        queue.put("END")


def matchup(track, ds, dL_tol: float, dt_tol, queue=None):
    """Search a granule for pixels within range (time/space) of a track

    For a given sequence of waypoints (`track`), it returns all satellite
    pixels (data) from `ds` which are at most `dL_tol` (distance tolerance)
    and `dt_tol` (time tolerance) from one of the waypoints. The output
    includes lat and lon of the found pixel, plus dL (distance), and dt
    (difference in time) between the matchup pixel - waypoint.

    This is the generic function that will choose which procedure to apply
    according to the processing level of the image, since they are structured
    in different projections.

    Parameters
    ----------
    track: pandas.DataFrame
        A collection of waypoints containing {time, lat, lon}. The index on
        this DataFrame will be used as the reference on for the output.

    ds: xarray.Dataset
        An L2 granule, which is usually loaded from a netCDF.

    dL_tol: float
        Maximum distance in meters from a waypoint to be considered a matchup.

    dt_tol: np.timedelta64
        Maximum accepted time difference to be considered a matchup.

    queue: Queue.queue, optional
        If given, the results are sent to this queue.

    Returns
    -------
    matchup: pd.DataFrame
        All pixels within space and time range from the given track of
        waypoints. One pixel per row. If queue is given as an input, the
        returns are instead transmitted to that queue and the function
        returns None instead.

    See Also
    --------
    matchup_L2 : Search an L2 dataset for pixels within a range
    matchup_L3m : Search an L3m dataset for pixels within a range
    """
    assert ds.processing_level in ("L2", "L3 Mapped")
    if ds.processing_level == "L2":
        module_logger.debug("processing_level L2, using matchup_L2")
        output = matchup_L2(track, ds, dL_tol, dt_tol)
    elif ds.processing_level == "L3 Mapped":
        module_logger.debug("processing_level L3 mapped, using matchup_L3m")
        output = matchup_L3m(track, ds, dL_tol, dt_tol)
    else:
        return

    if queue is None:
        return output
    elif output.size > 0:
        module_logger.info(
            "Found {} matchups in {}".format(
                len(output.index), ds.product_name
            )
        )
        queue.put(output)
    else:
        module_logger.info("No matchups from {}".format(ds.product_name))


def matchup_L2(track, ds, dL_tol: float, dt_tol):
    """Search an L2 Dataset for pixels within range of a track

    For a given data frame of waypoints, return all satellite data, including
    lat, lon, dL (distance), and dt (difference in time) in respect of all
    waypoints.

    Parameters
    ----------
    track: pd.DataFrame
        A collection of waypoints containing {time, lat, lon}. The index on
        this DataFrame will be used as the reference on for the output.

    ds: xr.Dataset
        An L2 granule, which is usually loaded from a netCDF.

    dL_tol: float
        Maximum distance in meters from a waypoint to be considered a matchup.

    dt_tol: np.timedelta64
        Maximum accepted time difference to be considered a matchup.

    Returns
    -------
    matchup: pd.DataFrame
        All pixels within space and time range from the given track of
        waypoints. One pixel per row.

    See Also
    --------
    matchup : Search a dataset for pixels within a range
    matchup_L3m : Search an L3m dataset for pixels within a range
    """
    assert ds.processing_level == "L2", "matchup_L2() requires L2 satellite data"
    output = pd.DataFrame()

    # Removing the Zulu part of the date definition. Better double
    #   check if it is UTC and then remove the tz.
    time_coverage_start = pd.to_datetime(ds.time_coverage_start.replace("Z", "",))
    time_coverage_end = pd.to_datetime(ds.time_coverage_end.replace("Z", "",))

    idx = (track.time >= (time_coverage_start - dt_tol)) & (
        track.time <= (time_coverage_end + dt_tol)
    )
    # Profiles possibly in the time window covered by the file
    subset = track[idx].copy()

    assert ds.pixel_control_points.shape == ds.pixels_per_line.shape
    ds = ds.swap_dims({"pixel_control_points": "pixels_per_line"})

    # ==== Restrict to lines and columns within the latitude range ====
    # Using 100 to get a larger deg_tol
    deg_tol = dL_tol / 100e3

    idx = (ds.lat >= (subset.lat.min() - deg_tol)) & (
        ds.lat <= (subset.lat.max() + deg_tol)
    )
    if not idx.any():
        return output

    # Meridians converge poleward, thus requiring a different criterion
    lon_tol = deg_tol / np.cos(np.pi / 180 * subset.lat.abs().max())
    lon_start = subset.lon.min() - lon_tol
    lon_end = subset.lon.max() + lon_tol
    # Otherwise do the precise distance estimate to handle the day line.
    if (lon_start > -180) and (lon_end < 180):
        idx &= (ds.lon >= (subset.lon.min() - lon_tol)) & (
            ds.lon <= (subset.lon.max() + lon_tol))
        if not idx.any():
            return output

    ds = ds.isel(pixels_per_line=idx.any(dim="number_of_lines"))
    ds = ds.isel(number_of_lines=idx.any(dim="pixels_per_line"))

    varnames = [
        v
        for v in ds.variables.keys()
        if ds.variables[v].dims == ("number_of_lines", "pixels_per_line")
    ]
    varnames = [v for v in varnames if v not in ("lat", "lon")]
    # ds = ds[varnames]

    g = Geod(ellps="WGS84")  # Use Clarke 1966 ellipsoid.
    assert ds.time.dims == ("number_of_lines",), "Assume time by n of lines"
    for i, p in subset.iterrows():
        for _, grp in ds.groupby("number_of_lines"):
            # Only sat. Chl within a certain distance.
            dL = g.inv(
                grp.lon.data,
                grp.lat.data,
                np.ones(grp.lon.shape) * p.lon,
                np.ones(grp.lat.shape) * p.lat,
            )[2]
            idx = dL <= dL_tol
            if idx.any():
                # Save the product_name??
                tmp = {
                    "waypoint_id": i,
                    "lon": grp.lon.data[idx],
                    "lat": grp.lat.data[idx],
                    "dL": dL[idx].astype("i"),
                    "dt": pd.to_datetime(grp.time.data) - p.time,
                }

                for v in varnames:
                    tmp[v] = grp[v].data[idx]

                tmp = pd.DataFrame(tmp)
                # Remove rows where all varnames are NaN
                tmp = tmp[(~tmp[varnames].isna()).any(axis="columns")]
                output = pd.concat([output, tmp], ignore_index=True)

    if "product_name" in ds.attrs:
        output["product_name"] = ds.product_name
    return output


def matchup_L3m(track, ds, dL_tol: float, dt_tol):
    """Search an L3 Dataset for pixels within range of a track

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
        All pixels within space and time range from the given track of
        waypoints. One pixel per row.

    See Also
    --------
    matchup : Search a dataset for pixels within a range
    matchup_L2 : Search an L2 dataset for pixels within a range

    Notes
    -----
    Since L3M product are daily means, dt=0 means a profile on the same day of
    the satellite measurement, while + 3hrs means the hour 3 of the following
    day. Further, dt_tol=0 limits to satellite mean for the same day of the
    profile, while dt_tol=12 limits to the day of the profile plus the previous
    day if spray measured in the morning or following day if measurement was
    done in the afternoon/evening.

    IDEA: Maybe crop nc by min/max lat and lon before estimate the distances.



    Return all satellite data in range of some profile

       For a given data frame of profiles, return all satellite data,
         including lat, lon, dL (distance), and dt (difference in time)
         in respect of all profiles.

       Since L3M product is daily means, dt=0 means a profile on the same day
         of the satellite measurement, while + 3hrs means the hour 3 of the
         following day. Further, dt_tol=0 limits to satellite mean for the
         same day of the profile, while dt_tol=12 limits to the day of the
         profile plus the previous day if spray measured in the morning or
         following day if measurement was done in the afternoon/evening.

         IDEA: Maybe crop nc by min/max lat and lon before estimate the
           distances.
    """
    #    if dt_tol is None:
    #    dt_tol = pd.to_timedelta(0)
    # elif isinstance(dt_tol, datetime):
    #    dt_tol = pd.to_timedelta(dt_tol)

    assert (
        ds.processing_level == "L3 Mapped"
    ), "matchup_L3m() requires L3 Mapped satellite data"

    # Removing the Zulu part of the date definition. Better double
    #   check if it is UTC and then remove the tz.
    time_coverage_start = pd.to_datetime(ds.time_coverage_start.replace("Z", "",))
    time_coverage_end = pd.to_datetime(ds.time_coverage_end.replace("Z", "",))

    time_reference = (
        time_coverage_start + (time_coverage_end - time_coverage_start) / 2.0
    )

    idx = (track.time >= (time_coverage_start - dt_tol)) & (
        track.time <= (time_coverage_end + dt_tol)
    )
    # Profiles possibly in the time window covered by the file
    subset = track[idx].copy()

    # Using 110 to get a slightly larger deg_tol
    deg_tol = dL_tol / 110e3
    ds = ds.isel(lat=(ds.lat >= subset.lat.min() - deg_tol))
    ds = ds.isel(lat=(ds.lat <= subset.lat.max() + deg_tol))

    Lon, Lat = np.meshgrid(ds.lon[:], ds.lat[:])

    varnames = [
        v for v in ds.variables.keys() if ds.variables[v].dims == ("lat", "lon")
    ]
    ds = ds[varnames]

    output = pd.DataFrame()
    g = Geod(ellps="WGS84")  # Use Clarke 1966 ellipsoid.
    # Maybe filter
    for i, p in subset.iterrows():
        # Only sat. Chl within a certain distance.
        dL = g.inv(Lon, Lat, np.ones(Lon.shape) * p.lon, np.ones(Lat.shape) * p.lat)[2]
        idx = dL <= dL_tol
        tmp = {
            "waypoint_id": i,
            "lon": Lon[idx],
            "lat": Lat[idx],
            "dL": dL[idx].astype("i"),
        }

        # What to do if idx is none? Need to do something here and stop earlier

        # Overlap between daily averages can result in more than 2 images
        # Any time inside the coverage range is considered dt=0
        # if p.datetime < time_coverage_start:
        #     tmp['dt'] = p.datetime - time_coverage_start
        # elif p.datetime > time_coverage_end:
        #     tmp['dt'] = p.datetime - time_coverage_end
        # else:
        #     tmp['dt'] = pd.Timedelta(0)
        tmp["dt"] = time_reference - p.time

        for v in varnames:
            tmp[v] = ds[v].data[idx]

        tmp = pd.DataFrame(tmp)
        # tmp.dropna(inplace=True)
        # Remove rows where all varnames are NaN
        tmp = tmp[(~tmp[varnames].isna()).any(axis="columns")]
        output = pd.concat([output, tmp], ignore_index=True)

    return output
