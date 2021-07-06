"""Main module."""

from datetime import datetime, timedelta
import logging
import numpy as np
import os
import random
import re
import tempfile
import time
import threading

import xarray as xr

from .gsfc import read_remote_file


module_logger = logging.getLogger("OceanColor.storage")


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

    lock = threading.Lock()
    time_last_download = datetime(1970, 1, 1)

    def __init__(self, username: str, password: str, download: bool = True):
        """Initializes OceanColorDB

        Parameters
        ----------
        username: str
        password: str
        download: bool
            Download new data when required, otherwise limit to the already
            available datasets.
        """
        self.username = username
        self.password = password
        self.download = download

    def __getitem__(self, key):
        """

        Maybe use BytesIO?? or ds.compute()?
        """
        module_logger.debug("Reading from backend: {}".format(key))
        try:
            return self.backend[key]
        except KeyError:
            module_logger.debug("{} is not on the storage".format(key))
            if not self.download:
                module_logger.info("{} is not available and download is off.".format(key))
                raise KeyError

        module_logger.debug("Downloading from Ocean Color: {}".format(key))
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

    def __contains__(self, item: str):
        return self.backend.__contains__(item)

    def backend(self):
        raise NotImplementedError("Must define a backend for OceanColorDB")

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


# db.backend
class FileSystem(object):
    """Backend for OceanColorDB based on files and directories

    A file system backend for OceanColorDB to save the data files in
    directories. Distribute the files in a directory system close to the one
    in the OceanColor website, otherwise it could pile more than the OS can
    hold in the same directory.

    ToDo
    ----
    Need to create some function that understands the filename syntax so that
    it can extract level of processing, platform and at least year so that
    the files can be split in multiple subdirectories, otherwise it can blow
    the contents limit for the operational system. Probably around several
    hundreds of files in the same directory.
    """

    def __init__(self, root):
        module_logger.debug("Using FileSystem as storage at: {}".format(root))

        if not os.path.isdir(root):
            module_logger.critical(
                "Invalid path for backend.FileSystem {}".format(root)
            )
            raise FileNotFoundError
        self.root = os.path.abspath(root)

    def __getitem__(self, key):
        filename = self.path(key)
        try:
            module_logger.debug("Openning file: {}".format(filename))
            ds = xr.open_dataset(filename)
        except FileNotFoundError:
            raise KeyError
        return ds

    def __setitem__(self, key, ds):
        assert isinstance(ds, xr.Dataset)
        filename = self.path(key)
        d = os.path.dirname(filename)
        if not os.path.exists(d):
            module_logger.debug("Creating missing directory: {}".format(d))
            os.makedirs(d)
        # ds.to_netcdf("{}.nc".format(filename))
        ds.to_netcdf(filename)

    def __contains__(self, key: str):
        # Improve this: Better handle invalid granule name (key).
        try:
            filename = self.path(key)
        except:
            return False

        if os.path.exists(filename):
            return True
        else:
            return False

    def path(self, filename):
        """Standard path for the given filename

        Ocean Color filenames follow certain standards that can be used to
        infer the platform, sensor, year, DOY, etc. From that information
        it is defined the standard directory where to store/find that file.

        Examples
        --------
        >>> f = FileSystem('/data')
        >>> f.path('A2019109.L3m_DAY_CHL_chlor_a_4km.nc')
        '/data/MODIS-Aqua/L3m/2019/109/A2019109.L3m_DAY_CHL_chlor_a_4km.nc'
        """
        return os.path.join(self.root, Filename(filename).path)


class Filename(object):
    def __init__(self, filename):
        self.filename = filename
        self.attrs = parse_filename(filename)

    @property
    def mission(self):
        attrs = self.attrs

        if attrs["platform"] == "S":
            return "SeaWIFS"
        elif attrs["platform"] == "A":
            return "MODIS-Aqua"
        elif attrs["platform"] == "T":
            return "MODIS-Terra"
        elif attrs["platform"] == "V":
            if attrs["instrument"] == "JPSS1":
                return "VIIRS-JPSS1"
            elif attrs["instrument"] == "SNPP":
                return "VIIRS-SNPP"

    @property
    def dirname(self):
        path = os.path.join(
            self.mission, self.attrs["mode"], self.attrs["year"], self.attrs["doy"]
        )
        return path

    @property
    def path(self):
        return os.path.join(self.dirname, self.filename)


def parse_filename(filename):
    """Parse an OceanColor data filename

    Parse filenames to extract information like the date or platform related
    to the given filename.

    Parameters
    ----------
    filename : str
        An Ocean Color dataset filename.

    Notes
    -----
    Examples of possible files:
      - S2002006003729.L2_[GAC_IOP|GAC_OC|MLAC_OC].nc
      - S2001006.L3m_DAY_[CHL_chlor_a|CHL_chl_ocx|ZLEE_Zeu_lee]_9km.nc
      - A2011010000000.L2[_LAC_OC|_LAC_IOP|SST|SST4].nc
      - T2004006.L3m[_DAY_CHL_chlor_a|_DAY_CHL_chl_ocx]_[4|9]km.nc
      - V2018007000000.L2_SNPP_OC.nc
      - V2015009.L3m_DAY_SNPP_CHL_chlor_a_4km.nc
      - V2018006230000.L2_JPSS1_OC.nc
    """
    rule = r"""
        (?P<platform>[S|A|T|V])
        (?P<year>\d{4})
        (?P<doy>\d{3})
        (?P<time>\d+)?
        \.
        (?P<mode>(L2)|(L3m))
        (?:_DAY)?
        _ (?P<instrument>(?:SNPP)|(?:JPSS1))?
        .*?
        \.nc
        """
    output = re.match(rule, filename, re.VERBOSE).groupdict()
    return output
