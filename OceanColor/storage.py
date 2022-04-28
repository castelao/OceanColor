"""Store and manage NASA data

Different backends allow for different ways to handle the data from NASA.
"""

from abc import ABC
from collections import OrderedDict
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

try:
    import s3fs
    S3FS_AVAILABLE = True
except:
    S3FS_AVAILABLE = False
    module_logger.debug("s3fs library is not available")


class OceanColorDB(object):
    """An abstraction of NASA's Ocean Color database

    While OceanColorDB provides access to NASA's ocean color data, it is the
    backend that manages the data accessed. Currently, there is only one
    backend based on local files and directories. But it is planned more
    alternatives such as AWS S3 storage.

    Examples
    --------
    >>> db = OceanColorDB(username, password)
    >>> db.backend = FileSystem('./')
    >>> ds = db['T2004006.L3m_DAY_CHL_chlor_a_4km.nc']
    >>> ds.attrs

    Notes
    -----
    Think about the best way to define the backend. Maybe add an optional
    parameter path, which if available is used to define the backend as a
    FileSystem.
    """

    logger = logging.getLogger("OceanColorDB.storage.OceanColorDB")
    lock = threading.Lock()
    time_last_download = datetime(1970, 1, 1)

    def __init__(self, username: str, password: str, download: bool = True):
        """Initializes OceanColorDB

        Parameters
        ----------
        username: str
            The username registered with EarthData
        password: str
            The password associated the the username
        download: bool, optional
            Download new data when required, otherwise limits to the already
            available datasets. Default is true, i.e. download when necessary.
        """
        self.logger.debug("Instatiating OceanColorDB")
        self.username = username
        self.password = password
        self.download = download

    def __contains__(self, item: str):
        return self.backend.__contains__(item)

    def __getitem__(self, key):
        """

        Maybe use BytesIO?? or ds.compute()?
        """
        self.logger.debug(f"Reading from backend: {key}")
        try:
            return self.backend[key]
        except KeyError:
            self.logger.debug(f"{key} is not on the storage")
            if not self.download:
                self.logger.info(
                    f"{key} is not available and download is off."
                )
                raise KeyError
            return self._download(key)

    def _download(self, index):
        module_logger.debug("Downloading from Ocean Color: {}".format(index))
        # Probably move this reading from remote to another function
        content = self._remote_content(index)
        # ds = xr.open_dataset(BytesIO(content))
        # Seems like it can't read groups using BytesIO
        with tempfile.NamedTemporaryFile(mode="w+b", delete=True) as tmp:
            self.logger.debug("Saving to temporary file: {tmp.name}")
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
            self.backend[index] = ds
        return ds

    def backend(self):
        """Placeholder to reinforce the use of a backend

        While OceanColorDB manages the access to NASA's database and does the
        front end with the user, the backend actually manages the data.

        See Also
        --------
        OceanColor.storage.FileSystem :
            A storage backend based on directories and files
        """
        self.logger.critical(
            "OceanColorDB requires a backend. Consider using OceanColor.storage.FileSystem"
        )
        raise NotImplementedError("Must define a backend for OceanColorDB")

    def _remote_content(self, filename: str, t_min: int = 4, t_random: int = 4):
        """Read a remote file with a minimum time between downloads

        NASA monitors the downloads and excessive activity is temporarily
        banned, so this function guarantees a minimum time between downloads
        to avoid ovoerloading NASA servers.
        """
        self.logger.debug("Acquiring lock for remote content")
        self.lock.acquire()
        self.logger.debug("Lock acquired")
        dt = t_min + round(random.random() * t_random, 2)
        next_time = self.time_last_download + timedelta(seconds=(dt))
        waiting_time = max((next_time - datetime.now()).total_seconds(), 0)
        self.logger.debug(
            f"Waiting {waiting_time} seconds before downloading."
        )
        time.sleep(waiting_time)
        try:
            self.logger.info(f"Downloading: {filename}")
            content = read_remote_file(filename, self.username, self.password)
        finally:
            self.time_last_download = datetime.now()
            self.logger.debug("remote_content releasing lock")
            self.lock.release()

        return content


class BaseStorage(ABC):
    """Base class for storage backends
    """
    def __contains__(self, index):
        raise NotImplementedError("Missing __contains__(), not implemented")

    def __getitem__(self, index):
        raise NotImplementedError("Missing __getitem__ for this Backend")

    def __setitem__(self, index, value):
        raise NotImplementedError("Missing __setitem__ for this Backend")


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
    logger = logging.getLogger("OceanColorDB.storage.FileSystem")

    def __init__(self, root: str):
        """Initiate a FileSystem backend

        Paremeters
        ----------
        root : str
            Base path where to build/find the local data structure. All data
            is contained inside this directory.
        """
        self.logger.debug(f"Using FileSystem as storage at: {root}")

        if not os.path.isdir(root):
            self.logger.critical(f"Invalid path for backend.FileSystem {root}")
            raise FileNotFoundError
        self.root = os.path.abspath(root)

    def __getitem__(self, key):
        filename = self.path(key)
        try:
            self.logger.debug(f"Openning file: {filename}")
            ds = xr.open_dataset(filename)
        except FileNotFoundError:
            raise KeyError
        return ds

    def __setitem__(self, key, ds):
        assert isinstance(ds, xr.Dataset)
        filename = self.path(key)
        d = os.path.dirname(filename)
        if not os.path.exists(d):
            self.logger.debug(f"Creating missing directory: {d}")
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

    def path(self, filename: str):
        """Standard path for the given filename

        Ocean Color filenames follow certain standards that can be used to
        infer the platform, sensor, year, DOY, etc. From that information
        it is defined the standard directory where to store/find that file.

        Parameters
        ----------
        filename: str
            Filename, or granule as called at NASA

        Examples
        --------
        >>> f = FileSystem('/data')
        >>> f.path('A2019109.L3m_DAY_CHL_chlor_a_4km.nc')
        '/data/MODIS-Aqua/L3m/2019/109/A2019109.L3m_DAY_CHL_chlor_a_4km.nc'
        """
        return os.path.join(self.root, Filename(filename).path)


class S3Storage(BaseStorage):
    logger = logging.getLogger("OceanColorDB.storage.S3Storage")

    def __init__(self, root: str):
        """
        Parameters
        ----------
        root:
            The S3 root point, including bucket and key prefix

        Examples
        --------
        >>> backend = S3Storage('s3://mybucket/NASA/')
        >>> 'T2004006.L3m_DAY_CHL_chlor_a_4km.nc' in backend
        """
        if not S3FS_AVAILABLE:
            self.logger.error("Missing s3fs library required by S3Storage")
            raise ImportError

        self.root = root
        self.fs = s3fs.S3FileSystem(anon=False)

    def __contains__(self, index: str):
        """Checks if the given index exists in the storage

        It doesn't actually recover the item, so it minimizes network
        transfer.
        """
        try:
            access_point = self.path(index)
        except:
            return False

        return self.fs.exists(access_point)

    def __getitem__(self, index):
        """Recover dataset identified by the given index

        Returns
        -------
        xr.Dataset
        """
        if index not in self:
            self.logger.debug(f"Object not available: {index}")
            raise KeyError

        access_point = self.path(index)
        self.logger.debug(f"Acessing remote: {access_point}")
        ds = xr.open_zarr(access_point)
        self.logger.debug(f"Finished opening remote: {access_point}")
        return ds

    def __setitem__(self, index, ds):
        """Saves Dataset ds identified by index
        """
        if not isinstance(ds, xr.Dataset):
            self.logger.warn("Trying to save a non xr.Dataset object")
            raise ValueError
        access_point = self.path(index)

        if index in self:
            self.logger.error("Not ready to update an S3 object")
            raise NotImplementedError

        store = s3fs.S3Map(root=access_point, s3 = self.fs)
        ds.to_zarr(store=store, consolidated=True, mode='w')

    def path(self, product_name: str):
        p = os.path.join(self.root, Filename(product_name).path)
        return p.replace(".nc", ".zarr")


class Filename(object):
    """Parse implicit information on NASA's filename

    NASA's data filename, and granules, follows a logical standard that can be
    used to infer some information, such as instrument or year of the
    measuremnt.

    This class is used in support for the FileSystem backend to guide its
    directory structure.
    """
    def __init__(self, filename: str):
        """
        Parameters
        ----------
        filename : str
            A filename following NASA's OceanColor standard

        Examples
        --------
        >>> f = Filename("A2019109.L3m_DAY_CHL_chlor_a_4km.nc")
        >>> f.mission
        MODIS-Aqua
        """
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


def parse_filename(filename: str):
    """Parse an OceanColor data filename

    There is a logical standard on the filenames and this function takes
    advantage of that to extract information such as date, processing level,
    and platform.

    Parameters
    ----------
    filename : str
        An Ocean Color dataset filename.

    Returns
    -------
    dict :
        A dictionary with fields such as platform, year, day of year (doy),
        time, mode (data processing level), and instrument. It returns None
        when the field is not available.

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


class InMemory(BaseStorage):
    """In memory storage

    Minimalist solution to store granules in memory.
    """
    logger = logging.getLogger("OceanColorDB.storage.InMemory")

    __data = OrderedDict()

    def __init__(self, quota: int = 5 * 1024 ** 3):
        """Initialize an InMemory object

        Parameters
        ----------
        quota: int
            Maximum ammount of bytes to store. Once that limit is reached,
            the oldest item stored is deleted.
        """
        self.quota = quota

    def __contains__(self, index):
        return index in self.__data

    def __getitem__(self, index):
        if index in self:
            self.__data.move_to_end(index)
        return self.__data[index]

    def __setitem__(self, index, ds):
        assert isinstance(ds, xr.Dataset)
        self.__data[index] = ds
        self.apply_quota()

    @property
    def nbytes(self):
        """Total bytes stored"""
        if len(self.__data) == 0:
            return 0
        return int(round(sum([v.nbytes for v in self.__data.values()])))

    def apply_quota(self):
        """Verify quota and remove old objects if necessary

        The most recently acessed objects have priority, thus the oldest
        objects are removed first.
        """
        while (len(self.__data) > 1) and (self.nbytes > self.quota):
            self.__data.popitem(last=False)
