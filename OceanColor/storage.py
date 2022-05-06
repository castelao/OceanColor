"""Store and manage NASA data

Different backends allow for different ways to handle the data from NASA.
"""

from datetime import datetime, timedelta
import logging
import numpy as np
import random
import tempfile
import time
import threading

import xarray as xr

from .gsfc import read_remote_file
# To guarantee backward compatibility
from .backend import *


module_logger = logging.getLogger("OceanColor.storage")


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

    logger = logging.getLogger("OceanColor.storage.OceanColorDB")
    backend = BaseStorage()
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
        self.logger.debug("Instantiating OceanColorDB")
        self.username = username
        self.password = password
        self.download = download

    def __contains__(self, item: str):
        return item in self.backend

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

    def _remote_content(
        self, filename: str, t_min: int = 4, t_random: int = 4
    ):
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

    def check(self, index):
        """Confirm that index is availble, otherwise, download it

        Useful in a pre-processing stage to guarantee that all required data
        is available. For instance a cronjob could run periodically just
        downloading new data so that it is available when the analysis is
        actually running.
        """
        if index in self:
            self.logger.debug(f"Item already available: {index}")
        else:
            ds = self._download(index)
            ds.close()
