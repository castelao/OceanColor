"""Main module."""

import logging
import os
import re

import xarray as xr


module_logger = logging.getLogger("OceanColor.storage")


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
        assert os.path.isdir(root)
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
        path = os.path.join(self.mission, self.attrs["mode"], self.attrs["year"], self.attrs["doy"])
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
    rule = """
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
