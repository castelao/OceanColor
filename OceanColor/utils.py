"""Miscellaneous utils such as flag mask decoding
"""

import logging
import os


module_logger = logging.getLogger("OceanColor.utils")


def oceancolorrc():
    """Path to custom configuration

    Define the path to the user custom configuration, such as EarthData's
    username to be used.

    The default path is at the user's home directory .config/oceancolor, but
    that can be modified by defining an environment variable OCEANCOLOR_DIR.

    Example
    -------
    >>> import os.path
    >>> print(os.path.join(oceancolorrc(), 'main.ini'))
    /Users/guilherme/.config/oceancolor/main.ini
    """
    path = os.path.expanduser(os.getenv("OCEANCOLOR_DIR", "~/.config/oceancolor"))
    return path


def decode_L2_flagmask(flag_mask: int):
    """Decode Ocean Color flag mask

    Some Ocean Color products use bitwise quality flags. This function converts
    those bits parsed as an integer into a list of flag labels. For instance,
    the binary 0010 values 2 in decimal and means that the second flag (LAND)
    is active.

    Parameters
    ----------
    flag_mask : int
        The bitwise flag parsed as uint

    Returns
    -------
    list of str
        List of flags activated byt the given `flag_mask`

    References
    ----------
    Flags reference:
    https://oceancolor.gsfc.nasa.gov/atbd/ocl2flags/

    Examples
    --------
    >>> decode_L2_flagmask(1073741828)
    ['PRODWARN', 'PRODFAIL']

    Notes
    -----
    Some key flags used for L3 products:
      - ATMFAIL: Atmospheric correction failure
      - LAND: Pixel is over land
      - HIGLINT: Sunglint: reflectance exceeds threshold
      - HILT: Observed radiance very high or saturated
      - HISATZEN: Sensor view zenith angle exceeds threshold
      - STRAYLIGHT: Probable stray light contamination
      - CLDICE: Probable cloud or ice contamination
      - COCCOLITH: Coccolithophores detected
      - HISOLZEN: Solar zenith exceeds threshold
      - LOWLW: Very low water-leaving radiance
      - CHLFAIL: Chlorophyll algorithm failure
      - NAVWARN: Navigation quality is suspect
      - MAXAERITER: Maximum iterations reached for NIR iteration
      - CHLWARN: Chlorophyll out-of-bounds
      - ATMWARN: Atmospheric correction is suspect
      - NAVFAIL: Navigation failure
      - HIPOL: High degree of polarization determined
    """

    # Full labels list and values
    # flag_masks = 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912, 1073741824, -2147483648 ;
    flag_labels = "ATMFAIL LAND PRODWARN HIGLINT HILT HISATZEN COASTZ SPARE STRAYLIGHT CLDICE COCCOLITH TURBIDW HISOLZEN SPARE LOWLW CHLFAIL NAVWARN ABSAER SPARE MAXAERITER MODGLINT CHLWARN ATMWARN SPARE SEAICE NAVFAIL FILTER SPARE BOWTIEDEL HIPOL PRODFAIL SPARE" ;
    flag_labels = flag_labels.split()
    flags = []
    for i, b in enumerate(bin(flag_mask)[:1:-1]):
        if b == '1':
            flags.append(flag_labels[i])
    return flags
