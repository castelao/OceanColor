"""Utils module."""

import logging
import os


module_logger = logging.getLogger("OceanColor.utils")


def oceancolorrc():
    """Path to custom configuration

    Default path is at the user's home directory .config/oceancolor
    """
    path = os.path.expanduser(os.getenv("OCEANCOLOR_DIR", "~/.config/oceancolor"))
    return path


def decode_L2_flagmask(flag_mask):
    """Decode Ocean Color flag mask

    Some Ocean Color products use bitwise quality flags. This function
    converts those bits encoded as an integer into a list of flags labels,
    i.e. binary 10 values 2 in decimal and means second flag active which
    is LAND.

    This function can be useful help to decide which data to use.

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

    Full labels list and values
    flag_masks = 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912, 1073741824, -2147483648 ;
flag_labels = "ATMFAIL LAND PRODWARN HIGLINT HILT HISATZEN COASTZ SPARE STRAYLIGHT CLDICE COCCOLITH TURBIDW HISOLZEN SPARE LOWLW CHLFAIL NAVWARN ABSAER SPARE MAXAERITER MODGLINT CHLWARN ATMWARN SPARE SEAICE NAVFAIL FILTER SPARE BOWTIEDEL HIPOL PRODFAIL SPARE" ;
    """
    flag_labels = "ATMFAIL LAND PRODWARN HIGLINT HILT HISATZEN COASTZ SPARE STRAYLIGHT CLDICE COCCOLITH TURBIDW HISOLZEN SPARE LOWLW CHLFAIL NAVWARN ABSAER SPARE MAXAERITER MODGLINT CHLWARN ATMWARN SPARE SEAICE NAVFAIL FILTER SPARE BOWTIEDEL HIPOL PRODFAIL SPARE" ;
    flag_labels = flag_labels.split()
    flags = []
    for i, b in enumerate(bin(flag_mask)[:1:-1]):
        if b == '1':
            flags.append(flag_labels[i])
    return flags
