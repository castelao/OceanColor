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
