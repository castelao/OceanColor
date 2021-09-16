"""Top-level package for NASA Ocean Color."""

import os
import sys
import warnings


__author__ = """Guilherme Castelão"""
__email__ = "guilherme@castelao.net"
__version__ = "0.0.9"

# Recent OSX requires this environment variable to run parallel processes
if sys.platform == "darwin":
    if os.environ.get("OBJC_DISABLE_INITIALIZE_FORK_SAFETY") != "YES":
        msg = "You might require OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES"
        warnings.warn(msg, RuntimeWarning)
