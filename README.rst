===========
Ocean Color
===========

.. image:: https://zenodo.org/badge/318619654.svg
   :target: https://zenodo.org/badge/latestdoi/318619654

.. image:: https://readthedocs.org/projects/oceancolor/badge/?version=latest
        :target: https://oceancolor.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/OceanColor.svg
        :target: https://pypi.python.org/pypi/OceanColor

.. image:: https://github.com/castelao/OceanColor/actions/workflows/ci.yml/badge.svg
        :target: https://github.com/castelao/OceanColor/actions

.. image:: https://mybinder.org/badge_logo.svg
   :target: https://mybinder.org/v2/gh/castelao/OceanColor/main?filepath=docs%2Fnotebooks

Search and subset NASA Ocean Color data

* Free software: BSD license
* Documentation: https://oceancolor.readthedocs.io.

NASA provides great resources to search and access its data. This package is
intended to fill a gap in obtaining chlorophyll data to be compared with in
situ observations by searching pixels within a given time and distance tolerance.
For instance, it is used to calibrate chlorophyll fluorescence measurements
from Spray underwater gliders.

The OceanColor package was developed at the `Instrument Development Group 
<https://idg.ucsd.edu>`_ of `Scripps Institution of Oceanography
<https://scripps.ucsd.edu>`_ in support for the `California Underwater Glider
Network <https://spraydata.ucsd.edu/projects/CUGN/>`_ operations, which is
funded by:

* Global Ocean Monitoring and Observing (GOMO) Program - NOAA
* Southern California Coastal Ocean Observing System (SCCOOS)

---------------------
Quickstart - terminal
---------------------

Let's install it

.. code-block:: console

    $ pip install OceanColor

or

.. code-block:: console

    $ conda install OceanColor

Let's get the L2 chlorophyll measurements from MODIS-Aqua nearby Scripps' Pier.
On the terminal, let's run:

.. code-block:: console

    $ OceanColor InRange \
      --username=<earthdata-username> \
      --password=<earthdata-password> \
      --data-type=L2 \
      --time-tolerance=12 \
      --distance-tolerance=15e3 \
      2019-05-21T12:00:00,32.867066,-117.257408

Using it inside Python is more flexible than in the terminal. Check the manual
on how to use it.

------------
Alternatives
------------

* `pyModis <https://github.com/lucadelu/pyModis>`_ is a well established and
  mature package. If you are not satisfied with OceanColor, consider using
  pyModis. If you are interested in working with full frames (granules),
  instead of the subset of pixels nearby some reference, pyModis might be
  a better solution for you.
