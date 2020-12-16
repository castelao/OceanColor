#!/usr/bin/env python

"""Tests for `OceanColor` package."""

import pytest
import numpy as np

from OceanColor.gsfc import oceandata_file_search


def test_nasa_file_search():
    """Minimalist test for searching NASA files

    I should expand this into several tests.
    """
    file_list = oceandata_file_search('aqua',
                                 'L3m',
                                 np.datetime64('2019-06-01'),
                                 np.datetime64('2019-06-01'),
                                 ['*DAY_CHL_chlor_a_4km*'])
    ans = 'A2019152.L3m_DAY_CHL_chlor_a_4km.nc'
    assert ans in [f['filename'] for f in file_list]
