#!/usr/bin/env python

"""Tests for `OceanColor` package."""

import pytest

from OceanColor.storage import parse_filename


def test_parse_filename_AL2():
    filename = "A2011010000000.L2_LAC_OC.nc"
    descriptors = parse_filename(filename)

    ans = {'platform': 'A', 'year': '2011', 'doy': '010', 'time': '000000', 'mode': 'L2', 'instrument': None}
    for a in ans:
        assert descriptors[a] == ans[a]

def test_parse_filename_AL3m():
    filename = "T2004006.L3m_DAY_CHL_chlor_a_4km.nc"
    descriptors = parse_filename(filename)

    ans = {'platform': 'T', 'year': '2004', 'doy': '006', 'time': None, 'mode': 'L3m', 'instrument': None}
    for a in ans:
        assert descriptors[a] == ans[a]
