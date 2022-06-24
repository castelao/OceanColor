#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

setup(
    packages=find_packages(include=['OceanColor', 'OceanColor.*']),
)
