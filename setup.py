#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

def requirements():
    with open('requirements.txt') as f:
        return f.read()

setup(
    entry_points={
        'console_scripts': [
            'OceanColor=OceanColor.cli:main',
        ],
    },
    install_requires=requirements(),
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    name='OceanColor',
    packages=find_packages(include=['OceanColor', 'OceanColor.*']),
    zip_safe=False,
)
