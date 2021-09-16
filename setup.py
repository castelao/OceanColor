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

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Guilherme Castelão",
    author_email='guilherme@castelao.net',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    description="Deal with NASA Ocean Color data (search and download)",
    entry_points={
        'console_scripts': [
            'OceanColor=OceanColor.cli:main',
        ],
    },
    install_requires=requirements(),
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='NASA Ocean Color chlorophyll oceanography matchup',
    name='OceanColor',
    packages=find_packages(include=['OceanColor', 'OceanColor.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/castelao/OceanColor',
    version="0.0.9",
    zip_safe=False,
    extras_require = {
        'parallel': ["loky>=2.8"],
    }
)
