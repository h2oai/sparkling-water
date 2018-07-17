#!/usr/bin/env python

from setuptools import setup, find_packages
from codecs import open
from os import path
here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Get the version from the relevant file
with open(path.join(here, 'pysparkling/version.txt'), encoding='utf-8') as f:
    version = f.read()

setup(
    name = 'h2o_pysparkling_' + version[:version.rindex(".")],

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version = version,
    description = 'Sparkling Water integrates H2O\'s Fast Scalable Machine Learning with Spark',
    long_description = long_description,

    url = 'https://github.com/h2oai/sparkling-water',
    download_url = 'https://github.com/h2oai/sparkling-water/',
    author = 'H2O.ai',
    author_email = 'support@h2o.ai',
    license = 'Apache v2',
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    keywords = 'machine learning, data mining, statistical analysis, modeling, big data, distributed, parallel',


    # find python packages starting in the current directory
    packages = find_packages(),

    # run-time dependencies
    install_requires=['requests', 'tabulate', 'future', 'colorama>=0.3.8', 'pyspark>=2.2.0,<=2.2.2'],

    # bundled binary packages
    package_data={'sparkling_water': ['*.jar'],
                  'h2o': ['version.txt', 'buildinfo.txt'],
                  'pysparkling': ['version.txt']},
)

