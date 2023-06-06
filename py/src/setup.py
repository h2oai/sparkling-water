#!/usr/bin/env python

from codecs import open
from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='h2o_pysparkling_SUBST_SPARK_MAJOR_VERSION',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version="SUBST_SW_VERSION",
    description='Sparkling Water integrates H2O\'s Fast Scalable Machine Learning with Spark',
    long_description=long_description,

    url='https://github.com/h2oai/sparkling-water',
    download_url='https://github.com/h2oai/sparkling-water/',
    author='H2O.ai',
    author_email='support@h2o.ai',
    license='Apache v2',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
SUBST_PYTHON_VERSIONS
    ],
    keywords='machine learning, data mining, statistical analysis, modeling, big data, distributed, parallel',

    # find python packages starting in the current directory
    packages=find_packages(),

    # run-time dependencies
    install_requires=[
        'requests',
        'tabulate'],

    # bundled binary packages
    package_data={'sparkling_water': ['*.jar'],
                  'h2o': ['version.txt', 'buildinfo.txt'],
                  'ai.h2o.sparkling': ['version.txt']},
)
