#!/usr/bin/env python

from setuptools import setup, find_packages
from codecs import open
from os import path
here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Get the version from the relevant file
with open(path.join(here, 'build', 'version.txt'), encoding='utf-8') as f:
    version = f.read()

setup(
    name='h2o_pysparkling_'+version[:version.rindex(".")],

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=version,
    description='Sparkling Water integrates H2O\'s Fast Scalable Machine Learning with Spark',
    long_description=long_description,

    url='https://github.com/h2oai/sparkling-water',
    download_url='https://github.com/h2oai/sparkling-water/',
    author='H2O.ai',
    author_email='support@h2o.ai',
    license='Apache v2',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='machine learning, data mining, statistical analysis, modeling, big data, distributed, parallel',


    package_dir={ 'pysparkling' : 'pysparkling', 'h2o' : 'build/h2o', 'sparkling_water':'build/sparkling_water'},
    # find python packages starting in the current directory
    packages=find_packages(exclude=['tests*'])+find_packages(where="build"),

    # run-time dependencies
    install_requires=['six','future', 'requests', 'tabulate'],
    package_data={'sparkling_water': ['*.jar']},
)
