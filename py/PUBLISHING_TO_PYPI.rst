This is tutorial how to publish PySparkling on PyPi
===================================================

1 - Build Sparkling Water. This is necessary step as the build process prepares additional packages which needs to be
in the python package

- sparkling_water package containing sparkling-water jar
- h2o package containing h2o python files

.. code:: bash

    ./gradlew build -x check

2 - Ensure you have ``.pypirc`` file in the ~ ( home) directory with the following content:

.. code:: xml

    [distutils]
    index-servers =
      pypi
      pypitest

    [pypi]
    username=your_username_to_pypi
    password=your_password_to_pypi

    [pypitest]
    repository=https://test.pypi.org/legacy/
    username=your_username_to_testpypi
    password=your_password_to_testpypi


There are actually two servers - pypi and pypitest. Pypi is live server where the package needs to be uploaded
and pypitest is just for testing that our package uploads well on the server and has correct format.

3 - Go to ``py/build/pkg``

4 - Test that the package successfully uploads to pypi test by running:

.. code:: bash

    python setup.py sdist upload -r pypitest

5 - If everything goes well, upload the package to pypi live:

.. code:: bash

    python setup.py sdist upload -r pypi

6 - That's it!