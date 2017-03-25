Launching iPython Examples
=========================

## Prerequisites:

- Python 2.7

---

Install iPython Notebook
-------------------------

1. Download pip, a Python package manager (if it's not already installed):

    `$ sudo easy_install pip`

2. Install iPython using pip install:

    `$ sudo pip install "ipython[notebook]"`

---

Install dependencies
--------------------

This module uses requests and tabulate modules, both of which are available on pypi, the Python package index.

    $ sudo pip install requests
    $ sudo pip install tabulate
  
---

Install and Launch H2O
----------------------

To use H2O in Python, follow the instructions on the **Install in Python** tab after selecting the H2O version on the [H2O Downloads page](http://h2o.ai/download). 

Launch H2O outside of the iPython notebook. You can do this in the top directory of your H2O build download. The version of H2O running must match the version of the H2O Python module for Python to connect to H2O. 
To access the H2O Web UI, go to [https://localhost:54321](https://localhost:54321) in your web browser.

---

Open Demos Notebook
-------------------
