Creating H2OFrame from an Existing Key
--------------------------------------

If the H2O cluster already contains a loaded ``H2OFrame`` referenced by
the key ``train.hex``, it is possible to reference it from Sparkling
Water by creating a proxy ``H2OFrame`` instance using the key as the
input:

.. code:: scala

    val trainHF = new H2OFrame("train.hex")
