Data Sharing
------------

Sparkling Water enables transformation between different types of Spark
``RDD`` and H2O's ``H2OFrame``, and vice versa.

Conversion Design
~~~~~~~~~~~~~~~~~

When converting from ``H2OFrame`` to ``RDD``, a wrapper is created
around the ``H2OFrame`` to provide an RDD-like API. In this case, no
data is duplicated; instead, the data is served directly from the
underlying ``H2OFrame``.

Converting in the opposite direction (i.e, from Spark
``RDD``/``DataFrame`` to ``H2OFrame``) needs evaluation of data stored
in Spark ``RDD`` and transfer them from RDD storage into ``H2OFrame``.
However, data stored in ``H2OFrame`` is heavily compressed.

Exchanging the Data
~~~~~~~~~~~~~~~~~~~

The way how data is transferred between Spark and H2O differs on the used
Sparkling Water backend.

In the Internal Sparkling Water Backend, Spark and H2O share the same
JVM as is depicted on the following figure. |Data Sharing|

In the External Sparkling Water Backend, Spark and H2O are separated
clusters and data has to be send between these cluster over the network.

.. |Data Sharing| image:: doc/images/internal_backend_data_sharing.png

