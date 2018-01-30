Calling H2O Algorithms
----------------------

1. Create the parameters object that holds references to input data and the parameters for the specific algorithm:

.. code:: scala

    val train: RDD = ...
    val valid: H2OFrame = ...

    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = valid
    gbmParams._response_column = "bikes"
    gbmParams._ntrees = 500
    gbmParams._max_depth = 6

2. Create a model builder:

.. code:: scala

    val gbm = new GBM(gbmParams)

3. Invoke the model build job and block until the end of computation (``trainModel`` is an asynchronous call by default):

.. code:: scala

    val gbmModel = gbm.trainModel.get
