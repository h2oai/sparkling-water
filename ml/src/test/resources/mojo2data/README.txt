-------------------------------------------------------------------------------

                H2O DRIVERLESS AI MOJO PIPELINE

-------------------------------------------------------------------------------

This folder contains a MOJO pipeline and Java runtime for deploying models
built with H2O Driverless AI. It enables low-latency, high-throughput scoring
on a broad range of hardware/software platforms with minimal requirements.

-------------------------------------------------------------------------------
REQUIREMENTS:

  - Java 8 runtime

-------------------------------------------------------------------------------
DIRECTORY LISTING:

  run_example.sh            An bash script to score a sample test set.

  pipeline.mojo             Standalone scoring pipeline in MOJO format.

  mojo2-runtime.jar         MOJO Java runtime.

  example.csv               Sample test set (synthetic, of the correct format).

-------------------------------------------------------------------------------
QUICKSTART:

To score all rows in the sample test set (example.csv) with the MOJO pipeline:

    $ bash run_example.sh

To score a specific test set `example.csv` with MOJO pipeline `pipeline.mojo`:

    $ bash run_example.sh pipeline.mojo example.csv

To run Java application for data transformation directly:

   $ java -cp mojo2-runtime.jar ai.h2o.mojos.ExecuteMojo pipeline.mojo example.csv

