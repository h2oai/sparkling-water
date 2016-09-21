# RSparkling


RSparkling is a based on this proof of concept project by J.J. Allaire: [https://github.com/jjallaire/sparklingwater](https://github.com/jjallaire/sparklingwater) 

This package is an extension package for [sparkapi](https://github.com/rstudio/sparkapi) / [sparklyr](http://spark.rstudio.com) that creates an R front-end for a Spark package ([Sparking Water](https://spark-packages.org/package/h2oai/sparkling-water) from H2O).  This provides an interface to H2O's machine learning algorithms on Spark, using R.

This package implements only the most basic functionality (creating an H2OContext, showing the H2O Flow interface, and converting a Spark DataFrame to an H2O Frame). 


## Installation

The **rsparkling** R package requires the **h2o** and **sparklyr** R packages to run.  We always recommend the latest stable version of h2o / sparklyr, which you can find on the [H2O R Downloads page](http://www.h2o.ai/download/h2o/r) / [sparklyr page](http://spark.rstudio.com/index.html).


### Install RSparkling
The latest stable version of RSparkling can be installed as follows:

```r
library(devtools)
devtools::install_github("h2oai/sparkling-water/tree/master/r/rsparkling")
``` 

The development version can be installed from the "rsparkling" branch as follows:

```r
library(devtools)
devtools::install_github("h2oai/sparkling-water/tree/rsparkling/r/rsparkling")
``` 



## Connecting to Spark

First we connect to Spark. The call to `library(rsparkling)` will make the H2O functions available on the R search path and will also ensure that the dependencies required by the Sparkling Water package are included when we connect to Spark.

``` r
library(sparklyr)  # Spark + R
library(rsparkling)  # H2O Sparkling Water Machine Learning
sc <- spark_connect(master = "local")
```

## H2O Context and Flow

The call to `library(rsparkling)` automatically registered the Sparkling Water extension, which in turn specified that the [Sparkling Water Spark package](https://spark-packages.org/package/h2oai/sparkling-water) should be made available for Spark connections. Let's inspect the `H2OContext` for our Spark connection:

``` r
h2o_context(sc)
```

    ## <jobj[6]>
    ##   class org.apache.spark.h2o.H2OContext
    ##   
    ## Sparkling Water Context:
    ##  * H2O name: sparkling-water-jjallaire_-1482215501
    ##  * number of executors: 1
    ##  * list of used executors:
    ##   (executorId, host, port)
    ##   ------------------------
    ##   (driver,localhost,54323)
    ##   ------------------------
    ## 
    ##   Open H2O Flow in browser: http://127.0.0.1:54323 (CMD + click in Mac OSX)
    ## 

We can also view the H2O Flow web UI:

``` r
h2o_flow(sc)
```

## H2O with Spark DataFrames

Let's copy the mtcars dataset to to Spark so we can access it from Sparkling Water:

``` r
library(dplyr)
mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
mtcars_tbl
```

    ## Source:   query [?? x 11]
    ## Database: spark connection master=local[8] app=sparklyr local=TRUE
    ## 
    ##      mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
    ##    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
    ## 1   21.0     6 160.0   110  3.90 2.620 16.46     0     1     4     4
    ## 2   21.0     6 160.0   110  3.90 2.875 17.02     0     1     4     4
    ## 3   22.8     4 108.0    93  3.85 2.320 18.61     1     1     4     1
    ## 4   21.4     6 258.0   110  3.08 3.215 19.44     1     0     3     1
    ## 5   18.7     8 360.0   175  3.15 3.440 17.02     0     0     3     2
    ## 6   18.1     6 225.0   105  2.76 3.460 20.22     1     0     3     1
    ## 7   14.3     8 360.0   245  3.21 3.570 15.84     0     0     3     4
    ## 8   24.4     4 146.7    62  3.69 3.190 20.00     1     0     4     2
    ## 9   22.8     4 140.8    95  3.92 3.150 22.90     1     0     4     2
    ## 10  19.2     6 167.6   123  3.92 3.440 18.30     1     0     4     4
    ## # ... with more rows

The use case we'd like to enable is calling the H2O algorithms and feature transformers directly on Spark DataFrames that we've manipulated with dplyr. This is indeed supported by the Sparkling Water package. Here though we'll just convert the Spark DataFrame into an H2O Frame to prove that it's possible:

``` r
mtcars_hf <- as_h2o_frame(mtcars_tbl)
mtcars_hf
```

    ## <jobj[103]>
    ##   class water.fvec.H2OFrame
    ##   Frame frame_rdd_39 (32 rows and 11 cols):
    ##                        mpg  cyl                disp   hp                drat                  wt                qsec  vs  am  gear  carb
    ##     min               10.4    4                71.1   52                2.76               1.513                14.5   0   0     3     1
    ##    mean          20.090625    6          230.721875  146           3.5965625             3.21725  17.848750000000003   0   0     3     2
    ##  stddev  6.026948052089104    1  123.93869383138194   68  0.5346787360709715  0.9784574429896966  1.7869432360968436   0   0     0     1
    ##     max               33.9    8               472.0  335                4.93               5.424                22.9   1   1     5     8
    ## missing                0.0    0                 0.0    0                 0.0                 0.0                 0.0   0   0     0     0
    ##       0               21.0    6               160.0  110                 3.9                2.62               16.46   0   1     4     4
    ##       1               21.0    6               160.0  110                 3.9               2.875               17.02   0   1     4     4
    ##       2               22.8    4               108.0   93                3.85                2.32               18.61   1   1     4     1
    ##       3               21.4    6               258.0  110                3.08               3.215               19.44   1   0     3     1
    ##       4               18.7    8               360.0  175                3.15                3.44               17.02   0   0     3     2
    ##       5               18.1    6               225.0  105                2.76                3.46               20.22   1   0     3     1
    ##       6               14.3    8               360.0  245                3.21                3.57               15.84   0   0     3     4
    ##       7               24.4    4               146.7   62                3.69                3.19                20.0   1   0     4     2
    ##       8               22.8    4               140.8   95                3.92                3.15                22.9   1   0     4     2
    ##       9               19.2    6               167.6  123                3.92                3.44                18.3   1   0     4     4
    ##      10               17.8    6               167.6  123                3.92                3.44                18.9   1   0     4     4
    ##      11               16.4    8               275.8  180                3.07                4.07                17.4   0   0     3     3
    ##      12               17.3    8               275.8  180                3.07                3.73                17.6   0   0     3     3
    ##      13               15.2    8               275.8  180                3.07                3.78                18.0   0   0     3     3
    ##      14               10.4    8               472.0  205                2.93                5.25               17.98   0   0     3     4
    ##      15               10.4    8               460.0  215                 3.0               5.424               17.82   0   0     3     4
    ##      16               14.7    8               440.0  230                3.23               5.345               17.42   0   0     3     4
    ##      17               32.4    4                78.7   66                4.08                 2.2               19.47   1   1     4     1
    ##      18               30.4    4                75.7   52                4.93               1.615               18.52   1   1     4     2
    ##      19               33.9    4                71.1   65                4.22               1.835                19.9   1   1     4     1


## Sparkling Water: H2O Machine Learning

Here is an example where we train a Gradient Boosting Machine (GBM):

``` r
y <- "mpg"
x <- setdiff(names(mtcars_hf), y)
```

``` r
fit <- h2o.gbm(x = x, 
               y = y, 
               training_frame = mtcars_hf,
               nfolds = 2, 
               min_rows = 1)

prediction_hf <- h2o.predict(fit, mtcars_hf)

prediction_tbl <- as_spark_dataframe(prediction_hf)
prediction_tbl
```

## Logs & Disconnect

Look at the Spark log from R:

``` r
spark_log(sc, n = 100)
```

Now we disconnect from Spark, this will result in the H2OContext being stopped as well since it's owned by the spark shell process used by our Spark connection:

``` r
spark_disconnect(sc)
```
