# RSparkling


The **rsparkling** R package is an extension package for [sparkapi](https://github.com/rstudio/sparkapi) / [sparklyr](http://spark.rstudio.com) that creates an R front-end for a Spark package ([Sparking Water](https://spark-packages.org/package/h2oai/sparkling-water) from H2O).  This provides an interface to H2O's machine learning algorithms on Spark, using R.

This package implements basic functionality (creating an H2OContext, showing the H2O Flow interface, and converting between Spark DataFrames and H2O Frames). 


## Installation

The **rsparkling** R package requires the **h2o** and **sparklyr** R packages to run.  We always recommend the latest stable version of h2o / sparklyr, which you can find on the [H2O R Downloads page](http://www.h2o.ai/download/h2o/r) / [sparklyr page](http://spark.rstudio.com/index.html).


### Install RSparkling
The latest stable version of rsparkling can be installed as follows:

```r
library(devtools)
devtools::install_github("h2oai/sparkling-water", ref = "master", subdir = "/r/rsparkling")
``` 

The development version can be installed from the "rsparkling" branch as follows:

```r
library(devtools)
devtools::install_github("h2oai/sparkling-water", ref = "rsparkling", subdir = "/r/rsparkling")
``` 



## Connecting to Spark

If Spark needs to be installed, that can be done using the following sparklyr command:

``` r
library(sparklyr)
spark_install(version = "1.6.2")
```


The call to `library(rsparkling)` will make the H2O functions available on the R search path and will also ensure that the dependencies required by the Sparkling Water package are included when we connect to Spark. 

``` r
library(rsparkling)  # H2O Sparkling Water Machine Learning
```

We must create a Spark connection as follows:

``` r
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

As an example, let's copy the mtcars dataset to to Spark so we can access it from H2O Sparkling Water:

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

The use case we'd like to enable is calling the H2O algorithms and feature transformers directly on Spark DataFrames that we've manipulated with dplyr. This is indeed supported by the Sparkling Water package. Here is how you convert a Spark DataFrame into an H2O Frame:

``` r
mtcars_hf <- as_h2o_frame(sc, mtcars_tbl)
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

Using the same mtcars dataset, here is an example where we train a Gradient Boosting Machine (GBM) to predict "mpg".

### Prep data:
Define the response, `y`, and set of predictor variables, `x`:

``` r
y <- "mpg"
x <- setdiff(names(mtcars_hf), y)
```

Let's split the data into a train and test set using H2O.  The `h2o.splitFrame` function defaults to a 75-25 split (`ratios = 0.75`), but here we will make a 70-30 train-test split:

``` r
# Split the mtcars H2O Frame into train & test sets
splits <- h2o.splitFrame(mtcars_hf, ratios = 0.7, seed = 1)
```

### Training: 
Now train an H2O GBM using the training H2OFrame.

``` r
fit <- h2o.gbm(x = x, 
               y = y, 
               training_frame = splits[[1]],
               min_rows = 1,
               seed = 1)
print(fit)
```

```
Model Details:
==============

H2ORegressionModel: gbm
Model ID:  GBM_model_R_1474763476171_1 
Model Summary: 
  number_of_trees number_of_internal_trees model_size_in_bytes min_depth
1              50                       50               14807         5
  max_depth mean_depth min_leaves max_leaves mean_leaves
1         5    5.00000         17         21    18.64000


H2ORegressionMetrics: gbm
** Reported on training data. **

MSE:  0.001211724
RMSE:  0.03480983
MAE:  0.02761402
RMSLE:  0.001929304
Mean Residual Deviance :  0.001211724
```


### Model Performance:

We can evaluate the performance of the GBM by evaluating its performance on a test set.
 
``` r
perf <- h2o.performance(fit, newdata = splits[[2]])
print(perf)
```

```
H2ORegressionMetrics: gbm

MSE:  2.707001
RMSE:  1.645297
MAE:  1.455267
RMSLE:  0.08579109
Mean Residual Deviance :  2.707001
```


### Prediction: 

To generate predictions on a test set, you do the following.  This will return an H2OFrame with a single (or multiple) columns of predicted values.  If regression, it will be a single colum, if binary classification it will be 3 columns and in multi-class prediction it will be C+1 columns (where C is the number of classes).

``` r
pred_hf <- h2o.predict(fit, newdata = splits[[2]])
head(pred_hf)
```
```
   predict
1 21.39512
2 16.92804
3 15.19558
4 20.47695
5 20.47695
6 15.24433
```			


Now let's say you want to make this H2OFrame available to Spark.  You can convert an H2OFrame into a Spark DataFrame using the `as_spark_dataframe` function:

``` r
pred_sdf <- as_spark_dataframe(sc, pred_hf)
head(pred_sdf)
```
```
Source:   query [?? x 1]
Database: spark connection master=local[8] app=sparklyr local=TRUE

   predict
     <dbl>
1 21.39512
2 16.92804
3 15.19558
4 20.47695
5 20.47695
6 15.24433
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
