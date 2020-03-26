context("Test transformations of H2O frames and Spark frames in rsparkling using H2OContext class")


config <- spark_config()
config <- c(config, list(
  "spark.hadoop.yarn.timeline-service.enabled" = "false",
  "spark.ext.h2o.external.cluster.size" = "1",
  "spark.ext.h2o.backend.cluster.mode" = Sys.getenv("spark.ext.h2o.backend.cluster.mode"),
  "sparklyr.gateway.connect.timeout" = 240,
  "sparklyr.gateway.start.timeout" = 240,
  "sparklyr.backend.timeout" = 240,
  "spark.ext.h2o.external.start.mode" = "auto",
  "spark.ext.h2o.external.disable.version.check" = "true"
))

test_that("Test transformation from h2o frame to data frame", {
  sc <- spark_connect(master = "local[*]", config = config)
  df <- as.data.frame(t(c(1, 2, 3, 4, "A"))) # workaround for sparklyr#316
  sdf <- copy_to(sc, df, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  hf <- hc$asH2OFrame(sdf)
  sdf2 <- hc$asSparkFrame(hf)

  expect_equal(sdf_nrow(sdf2), nrow(hf))
  expect_equal(sdf_ncol(sdf2), ncol(hf))
  expect_equal(colnames(sdf2), colnames(hf))
})

test_that("Test transformation of a spark data_frame of bools to an h2o frame of bools", {
  sc <- spark_connect(master = "local[*]", config = config)
  df <- as.data.frame(t(c(TRUE, FALSE, TRUE, FALSE)))
  sdf <- copy_to(sc, df, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  hf <- hc$asH2OFrame(sdf)

  expect_equal(hf[1, 1], 1)
  expect_equal(hf[1, 2], 0)
  expect_equal(hf[1, 3], 1)
  expect_equal(hf[1, 4], 0)
})

test_that("Test transformation of a spark data_frame of complex types to an h2o frame of complex types", {
  sc <- spark_connect(master = "local[*]", config = config)
  n <- c(2)
  s <- c("aa")
  b <- c(TRUE)
  df <- data.frame(n, s, b)
  sdf <- copy_to(sc, df, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  hf <- hc$asH2OFrame(sdf)

  expect_equal(hf[1, 1], 2)
  expect_equal(hf[1, 2], "aa")
  expect_equal(hf[1, 3], 1)
})

test_that("Test transformation of a spark data_frame of float types to an h2o frame of floats", {
  sc <- spark_connect(master = "local[*]", config = config)
  df <- as.data.frame(t(c(1.5, 1.3333333333, 178.5555)))
  sdf <- copy_to(sc, df, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  hf <- hc$asH2OFrame(sdf)

  expect_equal(hf[1, 1], 1.5)
  expect_equal(hf[1, 2], 1.3333333333)
  expect_equal(hf[1, 3], 178.5555)
})

test_that("Test transformation of a spark data_frame of int types to an h2o frame of ints", {
  sc <- spark_connect(master = "local[*]", config = config)
  df <- as.data.frame(t(c(1, 125, 1778)))
  sdf <- copy_to(sc, df, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  hf <- hc$asH2OFrame(sdf)

  expect_equal(hf[1, 1], 1)
  expect_equal(hf[1, 2], 125)
  expect_equal(hf[1, 3], 1778)
})

test_that("Test transformation of a spark data_frame of str types to an h2o frame of str", {
  sc <- spark_connect(master = "local[*]", config = config)
  df <- as.data.frame(t(c("A", "B", "C")))
  sdf <- copy_to(sc, df, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  hf <- hc$asH2OFrame(sdf)

  expect_equal(hf[1, 1], "A")
  expect_equal(hf[1, 2], "B")
  expect_equal(hf[1, 3], "C")
})

test_that("Test transformation from dataframe to h2o frame", {
  sc <- spark_connect(master = "local[*]", config = config)
  mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  mtcars_hf <- hc$asH2OFrame(mtcars_tbl)

  expect_equal(sdf_nrow(mtcars_tbl), nrow(mtcars_hf))
  expect_equal(sdf_ncol(mtcars_tbl), ncol(mtcars_hf))
  expect_equal(colnames(mtcars_tbl), colnames(mtcars_hf))
})

test_that("Test transformation from dataframe to h2o frame", {
  sc <- spark_connect(master = "local[*]", config = config)
  mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
  hc <- H2OContext.getOrCreate()
  mtcars_hf_name <- hc$asH2OFrame(mtcars_tbl, h2oFrameName = "frame1")

  expect_equal(h2o.getId(mtcars_hf_name), "frame1")
})
