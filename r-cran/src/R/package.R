spark_dependencies <- function(spark_version, scala_version, ...) {
  content <- read.csv(url("http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/latest"),
                      header = FALSE)
  version <- as.vector(content[1, 1])
  stop(paste0('You are using RSParkling installed from CRAN, but that version is not maintained anymore. Please install latest RSparkling.

  For Spark 2.4 -> install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/', version, '/R")
  For Spark 2.3 -> install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/', version, '/R")
  For Spark 2.2 -> install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/', version, '/R")
  For Spark 2.1 -> install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/', version, '/R")
  '))
  spark_dependency(jars = c())
}

.onLoad <- function(libname, pkgname) {
  register_extension(pkgname)
}
