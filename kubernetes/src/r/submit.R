library(sparklyr)

args = commandArgs(trailingOnly=TRUE)
master = args[1]
registryId = args[2]
version = args[3]
config <- spark_config()
config[["spark.executor.instances"]] <- 3
config[["spark.kubernetes.container.image"]] <- paste0(registryId, ".dkr.ecr.us-east-2.amazonaws.com/sw_kubernetes_repo/sparkling-water:r-", version)
spark_submit(master = master, file = "initTest.R", config = config)
