package ai.h2o.sparkling.backend

import ai.h2o.sparkling.H2OConf

trait SparklingBackend {

  def startH2OCluster(conf: H2OConf): Unit

  def backendUIInfo: Seq[(String, String)]

  def epilog: String
}
