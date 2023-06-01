package ai.h2o.sparkling

import org.scalatest.Suite

/**
  * Helper trait to simplify initialization and termination of H2O contexts.
  */
trait SharedH2OTestContext extends SparkTestContext {
  self: Suite =>

  @transient var hc: H2OContext = _

  override def beforeAll() {
    super.beforeAll()
    hc = H2OContext.getOrCreate(new H2OConf().setClusterSize(1))
  }
}
