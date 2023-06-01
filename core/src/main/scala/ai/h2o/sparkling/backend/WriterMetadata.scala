package ai.h2o.sparkling.backend

import java.util.TimeZone

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType

case class WriterMetadata(
    conf: H2OConf,
    frameId: String,
    expectedTypes: Array[ExpectedType],
    maxVectorSizes: Array[Int],
    timezone: TimeZone,
    featureColsForConstCheck: Option[Seq[String]])
