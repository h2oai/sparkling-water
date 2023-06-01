package org.apache.spark.h2o.ui

case class H2OClusterInfo(
    localClientIpPort: String,
    flowURL: String,
    cloudHealthy: Boolean,
    cloudSecured: Boolean,
    cloudNodes: Array[String],
    extraBackendInfo: Seq[(String, String)],
    h2oStartTime: Long)
