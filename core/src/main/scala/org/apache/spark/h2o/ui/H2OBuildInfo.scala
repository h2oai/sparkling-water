package org.apache.spark.h2o.ui

case class H2OBuildInfo(
    h2oBuildVersion: String,
    h2oGitBranch: String,
    h2oGitSha: String,
    h2oGitDescribe: String,
    h2oBuildBy: String,
    h2oBuildOn: String)
