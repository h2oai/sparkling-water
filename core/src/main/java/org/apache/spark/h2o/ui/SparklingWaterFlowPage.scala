package org.apache.spark.h2o.ui

import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.{UIUtils, WebUIPage}
import water.{H2O, H2ONode}

import scala.xml.{Node, NodeSeq}

/**
  * Sparkling Water info page.
  */
case class SparklingWaterInfoPage(parent: SparklingWaterUITab) extends WebUIPage("") {

  val hc = parent.hc

  override def render(request: HttpServletRequest): Seq[Node] = {
    val helpText =
      """
        |Sparkling Water runtime information.
      """.stripMargin
    
    val flowUrl = s"http://${hc.h2oLocalClient}"

    val swInfoTable = UIUtils.listingTable(
      propertyHeader, h2oRow, swInfo(), fixedWidth = true)
    val swPropertiesTable = UIUtils.listingTable(
      propertyHeader, h2oRow, swProperties(), fixedWidth = true)
    val h2oInfoTable = UIUtils.listingTable(
      propertyHeader, h2oRow, h2oInfo, fixedWidth = true)

    val content =
      <div>
        <ul class="unstyled">
          <li><strong>Uptime:</strong>
            {UIUtils.formatDuration(System.currentTimeMillis() - H2O.START_TIME_MILLIS.get())}
          </li>
          <li><strong>Health:</strong>
            {
              if (H2O.CLOUD.healthy()) "\u2714" else "\u2716"
            }
          </li>
          <li><strong>Nodes:</strong>
            {H2O.getCloudSize}
          </li>
          <li><a href={flowUrl}><strong>Flow UI</strong></a></li>
        </ul>
      </div>
      <span>
        <h4>Sparkling Water</h4> {swInfoTable}
        <h4>Sparkling Water Properties</h4> {swPropertiesTable}
        <h4>H2O Build Information</h4> {h2oInfoTable}
      </span>

    UIUtils.headerSparkPage("Sparkling Water", content, parent, helpText = Some(helpText))
  }

  private def propertyHeader = Seq("Name", "Value")
  private def h2oRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>

  private def swInfo(): Seq[(String, String)] = Seq(
    ("Flow UI", s"http://${hc.h2oLocalClient}"),
    ("Nodes", H2O.CLOUD.members().map(node => node.getIpPortString).mkString(","))
  ) ++ parent.hc.backend.backendUIInfo

  private def swProperties(): Seq[(String, String)] =
    hc._conf.getAll.filter(_._1.startsWith("spark.ext.h2o"))

  private def h2oInfo(): Seq[(String, String)] = Seq(
    ("H2O Build Version", H2O.ABV.projectVersion()),
    ("H2O Git Branch", H2O.ABV.branchName()),
    ("H2O Git SHA", H2O.ABV.lastCommitHash()),
    ("H2O Git Describe", H2O.ABV.describe()),
    ("H2O Build By", H2O.ABV.compiledBy()),
    ("H2O Build On", H2O.ABV.compiledOn())
  )

}
