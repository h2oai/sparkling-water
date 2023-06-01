package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.extensions.serde.ExpectedTypes
import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType

import scala.collection.mutable

/**
  * This class is not thread safe.
  */
private[backend] class CategoricalDomainBuilder(expectedTypes: Array[ExpectedType]) {

  private def categoricalIndexes: Array[Int] =
    for ((eType, index) <- expectedTypes.zipWithIndex if eType == ExpectedTypes.Categorical) yield index

  private val indexMapping = categoricalIndexes.zipWithIndex.toMap

  private val domains = (0 until indexMapping.size).map(_ => mutable.LinkedHashMap[String, Int]()).toArray

  /**
    * The method adds string value to a corresponding categorical domain and returns position of the value within
    * the domain. If the value is already there, returns just the original position and doesn't make any update.
    * @param value String value
    * @param columnIndex Index of a column determining the categorical domain.
    *                    Indexing also includes columns of other types.
    * @return Index of the value within the categorical domain.
    */
  def addStringToDomain(value: String, columnIndex: Int): Int = {
    val domainIndex = indexMapping(columnIndex)
    val domain = domains(domainIndex)
    domain.getOrElseUpdate(value, domain.size)
  }

  def getDomains(): Array[Array[String]] = domains.map(_.keys.toArray)
}
