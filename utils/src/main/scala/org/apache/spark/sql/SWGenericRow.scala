package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.GenericRow

class SWGenericRow(override val values: Array[Any]) extends GenericRow(values)
