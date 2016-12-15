package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

class AnyType(val value: String, val t: String) extends Serializable

class AnyTypeUDT extends UserDefinedType[AnyType] {

  UDTRegistration.register("org.apache.spark.sql.AnyType", "org.apache.spark.sql.AnyTypeUDT")

  override def sqlType: DataType = ArrayType(DataTypes.StringType, containsNull = false)

  override def serialize(p: AnyType): GenericArrayData = {
    val output = new Array[Any](2)
    output(0) = p.value
    output(1) = p.t
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): AnyType = {
    datum match {
      case values: ArrayData =>
        new AnyType(values.get(0, DataTypes.StringType).toString, values.get(1, DataTypes.StringType).toString)
    }
  }

  override def userClass: Class[AnyType] = classOf[AnyType]

  private[spark] override def asNullable: AnyTypeUDT = this
}