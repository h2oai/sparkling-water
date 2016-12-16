/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
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
