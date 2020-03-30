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

package ai.h2o.sparkling.ml.utils

import org.apache.spark.h2o.utils.TestFrameUtils
import org.apache.spark.sql.types._

case object FlatArraysOnlySchema extends TestFrameUtils.SchemaHolder {
  @transient lazy val schema: StructType = StructType(
    Seq(
      StructField("field_GHEYZJXM36Y", ArrayType(LongType)),
      StructField("field_2MAZNTKZYV", ArrayType(LongType)),
      StructField("field_ATREHWBOH214EGZ", ArrayType(StringType)),
      StructField("field_RJ8HBYCAZKAHHJ8", ArrayType(StringType)),
      StructField("field_LP6RHDLO1NMX8MJ", ArrayType(StringType)),
      StructField("field_GDPGNE", ArrayType(StringType)),
      StructField("field_QKYJKMQ55", ArrayType(BooleanType)),
      StructField("field_H4D6UGFRP", ArrayType(StringType)),
      StructField("field_9XELCFMLX", ArrayType(StringType)),
      StructField("field_ACXEJ2LP", ArrayType(StringType)),
      StructField("field_0ZA1LY", ArrayType(LongType)),
      StructField("field_9YOS47D", ArrayType(BooleanType)),
      StructField("field_O0TLM1MBFT", ArrayType(BooleanType)),
      StructField("field_3YIYXGZHQ3O5", ArrayType(StringType)),
      StructField("field_VMGT6PFPO", ArrayType(LongType)),
      StructField("field_92DNXTAWX", ArrayType(StringType))))
}
