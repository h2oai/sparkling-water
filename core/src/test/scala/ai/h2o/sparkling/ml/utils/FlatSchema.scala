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

case object FlatSchema extends TestFrameUtils.SchemaHolder {
  @transient lazy val schema: StructType = StructType(
    Seq(
      StructField("field_9AHKOGTE4", LongType),
      StructField("field_46N5KAKZSQ0", LongType),
      StructField("field_7UZVZAL9UJ", LongType),
      StructField("field_UZTB4TG2PX7HIZ", LongType),
      StructField("field_DINT7HBXXPBGFQZ", LongType),
      StructField("field_SD0545LQR9HB", LongType),
      StructField("field_XZTXP2LMLLR", LongType),
      StructField("field_I5OJPCUFY4B6Q3V", LongType),
      StructField("field_GHEYZJXM36Y", LongType),
      StructField("field_2MAZNTKZYV", LongType),
      StructField("field_ATREHWBOH214EGZ", StringType),
      StructField("field_RJ8HBYCAZKAHHJ8", StringType),
      StructField("field_LP6RHDLO1NMX8MJ", StringType),
      StructField("field_GDPGNE", StringType),
      StructField("field_QKYJKMQ55", BooleanType),
      StructField("field_H4D6UGFRP", StringType),
      StructField("field_YUMGB", StringType),
      StructField("field_25V6QVQAGRJE", StringType),
      StructField("field_CH11YW16FKIRZVD", StringType),
      StructField("field_JV0J7OADPC6ZD1D", StringType),
      StructField("field_HPCS43BFOKIWI", StringType),
      StructField("field_KA7TIMQU2R0NMU3", BooleanType),
      StructField("field_JIHXIXLOS", StringType),
      StructField("field_UTU8G58MI7MB", StringType),
      StructField("field_GBLNXW5AYN", StringType),
      StructField("field_EV8GOZ", StringType),
      StructField("field_9XELCFMLX", StringType),
      StructField("field_ACXEJ2LP", StringType),
      StructField("field_EA3IQ4", LongType),
      StructField("field_DAKOPYKD", LongType),
      StructField("field_KCD97", LongType),
      StructField("field_0ZA1LY", LongType),
      StructField("field_9YOS47D", BooleanType),
      StructField("field_O0TLM1MBFT", BooleanType),
      StructField("field_3YIYXGZHQ3O5", StringType),
      StructField("field_VMGT6PFPO", LongType),
      StructField("field_92DNXTAWX", StringType)))
}
