package ai.h2o.sparkling.bench

import ai.h2o.sparkling.TestUtils
import org.apache.spark.sql.types._

case object FlatArraysOnlySchema extends TestUtils.SchemaHolder {
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
