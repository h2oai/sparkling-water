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

case object StructsOnlySchema extends TestFrameUtils.SchemaHolder {
  @transient lazy val schema: StructType = {
    StructType(
      Seq(
        StructField("field_NFSCCWY41YRP27", LongType),
        StructField("field_L2TY79YNBTEVD0T", LongType),
        StructField("field_0I99MMJ9AEHY", LongType),
        StructField("field_VSUB21AD5B", StringType),
        StructField(
          "field_WPYS9H8QYI",
          StructType(Seq(
            StructField("field_9AHKOGTE4", LongType),
            StructField("field_46N5KAKZSQ0", LongType),
            StructField("field_7UZVZAL9UJ", LongType),
            StructField("field_UZTB4TG2PX7HIZ", LongType),
            StructField("field_DINT7HBXXPBGFQZ", LongType),
            StructField("field_SD0545LQR9HB", LongType),
            StructField("field_XZTXP2LMLLR", LongType),
            StructField("field_I5OJPCUFY4B6Q3V", LongType),
            StructField("field_GHEYZJXM36Y", LongType),
            StructField("field_2MAZNTKZYV", LongType)))),
        StructField(
          "field_5V460N466V",
          StructType(Seq(StructField("field_8QIMSA", LongType), StructField("field_NKFT9M5", StringType)))),
        StructField(
          "field_5V460N466AB",
          StructType(Seq(
            StructField("field_H8PSYZOGTVIC", LongType),
            StructField("field_SV5BGKH66JY5", LongType),
            StructField("field_Q90MIRU", LongType),
            StructField("field_WNZUA8FO1JLTW", LongType),
            StructField("field_GQVKL09AHTVYB0S", BooleanType),
            StructField("field_08TGFSZW8QIDK5X", BooleanType),
            StructField("field_ZE344PP2SY", BooleanType),
            StructField("field_P865N", StringType),
            StructField(
              "field_GOWL7",
              StructType(Seq(
                StructField("field_MZW55SC6R8BLMHQ", StringType),
                StructField("field_GFFT5SMPX9B3", LongType),
                StructField("field_YTKZF69", StringType),
                StructField("field_1A03E01", StringType),
                StructField("field_7DZCUC9Y", StringType)))),
            StructField(
              "field_HUNIPV4GU14UNK",
              StructType(
                Seq(StructField("field_AJT2H1JRVG1Y", StringType), StructField("field_O3EWUIF53YGACG4", StringType)))),
            StructField(
              "field_DBWW318U",
              StructType(Seq(
                StructField("field_YPXL4A9BKNZXUL6", StringType),
                StructField("field_A8AHFV6N0TQ4VV", StringType),
                StructField("field_C76H83G", StringType),
                StructField("field_1TH7QY18", StringType)))),
            StructField("field_A6BT9NO7I7FH", StructType(Seq(StructField("field_660J3JU7", BooleanType)))),
            StructField(
              "field_JXZFOCD",
              StructType(Seq(
                StructField("field_ME099TG", StringType),
                StructField("field_6HFHLRNB2R91X8C", StringType),
                StructField("field_BKJAHJ30", StringType),
                StructField("field_1TJFFVVM", LongType)))),
            StructField(
              "field_E8I8KFH8T8P8QBB",
              StructType(Seq(
                StructField("field_R7E6BIB3YW", StringType),
                StructField("field_11HRZKD6UPR", StringType),
                StructField("field_4XSU04BIRS8R46U", StringType),
                StructField("field_KB5JMNFH", StringType)))),
            StructField(
              "field_QB34G7X2QZNOU7V",
              StructType(
                Seq(StructField("field_AYWQZY7WE8M4W", BooleanType), StructField("field_BJYTJ4SX", BooleanType)))),
            StructField(
              "field_WXV4IKCHWQPZUB",
              StructType(Seq(
                StructField("field_YGPNCZZ8CCQJ08", StringType),
                StructField("field_9YY4Z88MCDCEI", LongType),
                StructField("field_DMBU5BO3Q", StringType)))),
            StructField(
              "field_CEW4JKV47G",
              StructType(
                Seq(StructField("field_9HI2CLD96658", StringType), StructField("field_V6NMVFBPZTK0IQ", StringType)))),
            StructField(
              "field_5HFZCI5KDM",
              StructType(
                Seq(StructField("field_1JJ8V0Y2K", LongType), StructField("field_W06XUE4JNETUKK6", StringType)))),
            StructField(
              "field_CN2PQ748U1OBJ",
              StructType(Seq(StructField("field_HKTXKOYLO6RFQ9H", LongType), StructField("field_V35TZP", LongType)))),
            StructField("field_HSFB2", StringType),
            StructField(
              "field_9VF1PE9PFDSYAP8",
              StructType(Seq(
                StructField("field_0H3Z85", StringType),
                StructField("field_0FQNKVOQ8I99CIN", StringType),
                StructField("field_QP4DDICXWEO1SG", StringType)))),
            StructField(
              "field_9C45YGQMZ4XQ1M",
              StructType(Seq(
                StructField("field_YZGX0N5", LongType),
                StructField("field_YL1Y2", LongType),
                StructField("field_F3W3K1B1", LongType),
                StructField("field_K4PSS48", LongType),
                StructField("field_VVXHDHMLV4K", LongType),
                StructField("field_7QTPEQU5WV", LongType),
                StructField("field_GQ57MA9B2KEW", LongType),
                StructField("field_7X2KRVAQUP4JHQD", LongType),
                StructField("field_7RI0C3GEJZ", LongType),
                StructField("field_Q9Y0DW2SEOAZEWW", LongType),
                StructField("field_35IZ8UVC6", LongType)))),
            StructField("field_OONK074H2", StringType)))),
        StructField("field_V5Z6FZQY3CP", StringType),
        StructField("field_JQRRX0W0GRF", StringType),
        StructField("field_M01P8XWSDBOCKJ", StringType),
        StructField("field_RDC7P0", StringType)))
  }
}
