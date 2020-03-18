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

case object ComplexSchema extends TestFrameUtils.SchemaHolder {
  @transient lazy val schema: StructType = {
    StructType(Seq(
      StructField("field_NFSCCWY41YRP27", LongType),
      StructField("field_L2TY79YNBTEVD0T", LongType),
      StructField("field_0I99MMJ9AEHY", LongType),
      StructField("field_VSUB21AD5B", StringType),
      StructField("field_WPYS9H8QYI", StructType(Seq(
        StructField("field_9AHKOGTE4", LongType),
        StructField("field_46N5KAKZSQ0", LongType),
        StructField("field_7UZVZAL9UJ", LongType),
        StructField("field_UZTB4TG2PX7HIZ", LongType),
        StructField("field_DINT7HBXXPBGFQZ", LongType),
        StructField("field_SD0545LQR9HB", LongType),
        StructField("field_XZTXP2LMLLR", LongType),
        StructField("field_I5OJPCUFY4B6Q3V", LongType),
        StructField("field_GHEYZJXM36Y", LongType),
        StructField("field_2MAZNTKZYV", LongType)
      ))),
      StructField("field_S5765", StructType(Seq(
        StructField("field_I8HOE0KR8RR34G6", StringType),
        StructField("field_X8YYK7IXAIZPF", StringType),
        StructField("field_APKTL1K66S", StringType),
        StructField("field_3UD6Y1", StringType),
        StructField("field_0AXFH3MKIPA", StringType),
        StructField("field_SEZ40Q", StringType),
        StructField("field_06RHE9", StructType(Seq(
          StructField("field_GCW6K", LongType),
          StructField("field_G2CRAPO", LongType),
          StructField("field_27Y7982AE8", LongType),
          StructField("field_P0TJBJIOJPL", LongType),
          StructField("field_8Q4I13", LongType),
          StructField("field_M2OC6IVCE9QS", StringType),
          StructField("field_6TVLNM7", StringType),
          StructField("field_0DY6HI43", StringType),
          StructField("field_LFJXH3HCYTKY1W", LongType),
          StructField("field_IMLEGNYTXSGHH5W", StringType),
          StructField("field_HJLOK", BooleanType)
        ))),
        StructField("field_DHDMS3K", BooleanType)
      ))),
      StructField("field_1RMREDHZF4AQ", StructType(Seq(
        StructField("field_5ZZPFEQA4JUEUQ", StringType),
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
        StructField("field_H3PXP6NG", StringType),
        StructField("field_146N3T9GQKP", StringType),
        StructField("field_0XZCE8KGZY", StringType)
      ))),
      StructField("field_S1YYUQ7E62FYA", StructType(Seq(
        StructField("field_I215HRZFWT3RV", StringType),
        StructField("field_X4LK13PO", StringType),
        StructField("field_V6P8MR1TU3QHDU", StringType),
        StructField("field_KBPAGC3I70", StringType),
        StructField("field_N61ICQO0UD", StringType),
        StructField("field_T13BLECKQ", StringType),
        StructField("field_L8L6O8WKFCY0", StringType),
        StructField("field_9UPC9H5", StringType),
        StructField("field_R82ACK8GHTC", StringType),
        StructField("field_J6B3ENEQ0TAM", StringType),
        StructField("field_F1B27T4B55WTK", StringType)
      ))),
      StructField("field_5V460N466V", ArrayType(
        StructType(Seq(
          StructField("field_8QIMSA", LongType),
          StructField("field_NKFT9M5", StringType)
        ))
      )),
      StructField("field_1CGUTYW", ArrayType(
        StructType(Seq(
          StructField("field_H8PSYZOGTVIC", LongType),
          StructField("field_SV5BGKH66JY5", LongType),
          StructField("field_Q90MIRU", LongType),
          StructField("field_WNZUA8FO1JLTW", LongType),
          StructField("field_GQVKL09AHTVYB0S", BooleanType),
          StructField("field_08TGFSZW8QIDK5X", BooleanType),
          StructField("field_ZE344PP2SY", BooleanType),
          StructField("field_P865N", StringType),
          StructField("field_J7Y1OXRIYNYD", StructType(Seq(
            StructField("field_9BKWQN0BR5EJIE", StringType),
            StructField("field_HULHNCPOBEI7", StringType),
            StructField("field_P7Q66AJ", StringType),
            StructField("field_GYU6V0268E5P", StringType),
            StructField("field_OEEZCC59VRZ7A", StringType),
            StructField("field_5ILJKAE3IAV9YD", StringType),
            StructField("field_C4QMZTAVM", StringType),
            StructField("field_7ODC1G26483B", StringType),
            StructField("field_OPL443M6Y", StringType)
          ))),
          StructField("field_GOWL7", StructType(Seq(
            StructField("field_MZW55SC6R8BLMHQ", StringType),
            StructField("field_GFFT5SMPX9B3", LongType),
            StructField("field_YTKZF69", StringType),
            StructField("field_1A03E01", StringType),
            StructField("field_7DZCUC9Y", StringType)
          ))),
          StructField("field_HUNIPV4GU14UNK", StructType(Seq(
            StructField("field_AJT2H1JRVG1Y", StringType),
            StructField("field_O3EWUIF53YGACG4", StringType)
          ))),
          StructField("field_DBWW318U", StructType(Seq(
            StructField("field_YPXL4A9BKNZXUL6", StringType),
            StructField("field_A8AHFV6N0TQ4VV", StringType),
            StructField("field_C76H83G", StringType),
            StructField("field_1TH7QY18", StringType)
          ))),
          StructField("field_A6BT9NO7I7FH", StructType(Seq(
            StructField("field_660J3JU7", BooleanType)
          ))),
          StructField("field_JXZFOCD", StructType(Seq(
            StructField("field_ME099TG", StringType),
            StructField("field_6HFHLRNB2R91X8C", StringType),
            StructField("field_BKJAHJ30", StringType),
            StructField("field_1TJFFVVM", LongType)
          ))),
          StructField("field_AQAOS4ILNE29EW", ArrayType(
            StructType(Seq(
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
              StructField("field_92DNXTAWX", StringType)
            ))
          )),
          StructField("field_E8I8KFH8T8P8QBB", ArrayType(
            StructType(Seq(
              StructField("field_R7E6BIB3YW", StringType),
              StructField("field_11HRZKD6UPR", StringType),
              StructField("field_4XSU04BIRS8R46U", StringType),
              StructField("field_KB5JMNFH", StringType)
            ))
          )),
          StructField("field_QB34G7X2QZNOU7V", StructType(Seq(
            StructField("field_AYWQZY7WE8M4W", BooleanType),
            StructField("field_BJYTJ4SX", BooleanType)
          ))),
          StructField("field_WXV4IKCHWQPZUB", StructType(Seq(
            StructField("field_YGPNCZZ8CCQJ08", StringType),
            StructField("field_9YY4Z88MCDCEI", LongType),
            StructField("field_DMBU5BO3Q", StringType)
          ))),
          StructField("field_CEW4JKV47G", ArrayType(
            StructType(Seq(
              StructField("field_9HI2CLD96658", StringType),
              StructField("field_V6NMVFBPZTK0IQ", StringType)
            ))
          )),
          StructField("field_5HFZCI5KDM", ArrayType(
            StructType(Seq(
              StructField("field_1JJ8V0Y2K", LongType),
              StructField("field_W06XUE4JNETUKK6", StringType)
            ))
          )),
          StructField("field_CN2PQ748U1OBJ", ArrayType(
            StructType(Seq(
              StructField("field_HKTXKOYLO6RFQ9H", LongType),
              StructField("field_V35TZP", LongType)
            ))
          )),
          StructField("field_HSFB2", StringType),
          StructField("field_9VF1PE9PFDSYAP8", StructType(Seq(
            StructField("field_0H3Z85", StringType),
            StructField("field_0FQNKVOQ8I99CIN", StringType),
            StructField("field_QP4DDICXWEO1SG", StringType)
          ))),
          StructField("field_9C45YGQMZ4XQ1M", StructType(Seq(
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
            StructField("field_35IZ8UVC6", LongType)
          ))),
          StructField("field_P3SL4ZD", StructType(Seq(
            StructField("field_INQJ3B", StringType),
            StructField("field_8GEKHS", StringType),
            StructField("field_SOWOZ", StringType),
            StructField("field_4AYHTP78I", StringType),
            StructField("field_O47PA", StringType),
            StructField("field_BDP9W", StringType),
            StructField("field_BH0SCF", StringType),
            StructField("field_J3SWN4", StringType),
            StructField("field_IR4C6GU", StringType),
            StructField("field_A0RV4LQ58RPPA", StringType),
            StructField("field_XUCTLKL8RB", LongType)
          ))),
          StructField("field_OONK074H2", StringType)
        ))
      )),
      StructField("field_V5Z6FZQY3CP", StringType),
      StructField("field_JQRRX0W0GRF", StringType),
      StructField("field_M01P8XWSDBOCKJ", StringType),
      StructField("field_RDC7P0", StringType)
    ))
  }
}
