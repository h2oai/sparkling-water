#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from h2o.frame import H2OFrame


class FrameConversions:

    @staticmethod
    def _as_h2o_frame_from_RDD_String(h2oContext, rdd, frame_name, full_cols=100):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDString(rdd._to_java_object_rdd(), frame_name)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key, full_cols)

    @staticmethod
    def _as_h2o_frame_from_RDD_Bool(h2oContext, rdd, frame_name, full_cols=100):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDBool(rdd._to_java_object_rdd(), frame_name)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key, full_cols)

    @staticmethod
    def _as_h2o_frame_from_RDD_Int(h2oContext, rdd, frame_name, full_cols=100):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDInt(rdd._to_java_object_rdd(), frame_name)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key, full_cols)

    @staticmethod
    def _as_h2o_frame_from_RDD_Double(h2oContext, rdd, frame_name, full_cols=100):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromPythonRDDDouble(rdd._to_java_object_rdd(), frame_name)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key, full_cols)

    @staticmethod
    def _as_h2o_frame_from_RDD_Float(h2oContext, rdd, frame_name, full_cols=100):
        return FrameConversions._as_h2o_frame_from_RDD_Double(h2oContext, rdd, frame_name, full_cols)

    @staticmethod
    def _as_h2o_frame_from_RDD_Long(h2oContext, rdd, frame_name, full_cols=100):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromPythonRDDLong(rdd._to_java_object_rdd(), frame_name)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key, full_cols)

    @staticmethod
    def _as_h2o_frame_from_dataframe(h2oContext, dataframe, frame_name, full_cols=100):
        j_h2o_frame = h2oContext._jhc.asH2OFrame(dataframe._jdf, frame_name)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key, full_cols)

    @staticmethod
    def _as_h2o_frame_from_complex_type(h2oContext, dataframe, frame_name, full_cols=100):
        # Creates a DataFrame from an RDD of tuple/list, list or pandas.DataFrame.
        # On scala backend, to transform RDD of Product to H2OFrame, we need to know Type Tag.
        # Since there is no alternative for Product class in Python, we first transform the rdd to dataframe
        # and then transform it to H2OFrame.
        df = h2oContext._spark_session.createDataFrame(dataframe)
        j_h2o_frame = h2oContext._jhc.asH2OFrame(df._jdf, frame_name)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key, full_cols)
