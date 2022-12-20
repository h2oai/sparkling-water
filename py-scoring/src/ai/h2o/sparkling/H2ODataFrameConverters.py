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

from pyspark.sql import DataFrame, SparkSession
from py4j.java_gateway import JavaObject


class H2ODataFrameConverters(object):

    @staticmethod
    def scalaToPythonDataFrame(jdf):
        if jdf is None:
            return None
        elif isinstance(jdf, JavaObject):
            session = SparkSession.builder.getOrCreate()
            if hasattr(session, '_wrapped'):
                sqlContext = session._wrapped
            else:
                sqlContext = session  # Spark 3.3+ utilizes SparkSession instead of SQLContext
            return DataFrame(jdf, sqlContext)
        else:
            raise TypeError("Invalid type.")

    @staticmethod
    def scalaDfArrayToPythonDfArray(array):
        if array is None:
            return None
        elif isinstance(array, JavaObject):
            return [H2ODataFrameConverters.scalaToPythonDataFrame(v) for v in array]
        else:
            raise TypeError("Invalid type.")
