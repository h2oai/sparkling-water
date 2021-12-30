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

import h2o

def testImportFromGCS(hc):
    some_public_gcp_file = "gs://gcp-public-data-landsat" \
           "/LC08/01/001/009/LC08_L1GT_001009_20210612_20210622_01_T2/LC08_L1GT_001009_20210612_20210622_01_T2_MTL.txt"
    frame = h2o.import_file(path=some_public_gcp_file)
    df = hc.asSparkFrame(frame)
    assert df.count() == 218
