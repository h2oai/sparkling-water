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

import os
from random import randrange
import subprocess

def listYarnApps():
    return str(subprocess.check_output("yarn application -list", shell=True))


def unique_cloud_name(script_name):
    return str(script_name[:-3].replace("/", "_")) + str(randrange(65536))


def locate(file_name):
    if os.path.isfile("/home/0xdiag/" + file_name):
        return os.path.abspath("/home/0xdiag/" + file_name)
    else:
        return os.path.abspath("../examples/" + file_name)
