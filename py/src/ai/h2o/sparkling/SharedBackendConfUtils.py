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

class SharedBackendConfUtils(object):

    def _get_option(self, option):
        if option.isDefined():
            return option.get()
        else:
            return None

    def set(self, key, value):
        self._jconf.set(key, value)
        return self

    def remove(self, key):
        self._jconf.remove(key)
        return self

    def contains(self, key):
        return self._jconf.contains(key)

    def get(self, key, default_value=None):
        """
        Get a parameter, throws a NoSuchElementException if the value
        is not available and default_value not set
        """
        if default_value is None:
            return self._jconf.get(key)
        else:
            return self._jconf.get(key, default_value)

    def get_all(self):
        """
        Get all parameters as a list of pairs
        :return: list_of_configurations: List of pairs containing configurations
        """
        python_conf = []
        all = self._jconf.getAll()
        for conf in all:
            python_conf.append((conf._1(), conf._2()))
        return python_conf

    def set_all(self, list_of_configurations):
        """
        Set multiple parameters together
        :param list_of_configurations: List of pairs containing configurations
        :return: this H2O configuration
        """
        for conf in list_of_configurations:
            self._jconf.set(conf[0], conf[1])
        return self

    def __str__(self):
        return self._jconf.toString()

    def __repr__(self):
        self.show()
        return ""

    def show(self):
        print(self)
