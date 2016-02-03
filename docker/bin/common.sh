#!/usr/bin/env bash

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

# Check if docker is running
if ! docker info > /dev/null; then
  echo "Exiting..."
  exit -1
fi

REQUIRED_MEMORY="5"
DOCKER_MEMORY=$(docker info | grep "Total Memory" | sed -e "s/Total Memory: //")

memory_size=${DOCKER_MEMORY/ [a-zA-Z]*/}
memory_unit=${DOCKER_MEMORY/[0-9\.]* /}

is_memory_enough=$(bc -l <<< "$memory_size > $REQUIRED_MEMORY")

if [[ "$memory_unit" != "GiB" ]] || [[ "$is_memory_enough" != "1" ]]; then
    echo "WARN: Docker reports $DOCKER_MEMORY total memory, but the Sparkling Water Dockerfile needs ${REQUIRED_MEMORY}G!" 
fi

