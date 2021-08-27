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

package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common.{EntitySubstitutionContext, ModelMetricsSubstitutionContext}

object MetricsInitTemplate extends ((Seq[ModelMetricsSubstitutionContext]) => String) with PythonEntityTemplate {

  def apply(metricSubstitutionContexts: Seq[ModelMetricsSubstitutionContext]): String = {
    val metricClasses = metricSubstitutionContexts.map(_.entityName)
    val imports = metricClasses.map(metricClass => s"ai.h2o.sparkling.ml.metrics.$metricClass.$metricClass")

    val entitySubstitutionContext = EntitySubstitutionContext(null, null, null, imports)

    s"""#
       |# Licensed to the Apache Software Foundation (ASF) under one or more
       |# contributor license agreements.  See the NOTICE file distributed with
       |# this work for additional information regarding copyright ownership.
       |# The ASF licenses this file to You under the Apache License, Version 2.0
       |# (the "License"); you may not use this file except in compliance with
       |# the License.  You may obtain a copy of the License at
       |#
       |#    http://www.apache.org/licenses/LICENSE-2.0
       |#
       |# Unless required by applicable law or agreed to in writing, software
       |# distributed under the License is distributed on an "AS IS" BASIS,
       |# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       |# See the License for the specific language governing permissions and
       |# limitations under the License.
       |#
       |
       |${generateImports(entitySubstitutionContext)}
       |""".stripMargin
  }
}
