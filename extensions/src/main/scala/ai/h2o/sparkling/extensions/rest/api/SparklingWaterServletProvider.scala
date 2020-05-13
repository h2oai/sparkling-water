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

package ai.h2o.sparkling.extensions.rest.api

import java.util
import scala.collection.JavaConverters._
import water.server.{ServletMeta, ServletProvider}

class SparklingWaterServletProvider extends ServletProvider {

  /**
    * Provides a collection of Servlets that should be registered.
    *
    * @return a map of context path to a Servlet class
    */
  override def servlets(): util.List[ServletMeta] = {
    Seq(
      new ServletMeta(Paths.CHUNK, classOf[ChunkServlet]),
      new ServletMeta(Paths.CHUNK_CATEGORICAL_DOMAINS, classOf[ChunkCategoricalDomainsServlet]),
      new ServletMeta(Paths.SPARKLING_INTERNAL, classOf[InternalUtilsServlet])).asJava
  }
}
