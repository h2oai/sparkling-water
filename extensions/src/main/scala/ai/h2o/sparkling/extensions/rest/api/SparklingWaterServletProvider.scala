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
      new ServletMeta.Builder(Paths.CHUNK, classOf[ChunkServlet]).withAlwaysEnabled(true).build(),
      new ServletMeta.Builder(Paths.CHUNK_CATEGORICAL_DOMAINS, classOf[ChunkCategoricalDomainsServlet])
        .withAlwaysEnabled(true)
        .build(),
      new ServletMeta.Builder(Paths.SPARKLING_INTERNAL, classOf[InternalUtilsServlet])
        .withAlwaysEnabled(true)
        .build()).asJava
  }
}
