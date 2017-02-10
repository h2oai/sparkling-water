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

package org.apache.spark.h2o

import java.util.ServiceLoader

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import org.apache.spark.internal.Logging

/**
  * A top-level announcement service.
  */
trait AnnouncementService {

  /**
    * Distributed announcement to registered announcement providers.
    * @param anno  announcement
    */
  def announce(anno: Announcement): Unit

  /**
    * Shutdown the service
    */
  def shutdown: Unit

}

/** Factory to create announcement service. */
object AnnouncementServiceFactory {

  /** SPI based announcement service implementation.
    * It use SPI providers to find all announcement providers and
    * distribute messages to all of them in the order the providers were returned.
    */
  class AnnouncementServiceImpl(val h2oConf: H2OConf,
                                val serviceProviders: Seq[AnnouncementProvider]) extends AnnouncementService {
    
    override def announce(anno: Announcement): Unit = {
      serviceProviders.foreach { provider =>
        if (provider.isEnabled) provider.announce(anno)
       }
    }

    /**
      * Shutdown the service
      */
    override def shutdown: Unit = {
      serviceProviders.foreach(_.shutdown)
    }
  }

  def create(conf: H2OConf): AnnouncementService = {
    import collection.JavaConversions._
    val spiLoader = ServiceLoader.load(classOf[AnnouncementProvider])
    val annoProviders = spiLoader.iterator().toSeq
    // Configure them first
    annoProviders.foreach(_.configure(conf))
    // And then create announcement service
    new AnnouncementServiceImpl(conf, annoProviders)
  }
}

/** Target announcement service implemented for different technologies - REST/Redis */
trait AnnouncementProvider extends AnnouncementService with Logging {
  val CONF_PREFIX = "spark.ext.h2o.announce"

  def name: String
  
  def configure(conf: H2OConf): Unit

  def isEnabled: Boolean

  def async(body: => Int): Unit = {
    // Naive async execution
    new Thread(s"Thread[${name}]") {
      override def run(): Unit = {
        try {
          val retStatus: Int = body
          logDebug(s"The '${name}' returned ${retStatus}!")
        } catch {
          case t: Throwable => logWarning(s"The '${name}' failed to post announcement!", t)
        }
      }
    }.start()
  }
}

/* An announcement */
trait Announcement {
  def clusterId: String
}

/* Announce Flow location */
case class FlowLocationAnnouncement(clusterId: String, proto: String, ip: String, port: Int) extends Announcement

class RestAnnouncementProvider extends AnnouncementProvider {
  val CONF_URL = s"${CONF_PREFIX}.rest.url"

  var url: Option[String] = None

  val httpClient = new DefaultHttpClient()

  def name = "REST Announcement Provider"

  override def announce(anno: Announcement): Unit = {
    anno match {
      case FlowLocationAnnouncement(clusterId, flowProto, flowIp, flowPort) => {
        val entities = Seq(new BasicNameValuePair("proto", flowProto),
                           new BasicNameValuePair("ip", flowIp),
                           new BasicNameValuePair("port", flowPort.toString))
        async { post(clusterId, entities) }
      }
      case _ =>
    }
  }

  override def configure(conf: H2OConf): Unit = {
    url = conf.getOption(CONF_URL)
  }

  override def isEnabled: Boolean = url.isDefined

  private def post(clusterId: String, entities: Seq[NameValuePair]): Int = {
    import collection.JavaConverters._
    println("posting")
    val postMethod = new HttpPost(s"${url.get}/${clusterId}")
    postMethod.setEntity(new UrlEncodedFormEntity(entities.asJava))
    val response = httpClient.execute(postMethod)
    try {
      response.getStatusLine.getStatusCode
    } finally {
      response.close()
    }
  }

  override def shutdown(): Unit = {
    httpClient.close()
  }
}


