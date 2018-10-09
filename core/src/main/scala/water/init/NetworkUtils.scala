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
package water.init

import java.net.{InetAddress, NetworkInterface}

object NetworkUtils {

  def indentifyClientIp(remoteAddress: String): Option[String] = {
    val interfaces = NetworkInterface.getNetworkInterfaces
    while (interfaces.hasMoreElements) {
      val interface = interfaces.nextElement()
      import scala.collection.JavaConverters._
      interface.getInterfaceAddresses.asScala.foreach { address =>
        val ip = address.getAddress.getHostAddress + "/" + address.getNetworkPrefixLength
        val cidr = HostnameGuesser.CIDRBlock.parse(ip)
        if (cidr != null && cidr.isInetAddressOnNetwork(InetAddress.getByName(remoteAddress))) {
          return Some(address.getAddress.getHostAddress)
      }
      }
    }
    None
  }

}
