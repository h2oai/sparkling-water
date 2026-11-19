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

package ai.h2o.sparkling.backend

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class NodeDescTestSuite extends FunSuite with Matchers {

  test("parseIpPort handles IPv4") {
    NodeDesc.parseIpPort("10.0.0.1:54321") shouldEqual (("10.0.0.1", 54321))
  }

  test("parseIpPort handles hostnames") {
    NodeDesc.parseIpPort("node-1.example.com:54321") shouldEqual (("node-1.example.com", 54321))
  }

  test("parseIpPort handles IPv6 without brackets") {
    NodeDesc.parseIpPort("2001:db8:0:1::42:54321") shouldEqual (("2001:db8:0:1::42", 54321))
  }

  test("parseIpPort handles IPv6 with brackets") {
    NodeDesc.parseIpPort("[2001:db8:0:1::42]:54321") shouldEqual (("2001:db8:0:1::42", 54321))
  }

  test("parseIpPort rejects input without a port") {
    intercept[IllegalArgumentException](NodeDesc.parseIpPort("10.0.0.1"))
  }

  test("ipPort does not change IPv4 addresses and hostnames") {
    NodeDesc("0", "10.0.0.1", 54321).ipPort() shouldEqual "10.0.0.1:54321"
    NodeDesc("0", "node-1.example.com", 54321).ipPort() shouldEqual "node-1.example.com:54321"
  }

  test("ipPort wraps IPv6 addresses in brackets") {
    NodeDesc("0", "2001:db8::42", 54321).ipPort() shouldEqual "[2001:db8::42]:54321"
  }

  test("ipPort of an IPv6 node can be used as a URL authority") {
    val url = new java.net.URI(s"http://${NodeDesc("0", "2001:db8::42", 54321).ipPort()}/3/Cloud").toURL
    url.getHost shouldEqual "[2001:db8::42]"
    url.getPort shouldEqual 54321
  }

  test("parseIpPort and ipPort round-trip") {
    Seq("10.0.0.1:54321", "[2001:db8::42]:54321").foreach { ipPort =>
      val (ip, port) = NodeDesc.parseIpPort(ipPort)
      NodeDesc("0", ip, port).ipPort() shouldEqual ipPort
    }
  }
}
