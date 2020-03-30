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

package ai.h2o.sparkling.extensions.stacktrace

import java.util.Date

import water.util.Log
import water.{AbstractH2OExtension, H2O}

class StackTraceCollector extends AbstractH2OExtension {
  private var interval = -1 // -1 means disabled

  override def getExtensionName = "StackTraceCollector"

  override def printHelp(): Unit = {
    System.out.println(
      "\nStack trace collector extension:\n" +
        "    -stacktrace_collector_interval\n" +
        "          Time in seconds specifying how often to collect logs. \n")
  }

  private def parseInterval(args: Array[String]): Array[String] = {
    for (i <- args.indices) {
      val s = new H2O.OptString(args(i))
      if (s.matches("stacktrace_collector_interval")) {
        interval = s.parseInt(args(i + 1))
        val new_args = new Array[String](args.length - 2)
        System.arraycopy(args, 0, new_args, 0, i)
        System.arraycopy(args, i + 2, new_args, i, args.length - (i + 2))
        return new_args
      }
    }
    args
  }

  override def parseArguments(args: Array[String]): Array[String] = parseInterval(args)

  override def onLocalNodeStarted(): Unit = {
    if (interval > 0) {
      new StackTraceCollectorThread().start()
    }
  }

  private class StackTraceCollectorThread extends Thread("StackTraceCollectorThread") {
    setDaemon(true)

    import scala.collection.JavaConverters._

    override def run(): Unit = {
      while (true) {
        try {
          Log.info("Taking stacktrace at time: " + new Date)
          val allStackTraces = Thread.getAllStackTraces.asScala
          for ((thread, traces) <- allStackTraces) {
            Log.info("Taking stacktrace for thread: " + thread)
            for (trace <- traces) {
              Log.info("\t" + trace.toString)
            }
          }
          Thread.sleep(interval * 1000)
        } catch {
          case _: InterruptedException =>
        }
      }
    }
  }

}
