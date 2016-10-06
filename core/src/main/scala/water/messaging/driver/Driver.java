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

package water.messaging.driver;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException {
        String appClass = "water.SparklingWater";
        List<String> params = new LinkedList<>();
        StringBuilder extraJava = new StringBuilder();
        for(String arg : args) {
            if(arg.contains("appClassName")) {
                appClass = arg.split("=")[1];
            } else if(!arg.startsWith("-D")){
                params.add(arg);
            } else {
                extraJava.append(arg);
                extraJava.append(" ");
            }
        }

        SparkLauncher launcher = new SparkLauncher()
                .setAppResource(Driver.class.getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .getPath())
                .setMainClass(appClass)
                .setMaster("yarn")
                // Important - we don't want it to run on the same node as Steam
                .setDeployMode("cluster")
                .setConf("spark.yarn.submit.waitAppCompletion", "false")
                // Necessary as we'll be packaging this in a driver jar
                .setConf("spark.ext.h2o.md5skip", "true")
                .setConf("spark.ext.h2o.client.ip", "localhost")
                .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, extraJava.toString());

        for(int i = 0; i < params.size(); i += 2) {
            if("-mem".equals(params.get(i))) {
                launcher.setConf("spark.executor.memory", params.get(i+1));
                launcher.setConf("spark.driver.memory", params.get(i+1));
            } else if("-size".equals(params.get(i))) {
                launcher.setConf("spark.executor.instances", params.get(i+1));
            } else if(params.get(i).startsWith("spark.")) {
                launcher.setConf(params.get(i), params.get(i+1));
            }
        }

        launcher.launch().waitFor();
    }

}
