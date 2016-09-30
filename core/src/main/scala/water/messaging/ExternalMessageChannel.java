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

package water.messaging;

import com.googlecode.jsonrpc4j.*;
import org.eclipse.jetty.server.Server;
import water.messaging.driver.DriverHandler;
import water.messaging.driver.DriverService;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;


public class ExternalMessageChannel implements Runnable {

    // Address of the remote host
    // TODO the h2o cluster manager needs an endpoint to change this
    private String addrs;
    private final DriverService driver;
    private final String topic;

    /**
     * @param addrs Remote host address
     * @param driver Implementation providing getter capabilities from the app layer
     * @throws Exception
     */
    public ExternalMessageChannel(
            String addrs,
            DriverService driver,
            String topic) throws Throwable {
        this.addrs = addrs;
        this.driver = driver;
        this.topic = topic;
    }

    public void send(final Map<String, Object> payload) throws Throwable {
        payload.put("Topic", topic);
        Map<String, Object>[] body = new HashMap[1];
        body[0] = payload;
        call("Receive", body);
    }

    public void call(String method,
                     Map<String, Object>[] payload) throws Throwable {
        JsonRpcHttpClient client = new JsonRpcHttpClient(new URL("http://" + addrs + "/h2ocluster"));
        client.invoke("Handler." + method, payload);
        client.setContentType("application/json-rpc");
    }

    @Override
    public void run() {
        Server server = new Server(0);
        server.setHandler(new DriverHandler(driver));

        try {
            server.start();

            driver.setPort(server.getConnectors()[0].getLocalPort());

            Map<String, Object> payload = new HashMap<>(2);

            Map<String, Object> value = new HashMap<>(3);
            value.putAll(driver.get(new String[] {"ip", "id", "messaging"}));

            payload.put("Value", value);

            send(payload);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

}
