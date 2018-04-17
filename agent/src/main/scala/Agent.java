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
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.instrument.Instrumentation;

public class Agent {

  public static void premain(String agentArgument, Instrumentation instrumentation) {
    try {
      new AgentBuilder.Default()
              .type(ElementMatchers.nameContains("ErrorConsumer"))
              .transform((builder, typeDescription, classLoader, javaModule) -> builder.method(ElementMatchers.nameContains("unseenCategorical")).intercept(MethodDelegation.to(new Interceptor()))).installOn(instrumentation);
    } catch (RuntimeException e) {
      System.out.println("Exception instrumenting code : " + e);
      e.printStackTrace();
    }

  }

}

