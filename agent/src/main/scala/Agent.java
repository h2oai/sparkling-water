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

