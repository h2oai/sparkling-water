package ai.h2o.extensions.stacktrace;

import water.AbstractH2OExtension;
import water.H2O;
import water.util.Log;

import java.util.Date;
import java.util.Map;

public class StackTraceExtension extends AbstractH2OExtension {
  private int interval = -1; // -1 means disabled

  @Override
  public String getExtensionName() {
    return "StackTraceCollector";
  }

  @Override
  public void printHelp() {
    System.out.println(
            "\nStack trace collector extension:\n" +
                    "    -stacktrace_collector_interval\n" +
                    "          Time in seconds specifying how often to collect logs. \n"

    );
  }

  private String[] parseInterval(String args[]) {
    for (int i = 0; i < args.length; i++) {
      H2O.OptString s = new H2O.OptString(args[i]);
      if (s.matches("stacktrace_collector_interval")) {
        interval = s.parseInt(args[i + 1]);
        String[] new_args = new String[args.length - 2];
        System.arraycopy(args, 0, new_args, 0, i);
        System.arraycopy(args, i + 2, new_args, i, args.length - (i + 2));
        return new_args;
      }
    }
    return args;
  }

  @Override
  public String[] parseArguments(String[] args) {
    return parseInterval(args);
  }

  @Override
  public void onLocalNodeStarted() {
    if (interval > 0) {
      new StackTraceCollectorThread().start();
    }
  }

  private class StackTraceCollectorThread extends Thread {
    public StackTraceCollectorThread() {
      super("StackTraceCollectorThread");
    }

    @Override
    public void run() {
      while (true) {
        try {
          Log.info("Taking stacktrace at time: " + new Date());
          Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
          for (Map.Entry<Thread, StackTraceElement[]> e : allStackTraces.entrySet()) {
            Log.info("Taking stacktrace for thread: " + e.getKey());
            for (StackTraceElement st : e.getValue()) {
              Log.info("\t" + st.toString());
            }
          }
          sleep(interval * 1000);
        } catch (InterruptedException ignored) {
        }
      }
    }
  }
}

