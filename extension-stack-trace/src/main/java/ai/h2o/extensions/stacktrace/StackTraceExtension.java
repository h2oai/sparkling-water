package ai.h2o.extensions.stacktrace;

import water.AbstractH2OExtension;
import water.H2O;
import water.util.Log;

import java.util.Date;
import java.util.Map;
import java.util.Set;

public class StackTraceExtension extends AbstractH2OExtension {
  private int interval = 10; // 10 seconds
  private boolean enabled = false;

  @Override
  public String getExtensionName() {
    return "StackTraceCollector";
  }

  @Override
  public void printHelp() {
    System.out.println(
            "\nFailed node watchdog extension:\n" +
                    "    -stacktrace_collector_interval\n" +
                    "          Time in seconds specifying how often to collect logs. \n" +
                    "    -stacktrace_collector_enabled\n" +
                    "          True if the collector is enabled, false otherwise \n"

    );
  }

  private String[] parseEnabled(String[] args) {
    for (int i = 0; i < args.length; i++) {
      H2O.OptString s = new H2O.OptString(args[i]);
      if (s.matches("stacktrace_collector_enabled")) {
        enabled = true;
        String[] new_args = new String[args.length - 1];
        System.arraycopy(args, 0, new_args, 0, i);
        System.arraycopy(args, i + 1, new_args, i, args.length - (i + 1));
        return new_args;
      }
    }
    return args;
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
    return parseEnabled(parseInterval(args));
  }


  public void validateArguments() {
    if (interval < 0) {
      H2O.parseFailed("Stack trace collector interval has to be positive, got : " + interval);
    }
  }

  @Override
  public void onLocalNodeStarted() {
    if (enabled) {
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
          Log.debug("Taking stacktrace at time: " + new Date());
          Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
          for (Map.Entry<Thread, StackTraceElement[]> e : allStackTraces.entrySet()) {
            Log.debug("Taking stacktrace for thread: " + e.getKey());
            for (StackTraceElement st : e.getValue()) {
              Log.debug("\t" + st.toString());
            }
          }
          sleep(interval * 1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}

