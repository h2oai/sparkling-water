package ai.h2o.sparkling.extensions.internals;

import java.util.Arrays;
import jsr166y.ForkJoinTask;
import water.H2O;
import water.Key;
import water.MRTask;
import water.parser.BufferedString;
import water.parser.PackedDomains;
import water.util.Log;

public class CollectCategoricalDomainsTask extends MRTask<CollectCategoricalDomainsTask> {
  private final Key frameKey;
  private byte[][] packedDomains;
  private int maximumCategoricalLevels;

  public CollectCategoricalDomainsTask(Key frameKey) {
    this.frameKey = frameKey;
    this.maximumCategoricalLevels = CategoricalConstants.getMaximumCategoricalLevels();
  }

  @Override
  public void setupLocal() {
    if (!LocalNodeDomains.containsDomains(frameKey)) return;
    final String[][][] localDomains = LocalNodeDomains.getDomains(frameKey);
    if (localDomains.length == 0) return;
    packedDomains = chunkDomainsToPackedDomains(localDomains[0]);
    for (int i = 1; i < localDomains.length; i++) {
      byte[][] anotherPackedDomains = chunkDomainsToPackedDomains(localDomains[i]);
      mergePackedDomains(packedDomains, anotherPackedDomains);
    }
    Log.trace("Done locally collecting domains on each node.");
  }

  private byte[][] chunkDomainsToPackedDomains(String[][] domains) {
    byte[][] result = new byte[domains.length][];
    for (int i = 0; i < domains.length; i++) {
      String[] columnDomain = domains[i];
      if (columnDomain.length > this.maximumCategoricalLevels) {
        result[i] = null;
      } else {
        BufferedString[] values = BufferedString.toBufferedString(columnDomain);
        Arrays.sort(values);
        result[i] = PackedDomains.pack(values);
      }
    }
    return result;
  }

  private void mergePackedDomains(byte[][] target, byte[][] source) {
    for (int i = 0; i < target.length; i++) {
      if (target[i] == null || source[i] == null) {
        target[i] = null;
      } else if (target[i].length + source[i].length > this.maximumCategoricalLevels) {
        target[i] = null;
      } else {
        target[i] = PackedDomains.merge(target[i], source[i]);
      }
    }
  }

  @Override
  public void reduce(final CollectCategoricalDomainsTask other) {
    if (packedDomains == null) {
      packedDomains = other.packedDomains;
    } else if (other.packedDomains != null) { // merge two packed domains
      H2O.H2OCountedCompleter[] tasks = new H2O.H2OCountedCompleter[packedDomains.length];
      for (int i = 0; i < packedDomains.length; i++) {
        final int fi = i;
        tasks[i] =
            new H2O.H2OCountedCompleter(currThrPriority()) {
              @Override
              public void compute2() {
                if (packedDomains[fi] == null || other.packedDomains[fi] == null) {
                  packedDomains[fi] = null;
                } else if (PackedDomains.sizeOf(packedDomains[fi])
                        + PackedDomains.sizeOf(other.packedDomains[fi])
                    > maximumCategoricalLevels) {
                  packedDomains[fi] = null;
                } else {
                  packedDomains[fi] =
                      PackedDomains.merge(packedDomains[fi], other.packedDomains[fi]);
                }
                tryComplete();
              }
            };
      }
      ForkJoinTask.invokeAll(tasks);
    }
    Log.trace("Done merging domains.");
  }

  public String[][] getDomains() {
    if (packedDomains == null) return null;
    String[][] result = new String[packedDomains.length][];
    for (int i = 0; i < packedDomains.length; i++) {
      if (packedDomains[i] == null) {
        result[i] = null;
      } else {
        result[i] = PackedDomains.unpackToStrings(packedDomains[i]);
      }
    }
    return result;
  }
}
