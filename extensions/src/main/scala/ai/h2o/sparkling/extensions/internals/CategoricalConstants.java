package ai.h2o.sparkling.extensions.internals;

import water.parser.Categorical;

public final class CategoricalConstants {
  public static final String TESTING_MAXIMUM_CATEGORICAL_LEVELS_PROPERTY_NAME =
      "testing.maximumCategoricalLevels";

  public static int getMaximumCategoricalLevels() {
    String testingThreshold = System.getProperty(TESTING_MAXIMUM_CATEGORICAL_LEVELS_PROPERTY_NAME);
    if (testingThreshold == null) {
      return Categorical.MAX_CATEGORICAL_COUNT;
    } else {
      return new Integer(testingThreshold).intValue();
    }
  }
}
