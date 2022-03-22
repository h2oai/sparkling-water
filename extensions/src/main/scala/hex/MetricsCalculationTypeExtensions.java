package hex;

import java.util.Arrays;
import water.TypeMapExtension;
import water.api.schemas3.*;

public class MetricsCalculationTypeExtensions implements TypeMapExtension {
  public static final String[] MODEL_BUILDER_CLASSES = {
    ModelMetrics.IndependentMetricBuilder.class.getName(),
    ModelMetricsSupervised.IndependentMetricBuilderSupervised.class.getName(),
    ModelMetricsBinomial.IndependentMetricBuilderBinomial.class.getName(),
    AUC2.AUCBuilder.class.getName(),
    ModelMetricsRegression.IndependentMetricBuilderRegression.class.getName(),
    Distribution.class.getName(),
    GaussianDistribution.class.getName(),
    BernoulliDistribution.class.getName(),
    QuasibinomialDistribution.class.getName(),
    ModifiedHuberDistribution.class.getName(),
    MultinomialDistribution.class.getName(),
    PoissonDistribution.class.getName(),
    GammaDistribution.class.getName(),
    TweedieDistribution.class.getName(),
    HuberDistribution.class.getName(),
    LaplaceDistribution.class.getName(),
    QuantileDistribution.class.getName(),
    CustomDistribution.class.getName(),
    CustomDistributionWrapper.class.getName(),
    LinkFunction.class.getName(),
    IdentityFunction.class.getName(),
    InverseFunction.class.getName(),
    LogFunction.class.getName(),
    LogitFunction.class.getName(),
    OlogitFunction.class.getName(),
    OloglogFunction.class.getName(),
    OprobitFunction.class.getName(),
    ModelMetricsMultinomial.IndependentMetricBuilderMultinomial.class.getName()
  };

  public static final String[] SCHEMA_CLASSES = {
    ModelMetricsBaseV3.class.getName(),
    ModelMetricsBinomialV3.class.getName(),
    ModelMetricsMultinomialV3.class.getName(),
    ModelMetricsRegressionV3.class.getName(),
    ConfusionMatrixV3.class.getName(),
    TwoDimTableV3.class.getName(),
    TwoDimTableV3.ColumnSpecsBase.class.getName()
  };

  @Override
  public String[] getBoostrapClasses() {
    String[] result =
        Arrays.copyOf(MODEL_BUILDER_CLASSES, MODEL_BUILDER_CLASSES.length + SCHEMA_CLASSES.length);
    System.arraycopy(
        SCHEMA_CLASSES, 0, result, MODEL_BUILDER_CLASSES.length, SCHEMA_CLASSES.length);
    return result;
  }
}
