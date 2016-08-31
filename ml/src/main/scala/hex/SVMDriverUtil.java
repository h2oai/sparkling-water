package hex;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.spark.models.svm.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import water.fvec.Frame;

public class SVMDriverUtil {

    public static ModelMetrics computeMetrics(SVMModel model, RDD<LabeledPoint> training,
                                              final org.apache.spark.mllib.classification.SVMModel trainedModel,
                                              Frame f,
                                              String[] responseDomains) {

        // Compute Spark evaluations
        JavaRDD<Tuple2<Double, Double>> predictionAndLabels = training.toJavaRDD().map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        Double prediction = trainedModel.predict(p.features());
                        return new Tuple2<>(prediction, p.label());
                    }
                }
        );

        double mse;

        // Set the metrics
        switch (model._output.getModelCategory()) {
            case Binomial:
                ModelMetricsBinomial.MetricBuilderBinomial builderB = new ModelMetricsBinomial.MetricBuilderBinomial(responseDomains);
                for (Tuple2<Double, Double> predAct : predictionAndLabels.collect()) {
                    Double pred = predAct._1;
                    builderB.perRow(
                            new double[] {pred, pred == 1 ? 0 : 1, pred == 1 ? 1 : 0},
                            new float[] {predAct._2.floatValue()},
                            model
                    );
                }
                mse = builderB._sumsqe / builderB._nclasses;

                return new ModelMetricsBinomial(
                        model, f, builderB._count, mse, responseDomains,
                        builderB.weightedSigma(), new AUC2(builderB._auc), builderB._logloss, null
                );
            default:
                ModelMetricsRegression.MetricBuilderRegression builderR = new ModelMetricsRegression.MetricBuilderRegression();
                for (Tuple2<Double, Double> predAct : predictionAndLabels.collect()) {
                    Double pred = predAct._1;
                    builderR.perRow(
                            new double[] {pred, pred == 1 ? 0 : 1, pred == 1 ? 1 : 0},
                            new float[] {predAct._2.floatValue()},
                            model
                    );
                }
                mse = builderR._sumsqe / builderR._nclasses;

                return new ModelMetricsRegression(
                        model, f, builderR._count, mse, builderR.weightedSigma(), builderR._sumdeviance / builderR._wcount, 0
                );
        }
    }

}
