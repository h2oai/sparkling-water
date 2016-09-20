package org.apache.spark.ml.spark.models.gm;

import org.apache.spark.mllib.ClusteringUtils;
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian;
import scala.Tuple2;
import water.codegen.CodeGeneratorPipeline;
import water.util.SBPrintStream;

public class GaussianMixtureScorer {


    public static void generate(SBPrintStream bodySb,
                                CodeGeneratorPipeline classCtx,
                                CodeGeneratorPipeline fileCtx,
                                boolean verboseCode,
                                double[] weights,
                                MultivariateGaussian[] gaussians,
                                double[] data) {
        // TODO implement
    }

    public static int score(double[] weights,
                            MultivariateGaussian[] gaussians,
                            double[] data) {
        int idx = 0;
        int max = Integer.MIN_VALUE;
        double epsilon = ClusteringUtils.EPSILON();
        double[] p = new double[weights.length];
        for (int i = 0; i < p.length; i++) {
            p[i] = epsilon + weights[i] * pdf(gaussians[i], data);
        }

        double pSum = 0;
        for (double aP : p) {
            pSum += aP;
        }

        for (int i = 0; i < p.length; i++) {
            p[i] /= pSum;
            if (p[i] >= max) {
                idx = i;
            }
        }
        return idx;
    }

    private static double pdf(MultivariateGaussian gaussian, double[] data) {
        Tuple2<Double[][], Double> dm = ClusteringUtils.calculateCovarianceConstants(gaussian.sigma(), gaussian.mu());
        double[] delta = delta(data, gaussian.mu().toArray());
        double[] v = multiply(dm._1, delta);
        return Math.exp((dm._2 + multiply(v, v) * -0.5));
    }

    private static double[] multiply(Double[][] matrix, double[] vec) {
        assert matrix.length != 0;
        int resSize = matrix[0].length;
        double[] res = new double[resSize];
        for (int i = 0; i < resSize; i++) {
            res[i] = 0;
            for (int j = 0; j < matrix.length; j++) {
                res[i] += (matrix[j][i] * vec[j]);
            }
        }
        return res;
    }

    private static double multiply(double[] vec1, double[] vec2) {
        double res = 0;
        for (int i = 0; i < vec1.length; i++) {
            res += (vec1[i] * vec2[i]);
        }
        return res;
    }

    private static double[] delta(double[] data, double[] mu) {
        assert data.length == mu.length;
        double[] delta = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            delta[i] = data[i] - mu[i];
        }
        return delta;
    }

}
