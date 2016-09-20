package org.apache.spark.ml.spark.models.gm;

import org.apache.spark.mllib.ClusteringUtils;
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian;
import scala.Tuple2;
import water.codegen.CodeGeneratorPipeline;
import water.util.SBPrintStream;

/**
 * Class written in Java as it's a template for the POJO code we will generate for Gaussian
 * Mixture models.
  */
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

    /**
     * Predict to which cluster a given data point should be assigned to.
     * This computes the loglikelihood (multiplied by a given weight) at a given data point for each
     * of the passed multivariate Gaussian distributions and chooses the index of the highest one.
     *
     * @param weights Weight for each of the Gaussian distributions
     * @param gaussians Gaussians to be checked, each gaussian describes one cluster
     * @param epsilon
     * @param data Data to be assigned to a cluster
     * @return ID of the cluster in range [0, k-1]
     */
    public static int score(double[] weights,
                            MultivariateGaussian[] gaussians,
                            double epsilon,
                            double[] data) {
        int idx = 0;
        double max = Double.MIN_VALUE;
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
                max = p[i];
            }
        }
        return idx;
    }

    /**
     * Probability density function value for a given multivariate Guassian at a given point
     *
     * @param gaussian Multivariate Gaussian distribution
     * @param data     Data point for which the pdf should be calculated
     * @return The loglikelihood for given Gaussian and point
     */
    static double pdf(MultivariateGaussian gaussian, double[] data) {
        Tuple2<Double[][], Double> dm =
                ClusteringUtils.calculateCovarianceConstants(gaussian.sigma(), gaussian.mu());
        double[] delta = delta(data, gaussian.mu().toArray());
        double[] v = multiply(dm._1, delta);
        return Math.exp((dm._2 + multiply(v, v) * -0.5));
    }

    /**
     * Multiply a matrix with a (non transposed) vector
     * <p>
     * Example
     *           [1]
     *           [2]
     *           [3]
     * [1 2 3]  [14]
     * [4 5 6]  [32]
     * [7 8 9]  [50]
     *
     * @param matrix
     * @param vec
     * @return
     */
    static double[] multiply(Double[][] matrix, double[] vec) {
        assert matrix.length != 0;
        int resSize = matrix.length;
        double[] res = new double[resSize];
        for (int i = 0; i < resSize; i++) {
            res[i] = 0;
            for (int j = 0; j < matrix[i].length; j++) {
                res[i] += (matrix[i][j] * vec[j]);
            }
        }
        return res;
    }

    /**
     * Multiply two vectors element by element and sum to produce a scalar
     *
     * @param vec1 First vector
     * @param vec2 Second vector
     * @return Scala value
     */
    static double multiply(double[] vec1, double[] vec2) {
        double res = 0;
        for (int i = 0; i < vec1.length; i++) {
            res += (vec1[i] * vec2[i]);
        }
        return res;
    }

    /**
     * Calculates the difference (delta) between each element in the array and its respective
     * mean value
     *
     * @param data Input data
     * @param mu   Means
     * @return Array of deltas
     */
    static double[] delta(double[] data, double[] mu) {
        assert data.length == mu.length;
        double[] delta = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            delta[i] = data[i] - mu[i];
        }
        return delta;
    }

}
