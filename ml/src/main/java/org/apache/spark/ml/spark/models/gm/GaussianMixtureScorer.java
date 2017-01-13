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
                                double[] weights,
                                MultivariateGaussian[] gaussians,
                                double epsilon,
                                boolean meanImputation) {
        // TODO implement
    }

    /**
     * Predict to which cluster a given data point should be assigned to.
     * This computes the loglikelihood (multiplied by a given weight) at a given data point for each
     * of the passed multivariate Gaussian distributions and chooses the index of the highest one.
     *
     * @param weights        Weight for each of the Gaussian distributions
     * @param data           Data to be assigned to a cluster
     * @return ID of the cluster in range [0, k-1]
     */
    public static int score(double[] weights,
                            MultivariateGaussian[] gaussians,
                            double epsilon,
                            double[] data,
                            boolean meanImputation,
                            double[] means) {
        double[] filledData = new double[data.length];

        for (int i = 0; i < filledData.length; i++) {
            double value = data[i];
            if (meanImputation && Double.isNaN(value)) {
                filledData[i] = means[i];
            } else {
                filledData[i] = value;
            }
        }

        int idx = 0;
        double max = Double.MIN_VALUE;
        double[] p = new double[weights.length];
        for (int i = 0; i < p.length; i++) {
            // PDF
            Tuple2<Double[][], Double> dm = ClusteringUtils.calculateCovarianceConstants(gaussians[i].mu(), gaussians[i].sigma());

            double[] delta = new double[filledData.length];
            for (int j = 0; j < filledData.length; j++) {
                delta[j] = filledData[j] - gaussians[i].mu().apply(j);
            }

            int resSize = dm._1.length;
            double[] v = new double[resSize];
            for (int j = 0; j < resSize; j++) {
                v[j] = 0;
                for (int k = 0; k < dm._1[j].length; k++) {
                    v[i] += (dm._1[j][k] * delta[k]);
                }
            }

            double vv = 0;
            for (int j = 0; j < v.length; j++) {
                vv += (v[j] * v[j]);
            }

            double pdf = Math.exp((dm._2 + vv * -0.5));
            p[i] = epsilon + weights[i] * pdf;
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

}
