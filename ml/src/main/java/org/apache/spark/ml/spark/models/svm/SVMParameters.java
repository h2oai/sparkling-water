package org.apache.spark.ml.spark.models.svm;

import hex.Model;
import org.apache.spark.ml.spark.models.MissingValuesHandling;
import water.Key;
import water.fvec.Frame;

public class SVMParameters extends Model.Parameters {
    @Override
    public String algoName() { return "SVM"; }

    @Override
    public String fullName() { return "Support Vector Machine (*Spark*)"; }

    @Override
    public String javaName() { return SVMModel.class.getName(); }

    @Override
    public long progressUnits() { return _max_iterations; }

    public final Frame initialWeights() {
        if (null == _initial_weights) {
            return null;
        } else {
            return _initial_weights.get();
        }
    }

    public int _max_iterations = 1000;
    public boolean _add_intercept = false;
    public double _step_size = 1.0;
    public double _reg_param = 0.01;
    public double _convergence_tol = 0.001;
    public double _mini_batch_fraction = 1.0;
    public double _threshold = 0.0;
    public Updater _updater = Updater.L2;
    public Gradient _gradient = Gradient.Hinge;
    public Key<Frame> _initial_weights = null;
    public MissingValuesHandling _missing_values_handling = MissingValuesHandling.MeanImputation;

    public void validate(SVM svm) {
        if (_max_iterations < 0 || _max_iterations > 1e6) {
            svm.error("_max_iterations", " max_iterations must be between 0 and 1e6");
        }

        if(_step_size <= 0) {
            svm.error("_step_size", "The step size has to be positive.");
        }

        if(_reg_param <= 0) {
            svm.error("_reg_param", "The regularization parameter has to be positive.");
        }

        if(_convergence_tol <= 0) {
            svm.error("_convergence_tol", "The convergence tolerance has to be positive.");
        }

        if(_mini_batch_fraction <= 0) {
            svm.error("_mini_batch_fraction", "The minimum batch fraction has to be positive.");
        }

    }


}
