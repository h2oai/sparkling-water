# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import h2o
from h2o.estimators import H2OGradientBoostingEstimator


class WeightedFalseNegativeLossMetric:
    def map(self, predicted, actual, weight, offset, model):
        cost_tp = 5000  # set prior to use
        cost_tn = 0  # do not change
        cost_fp = cost_tp  # do not change
        cost_fn = weight  # do not change
        y = actual[0]
        p = predicted[2]  # [class, p0, p1]
        if y == 1:
            denom = cost_fn
        else:
            denom = cost_fp
        return [(y * (1 - p) * cost_fn) + (p * cost_fp), denom]

    def reduce(self, left, right):
        return [left[0] + right[0], left[1] + right[1]]

    def metric(self, last):
        return last[0] / last[1]


def testCustomMetric(loanDatasetPath):
    train = h2o.import_file(loanDatasetPath, destination_frame="loan_train")
    train["bad_loan"] = train["bad_loan"].asfactor()

    y = "bad_loan"
    x = train.col_names
    x.remove(y)
    x.remove("int_rate")

    train["weight"] = train["loan_amnt"]

    weightedFalseNegativeLossFunc = h2o.upload_custom_metric(WeightedFalseNegativeLossMetric,
                                                             func_name="WeightedFalseNegativeLoss",
                                                             func_file="weighted_false_negative_loss.py")
    gbm = H2OGradientBoostingEstimator(model_id="gbm.hex", custom_metric_func=weightedFalseNegativeLossFunc)
    gbm.train(y=y, x=x, training_frame=train, weights_column="weight")

    perf = gbm.model_performance()
    assert perf.custom_metric_name() == "WeightedFalseNegativeLoss"
    assert perf.custom_metric_value() == 0.24579011595430142
