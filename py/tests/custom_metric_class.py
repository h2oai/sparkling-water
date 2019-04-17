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
