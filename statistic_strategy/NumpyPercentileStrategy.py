import logging

import numpy as np

from constants.StatisticServiceState import StatisticServiceState
from statistic_strategy.Strategy import Strategy


class NumpyPercentileStrategy(Strategy):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self._name = "Percentile Strategy using in-memory NumPy package"

    def test(self, parametric_data, constraint):
        # Convert Decimal values to float for NumPy operations, filtering out None values
        filtered_data = [float(x) for x in parametric_data if x is not None]
        
        if len(filtered_data) == 0:
            self.logger.warning("No valid data points available for percentile calculation")
            return {
                "result": StatisticServiceState.SKIP,
                "upper_bound_value": None,
                "lower_bound_value": None,
                "difference": None,
                "upper_bound_test": False,
                "lower_bound_test": False,
            }
            
        np_data = np.array(filtered_data)
        # Convert Decimal to float for percentile parameters
        upper_percentile = float(constraint.centile_pct)
        lower_percentile = float(100 - constraint.centile_pct)
       
        # Use interpolation for older NumPy versions (<1.22)
        upper_bound_value = np.percentile(np_data, upper_percentile, method="higher")
        lower_bound_value = np.percentile(np_data, lower_percentile, method="lower")
       
        self.logger.info(
            f"test(): upper_bound: {upper_bound_value}, lower_bound: {lower_bound_value}"
        )

        difference = 0
        if upper_bound_value > lower_bound_value:
            # Select values strictly between lower and upper bounds
            bounded_data = np_data[
                (np_data > lower_bound_value) & (np_data < upper_bound_value)
            ]
            if bounded_data.size > 0:
                difference = np.ptp(bounded_data)

        upper_bound_test = (
            constraint.upper_bound > upper_bound_value > constraint.lower_bound
        )
        lower_bound_test = (
            constraint.upper_bound > lower_bound_value > constraint.lower_bound
        )

        state = StatisticServiceState.FAIL
        if upper_bound_test and lower_bound_test:
            self.logger.info(
                f"test(): constraint:  {constraint.criteria_key}-{constraint.criteria_name} - "
                f"result: PASS"
            )
            state = StatisticServiceState.PASS
        else:
            self.logger.info(
                f"test(): constraint: {constraint.criteria_key}-{constraint.criteria_name} - "
                f"result: FAIL"
            )

        return {
            "state": state,
            "upper_bound_test": bool(upper_bound_test),
            "lower_bound_test": bool(lower_bound_test),
            "upper_bound_value": float(upper_bound_value) if upper_bound_value is not None else None,
            "lower_bound_value": float(lower_bound_value) if lower_bound_value is not None else None,
            "difference": float(difference) if difference is not None else None,
        }

    @property
    def name(self):
        return self._name
