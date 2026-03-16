import logging
import numpy as np

from constants.StatisticServiceState import StatisticServiceState
from statistic_strategy.Strategy import Strategy


class DatabasePercentileStrategy (Strategy):
    logger = logging.getLogger(__name__)

    def __init__(self, db_provider):
        self._name = "Percentile Strategy using database stored function"
        self._db_provider = db_provider

    def test(self, parametric_data, constraint):
        udj_pct = (1 - constraint.centile_pct / 100) / 2
        # Convert numpy array to list for PostgreSQL function compatibility
        data_list = [float(x) for x in parametric_data] if hasattr(parametric_data, '__iter__') else parametric_data
        upper_bound_value = self._db_provider.calc_percentile_using_stored_function(
            data_list, udj_pct
        )
        lower_bound_value = self._db_provider.calc_percentile_using_stored_function(
            data_list, 1 - udj_pct
        )
        
        # Convert Decimal objects to float for JSON serialization
        if upper_bound_value is not None:
            upper_bound_value = float(upper_bound_value)
        if lower_bound_value is not None:
            lower_bound_value = float(lower_bound_value)
        self.logger.info(
            f"""test(): udj_pct: {udj_pct}, upper_bound: {upper_bound_value}, 
            lower_bound: {lower_bound_value}"""
        )
        bounded_data = parametric_data[
            (parametric_data > lower_bound_value)
            & (parametric_data < upper_bound_value)
        ]
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
                f"""test(): constraint:  {constraint.criteria_key}-{constraint.criteria_name} - 
                PASS"""
            )
            state = StatisticServiceState.PASS
        else:
            self.logger.info(
                f"""test(): constraint: {constraint.criteria_key}-{constraint.criteria_name} - 
                FAIL"""
            )

        return {
            "state": state,
            "upper_bound_test": bool(upper_bound_test),
            "lower_bound_test": bool(lower_bound_test),
            "upper_bound_value": upper_bound_value,
            "lower_bound_value": lower_bound_value,
            "difference": float(difference) if difference is not None else None,
        }

    @property
    def name(self):
        return self._name
