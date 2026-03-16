from utils import Utils


class ConstraintsToTest:
    def __init__(self):
        self._should_process_work_order = True
        self._calculated_delta_e = False
        self._work_order_status = None
        self._constraint_keys = []
        self._constraints_errors = {}
        self._constraints_results = {}
        self._errors = []

    def add_constraint_key(self, key):
        if key not in self._constraint_keys:
            self._constraint_keys.append(key)

    def add_constraint_error(self, key, error):
        if key not in self._constraints_errors:
            self._constraints_errors[key] = []

        self._constraints_errors[key].append(Utils.multiple_to_single_space(error))

    def add_constraint_result(self, key, result):
        if key in self._constraints_results:
            self._constraints_results[key].update(result)
        else:
            self._constraints_results[key] = result

    def add_error(self, error):
        self._errors.append(Utils.multiple_to_single_space(error))

    def sort_constraint_keys(self):
        self._constraint_keys.sort()

    @property
    def work_order_status(self):
        return self._work_order_status

    @work_order_status.setter
    def work_order_status(self, value):
        self._work_order_status = value

    @property
    def should_process_work_order(self):
        return self._should_process_work_order

    @should_process_work_order.setter
    def should_process_work_order(self, value):
        self._should_process_work_order = value

    @property
    def constraint_keys(self):
        return self._constraint_keys

    @property
    def constraints_errors(self):
        return self._constraints_errors

    @property
    def constraints_results(self):
        return self._constraints_results

    @property
    def errors(self):
        return self._errors

    @property
    def calculated_delta_e(self):
        return self._calculated_delta_e

    @calculated_delta_e.setter
    def calculated_delta_e(self, value):
        self._calculated_delta_e = value
