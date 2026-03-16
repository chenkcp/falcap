from datetime import datetime

from constants.StatisticServiceState import StatisticServiceState
from utils import Utils


class WorkOrder:
    def __init__(
        self,
        work_order_id,
        inv_item_dim_ky,
        ink_type_dim_ky,
        arch_id,
        test_type,
        pens_count,
        is_printed_at_falcap,
        is_dual_color,
        wo_close_dm=None,
        wo_slot_type_cd=None,
        prod_color_dim_kys=None,
        test_type_count=0,
        filtered_constraint_count=0,
    ):
        self._id = work_order_id
        self._inv_item_dim_ky = inv_item_dim_ky
        self._ink_type_dim_ky = ink_type_dim_ky
        self._arch_id = arch_id
        self._test_type = test_type
        self._pens_count = pens_count
        self._is_printed_at_falcap = is_printed_at_falcap
        self._is_dual_color = is_dual_color
        self._wo_close_dm = wo_close_dm
        self._wo_slot_type_cd = wo_slot_type_cd or []
        self._prod_color_dim_kys = prod_color_dim_kys or []
        self._test_type_count = test_type_count
        self._filtered_constraint_count = filtered_constraint_count
        self._status_result = {
            "wo_st": "",
            "wo_desc": "",
            "test_st": [],
            "result_fg": StatisticServiceState.NA,
            "result_summary": "",
            "email_dt": "",
        }
        self._constraints_to_test = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def inv_item_dim_ky(self):
        return self._inv_item_dim_ky

    @inv_item_dim_ky.setter
    def inv_item_dim_ky(self, value):
        self._inv_item_dim_ky = value

    @property
    def ink_type_dim_ky(self):
        return self._ink_type_dim_ky

    @ink_type_dim_ky.setter
    def ink_type_dim_ky(self, value):
        self._ink_type_dim_ky = value

    @property
    def arch_id(self):
        return self._arch_id

    @arch_id.setter
    def arch_id(self, value):
        self._arch_id = value

    @property
    def test_type(self):
        return self._test_type

    @test_type.setter
    def test_type(self, value):
        self._test_type = value

    @property
    def constraints_to_test(self):
        return self._constraints_to_test

    @constraints_to_test.setter
    def constraints_to_test(self, value):
        self._constraints_to_test = value

    @property
    def pens_count(self):
        return self._pens_count

    @pens_count.setter
    def pens_count(self, value):
        self._pens_count = value

    @property
    def is_printed_at_falcap(self):
        return self._is_printed_at_falcap

    @is_printed_at_falcap.setter
    def is_printed_at_falcap(self, value):
        self._is_printed_at_falcap = value

    @property
    def is_dual_color(self):
        return self._is_dual_color

    @is_dual_color.setter
    def is_dual_color(self, value):
        self._is_dual_color = value
        
    @property
    def wo_close_dm(self):
        return self._wo_close_dm

    @wo_close_dm.setter
    def wo_close_dm(self, value):
        self._wo_close_dm = value

    @property
    def wo_slot_type_cd(self):
        return self._wo_slot_type_cd

    @wo_slot_type_cd.setter
    def wo_slot_type_cd(self, value):
        self._wo_slot_type_cd = value

    @property
    def prod_color_dim_kys(self):
        return self._prod_color_dim_kys

    @prod_color_dim_kys.setter
    def prod_color_dim_kys(self, value):
        self._prod_color_dim_kys = value

    @property
    def test_type_count(self):
        return self._test_type_count

    @test_type_count.setter
    def test_type_count(self, value):
        self._test_type_count = value

    @property
    def filtered_constraint_count(self):
        return self._filtered_constraint_count

    @filtered_constraint_count.setter
    def filtered_constraint_count(self, value):
        self._filtered_constraint_count = value

    @property
    def status_result(self):
        return self._status_result

    def get_constraints_color_with_same_criteria(self, criteria_key):
        colors = []
        for constraint in self._test_type.constraints.values():
            if (
                constraint.criteria_key == criteria_key
                and constraint.prod_color_dim_ky is not None
            ):
                colors.append(constraint.prod_color_dim_ky)

        return colors

    #status of the work order, to  ACCEPT for constraint testing, or SKIP for no testtype or param data or BLOCK if time exceeded wait time setting
    def log_workorder_status(self, work_order_status, reason=None):
        self._status_result["wo_st"] = work_order_status
        if reason:
            self._status_result["wo_desc"] = Utils.multiple_to_single_space(reason)

    #result of the test status from each constraint test in a workorder
    def log_test_status(self, status):
        self._status_result["test_st"].append(Utils.multiple_to_single_space(status))

    def log_email_sent(self):
        self._status_result["email_dt"] = datetime.utcnow().isoformat()

    #result for each work order, pass/fail/skip, this is the summary of all constraints test in the workorder, it is different from the test status which is for each constraint
    def log_workorder_result(self, work_order_result, summary=None):
        self._status_result["result_fg"] = work_order_result
        if summary:
            self._status_result["result_summary"] = Utils.multiple_to_single_space(summary)
