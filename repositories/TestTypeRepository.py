import copy
import logging

from models.Constraint import Constraint
from models.TestType import TestType


class TestTypeRepository:
    logger = logging.getLogger(__name__)

    def __init__(
        self,
        db_provider,
    ):
        self._test_types = {}
        self._db_provider = db_provider

    def init_test_types(self):
        self.logger.info("init_test_types()")

        # loop through all test types and create a TestType object
        for (
            wo_type_dim_ky,
            work_order_type_name,
            ink_type_dim_ky,
            arch_id,
            min_pen_ct,
            days_to_process_wo_ct,
            work_order_active,
        ) in self._db_provider.get_all_test_types():
            constraints = {}
            for (
                constraint_key,
                test_criteria_ky,
                prod_color_dim_ky,
                upper_bound,
                lower_bound,
                centile_pct,
                slot_type_cd,
                constraint_active,
            ) in self._db_provider.get_constraints(wo_type_dim_ky):
                (
                    table_name,
                    column_name,
                    test_criteria_name,
                ) = self._db_provider.get_test_criteria(constraint_key)
                constraints[constraint_key] = Constraint(
                    constraint_key=constraint_key,
                    prod_color_dim_ky=prod_color_dim_ky,
                    criteria_key=test_criteria_ky,
                    criteria_name=test_criteria_name,
                    upper_bound=upper_bound,
                    lower_bound=lower_bound,
                    centile_pct=centile_pct,
                    slot_type_cd=slot_type_cd,
                    table_name=table_name,
                    column_name=column_name,
                    active=constraint_active == "Y",
                )
            if self._test_types.get(f"{ink_type_dim_ky},{arch_id}") is None:
                self._test_types[f"{ink_type_dim_ky},{arch_id}"] = []

            self._test_types[f"{ink_type_dim_ky},{arch_id}"].append(
                TestType(
                    test_type_key=wo_type_dim_ky,
                    name=work_order_type_name,
                    ink_type_dim_ky=ink_type_dim_ky,
                    arch_id=int(arch_id),
                    min_pen_ct=min_pen_ct,
                    days_to_process_wo_ct=days_to_process_wo_ct,
                    active=work_order_active == "Y",
                    constraints=constraints,
                )
            )

    def get_test_types(self, ink_type_dim_ky, arch_id):
        self.logger.info(
            f"get_test_types() for ink type {ink_type_dim_ky} and arch id {arch_id}"
        )

        if self._test_types.get(f"{ink_type_dim_ky},{arch_id}") is None:
            self._get_test_types_from_db(ink_type_dim_ky, arch_id)

        if self._test_types.get(f"{ink_type_dim_ky},{arch_id}") is None:
            return []
        else:
            return copy.deepcopy(self._test_types[f"{ink_type_dim_ky},{arch_id}"])

    def _get_test_types_from_db(self, ink_ky, arch_id):

        test_types_rows = self._db_provider.get_test_types(arch_id, ink_ky)
        for (
            wo_type_dim_ky,
            work_order_type_name,
            ink_type_dim_ky,
            arch_id,
            min_pen_ct,
            days_to_process_wo_ct,
            work_order_active,
        ) in test_types_rows:
            constraints = {}
            for (
                constraint_key,
                test_criteria_ky,
                prod_color_dim_ky,
                upper_bound,
                lower_bound,
                centile_pct,
                slot_type_cd,
                constraint_active,
            ) in self._db_provider.get_constraints(wo_type_dim_ky):
                (
                    table_name,
                    column_name,
                    test_criteria_name,
                ) = self._db_provider.get_test_criteria(constraint_key)

                constraints[constraint_key] = Constraint(
                    constraint_key=constraint_key,
                    prod_color_dim_ky=prod_color_dim_ky,
                    criteria_key=test_criteria_ky,
                    criteria_name=test_criteria_name,
                    upper_bound=upper_bound,
                    lower_bound=lower_bound,
                    centile_pct=centile_pct,
                    slot_type_cd=slot_type_cd,
                    table_name=table_name,
                    column_name=column_name,
                    active=constraint_active == "Y",
                )

            if self._test_types.get(f"{ink_type_dim_ky},{arch_id}") is None:
                self._test_types[f"{ink_type_dim_ky},{arch_id}"] = []
            self._test_types[f"{ink_type_dim_ky},{arch_id}"].append(
                TestType(
                    test_type_key=wo_type_dim_ky,
                    name=work_order_type_name,
                    ink_type_dim_ky=ink_type_dim_ky,
                    arch_id=int(arch_id),
                    min_pen_ct=min_pen_ct,
                    days_to_process_wo_ct=days_to_process_wo_ct,
                    active=work_order_active == "Y",
                    constraints=constraints,
                )
            )
