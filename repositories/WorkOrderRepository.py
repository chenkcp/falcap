import json
import logging
import os
from datetime import date, datetime
from unittest import result

from constants import StatisticServiceState
from constants.StatisticServiceState import StatisticServiceState
from constants.LogStatusMessage import LogStatusMessage
from exceptions.WorkOrderException import WorkOrderException
from models.ConstraintsToTest import ConstraintsToTest
from models.TestType import TestType
from models.WorkOrder import WorkOrder
from services.StatisticService import StatisticService
from collections import Counter

class WorkOrderRepositoryState:
    INVALID_WORK_ORDER_ID = "INVALID_WORK_ORDER_ID"
    NOT_PRINTED_AT_FALCAP = "NOT_PRINTED_AT_FALCAP"
    INVALID_INK_TYPE = "INVALID_INK_TYPE"
    INVALID_ARCH_ID = "INVALID_ARCH_ID"
    INVALID_INV_ITEM_KY = "INVALID_INV_ITEM_KY"
    INVALID_TEST_TYPE = "INVALID_TEST_TYPE"
    INVALID_PEN_COUNT = "INVALID_PEN_COUNT"


class WorkOrderRepository:
    logger = logging.getLogger(__name__)

    def __init__(
        self,
        db_provider,
        test_repository,
        email_service,
        work_order_status_service,
        inventory_color_lookup_service,
        wo_result_fact_repository,
        filters,
        filters_hue2_test_pen_count,
        statistic_service=None,
    ):
        self._test_repository = test_repository
        self._db_provider = db_provider
        self._email_service = email_service
        self._persist_work_order_to_aws = work_order_status_service
        self._inventory_color_lookup_service = inventory_color_lookup_service
        self._wo_result_fact_repository = wo_result_fact_repository
        self._filters = filters
        self._filters_hue2_test_pen_count = filters_hue2_test_pen_count
        self._statistic_service = statistic_service

    # def get_work_orders(self):
    #     self.logger.info("get_work_orders()")
    #     accepted_work_orders = []
    #     rejected_work_orders = []
    #     skipped_work_orders = []

    #     for work_order_id, inv_item_dim_ky in self._db_provider.get_all_work_orders(
    #         self._filters
    #     ):
    #         try:
    #             work_order = self.get_work_order(work_order_id, inv_item_dim_ky, True)
    #             if (
    #                 work_order.pens_count >= work_order.test_type.min_pen_ct
    #                 and work_order.constraints_to_test.should_process_work_order
    #             ):
    #                 work_order.log_workorder_status(LogStatusMessage.accept())
    #                 accepted_work_orders.append(work_order)
    #             else:
    #                 days_to_failure_exceed = (
    #                     self._date_service.compare_dates_to_failure(
    #                         work_order.id,
    #                         work_order.test_type.days_to_process_wo_ct,
    #                     )
    #                 )
    #                 if days_to_failure_exceed:
    #                     self.logger.info(
    #                         f"""Reject: {work_order.id}, it has been more than 
    #                         {work_order.test_type.days_to_process_wo_ct} days"""
    #                     )
    #                     work_order.log_workorder_status(
    #                         LogStatusMessage.block(),
    #                         f"it has been more than {work_order.test_type.days_to_process_wo_ct} days",
    #                     )
    #                     self._email_service.send_rejected_email(work_order)
    #                     work_order.log_email_sent()
    #                     rejected_work_orders.append(work_order)
    #                 else:
    #                     work_order.log_workorder_status(LogStatusMessage.skip())
    #                     skipped_work_orders.append(work_order)
    #                     self.logger.info(
    #                         f"get_work_orders(): Skipping work order {work_order.id}"
    #                     )
    #         except WorkOrderException as e:
    #             work_order = e.work_order
    #             if e.state == WorkOrderRepositoryState.INVALID_PEN_COUNT:
    #                 days_to_failure_exceed = (
    #                     self._date_service.compare_dates_to_failure(
    #                         work_order.id,
    #                         work_order.test_type.days_to_process_wo_ct,
    #                     )
    #                 )
    #                 if days_to_failure_exceed:
    #                     self.logger.info(
    #                         f"""Reject: {work_order.id}, it has been more than
    #                             {work_order.test_type.days_to_process_wo_ct} days"""
    #                     )
    #                     work_order.log_workorder_status(
    #                         LogStatusMessage.block(),
    #                         f"{e} - it has been more than {work_order.test_type.days_to_process_wo_ct} days",
    #                     )
    #                     self._email_service.send_rejected_email(work_order)
    #                     work_order.log_email_sent()
    #                     rejected_work_orders.append(work_order)
    #                     continue
    #             else:
    #                 work_order.log_workorder_status(LogStatusMessage.skip(), str(e))
    #                 skipped_work_orders.append(work_order)
    #                 self.logger.info(
    #                     f"get_work_orders(): Skipping work order {work_order.id} - {e}"
    #                 )

    #     if len(rejected_work_orders) > 0:
    #         self._wo_result_fact_repository.block_work_orders(rejected_work_orders)
    #         self._wo_result_fact_repository.set_email_sent(rejected_work_orders)

    #     # concat skipped and rejected work orders to update the status in dynamodb and s3
    #     skipped_and_rejected_work_orders = skipped_work_orders + rejected_work_orders
    #     if len(skipped_and_rejected_work_orders) > 0:
    #         self._persist_work_order_to_aws.add_work_order(
    #             work_orders=skipped_and_rejected_work_orders
    #         )

    #     return accepted_work_orders

    # def is_printed_at_falcap(self, work_order_id):
    #     get_count_from_ppf_and_pid_row = (
    #         self._db_provider.get_printed_count_for_falcap(work_order_id)
    #     )
    #     self.logger.info(f"get_printed_count_for_falcap() for {work_order_id} is {get_count_from_ppf_and_pid_row[0]}")
    #     return get_count_from_ppf_and_pid_row[0] > 0

    # def get_ink_type_dim_ky(self, work_order_id):
    #     return self._db_provider.get_ink_type_dim_ky(work_order_id)

    # def get_arch_id(self, work_order_id):
    #     return self._db_provider.get_arch_id(work_order_id)

    # def get_prod_colors(self, work_order_id):
    #     return self._db_provider.get_prod_colors(work_order_id)


    def get_work_order(
        self,
        work_order_id,
        inv_item_dim_ky=None,
        skip_work_order_id_check=False,
    ):
        self.logger.info(f"get_work_order() for {work_order_id}")
        work_order = WorkOrder(
            work_order_id, inv_item_dim_ky, None, None, None, None, None
        )
        if not skip_work_order_id_check:
            work_order_row = self._db_provider.get_work_order(work_order_id)
            if not work_order_row:
                self.logger.info(
                    f"""get_work_order(): work order {work_order_id} is 
                                    not found"""
                )
                raise WorkOrderException(
                    f"Work order {work_order_id} is not found",
                    WorkOrderRepositoryState.INVALID_WORK_ORDER_ID,
                    work_order,
                )
            work_order_id = work_order_row[0]
            work_order.id = work_order_row[0]
            work_order.inv_item_dim_ky = work_order_row[1]
            inv_item_dim_ky = work_order_row[1]

        get_count_result = self._db_provider.get_printed_count_for_falcap(work_order_id)
        is_printed_at_falcap = get_count_result[0] > 0 if get_count_result else False
        if not is_printed_at_falcap:
            self.logger.info(
                f"""get_work_order(): work order has not completed processlk_ky 2130 for falcap"""
            )
            raise WorkOrderException(
                f"Work order {work_order_id} is not printed at falcap ofprocesslk_ky 2130",
                WorkOrderRepositoryState.NOT_PRINTED_AT_FALCAP,
                work_order,
            )
        work_order.is_printed_at_falcap = is_printed_at_falcap

        ink_types_rows = self._db_provider.get_ink_type_dim_ky(work_order_id)

        if len(ink_types_rows) == 0 or len(ink_types_rows) > 1:
            self.logger.info(
                f"""get_work_order(): work order {work_order_id} has 
                {len(ink_types_rows)} ink types"""
            )
            raise WorkOrderException(
                f"No data in PTDS.PEN_SLOT_FACT - Work order has {len(ink_types_rows)} ink types",
                WorkOrderRepositoryState.INVALID_INK_TYPE,
                work_order,
            )

        ink_type_dim_ky = ink_types_rows[0][0]
        work_order.ink_type_dim_ky = ink_type_dim_ky

        arch_id_rows = self._db_provider.get_arch_id(work_order_id)
        if len(arch_id_rows) == 0 or len(arch_id_rows) > 1:
            self.logger.info(
                f"""get_work_order(): work order {work_order_id} has 
                {len(arch_id_rows)} arch ids"""
            )
            raise WorkOrderException(
                f"No data in PTDS.PEN_SLOT_FACT - Work order has {len(arch_id_rows)} arch ids",
                WorkOrderRepositoryState.INVALID_ARCH_ID,
                work_order,
            )

        arch_id = arch_id_rows[0][0]
        work_order.arch_id = arch_id

        # check if work order is dual color
        color_lookup = self._inventory_color_lookup_service.get_color_lookup(
            inv_item_dim_ky
        )
        if color_lookup is None:
            self.logger.info(
                f"""get_work_order(): unable to find dual color for {work_order_id}, 
                inv_item_dim_ky: {inv_item_dim_ky}"""
            )
            raise WorkOrderException(
                f"Work order has no dual color for {work_order_id} and inv_item_dim_ky: {inv_item_dim_ky}",
                WorkOrderRepositoryState.INVALID_INV_ITEM_KY,
                work_order,
            )

        work_order.is_dual_color = color_lookup["dual_color"]
        # if work order is dual color but the fluid is transparent, consider it as single color
        if color_lookup["dual_color"] and color_lookup["transparent_fluid"]:
            work_order.is_dual_color = False
        
        # Get color dimension keys from the color names
        color_results = self._db_provider.get_color_keys((color_lookup["EVEN_INK_COLOR_NM"].strip(), color_lookup["ODD_INK_COLOR_NM"].strip()))
        colors = [row[0] for row in color_results] if color_results else []

        #call TestTypeRepository's get_test_types() to get the test type and the constraints of the test type
        test_types = self._test_repository.get_test_types(ink_type_dim_ky, arch_id)
        self.logger.debug(
            f"DEBUG: Retrieved {len(test_types)} test types for work order {work_order_id} - "
            f"ink_type_dim_ky: {ink_type_dim_ky}, arch_id: {arch_id}"
        )
        if len(test_types) != 1:
            self.logger.info(
                f"""get_work_order(): work order {work_order_id} has 
                {len(test_types)} test types"""
            )
            raise WorkOrderException(
                f"Work order has {len(test_types)} test types",
                WorkOrderRepositoryState.INVALID_TEST_TYPE,
                work_order,
            )

        test_type = test_types[0]
        
        # some constraint is defined with color as None or Any, treat them to assume the prod_color_dim_ky from pen_slot_fact
        if hasattr(test_type, 'constraints') and test_type.constraints:
            filtered_constraints = {}
            for key, constraint in test_type.constraints.items():
                # Keep constraint if prod_color_dim_ky is None/empty (generic) or if it exists in colors
                if (constraint.prod_color_dim_ky is None or 
                    constraint.prod_color_dim_ky == 'Any' or 
                    constraint.prod_color_dim_ky == '' or
                    constraint.prod_color_dim_ky in colors):
                    filtered_constraints[key] = constraint
                else:
                    self.logger.info(
                        f"DEBUG: Filtered out constraint {key} - prod_color_dim_ky {constraint.prod_color_dim_ky} not found in colors {colors}"
                    )
            
            # Update test_type with filtered constraints
            test_type.constraints = filtered_constraints

        if len(test_type.constraints) == 0:
            self.logger.info(
                f"""get_work_order(): work order {work_order_id} has 
                0 valid constraints after filtering"""
            )
            raise WorkOrderException(
                f"Work order has 0 constraints. Check actual pen colors against constraint colors in rptds.FCEOLQT_WO_TEST_CNSTR_DIM and DDB.line_ods_falcap_color_lookup.",
                WorkOrderRepositoryState.INVALID_TEST_TYPE,
                work_order,
            )
            
        
        test_type_data = {
            "work_order_id": work_order_id,
            "inv_item_dim_ky": inv_item_dim_ky,
            "ink_type_dim_ky": ink_type_dim_ky,
            "arch_id": arch_id,
            "test_type_data": {
                "min_pen_ct": test_type.min_pen_ct,
                "days_to_process_wo_ct": test_type.days_to_process_wo_ct,
                "constraints": {key: {
                    "constraint_key": constraint.constraint_key,
                    "table_name": constraint.table_name,
                    "column_name": constraint.column_name,
                    "slot_type_cd": constraint.slot_type_cd,
                    "prod_color_dim_ky": constraint.prod_color_dim_ky,
                    "criteria_key": constraint.criteria_key
                } for key, constraint in test_type.constraints.items()} if hasattr(test_type, 'constraints') else {}
            }
        }
            
          
        
        work_order.test_type = test_type

        # Debug log the complete test_type constraint configuration
        self.logger.debug(
            f"DEBUG: TestType loaded for work order {work_order_id} - "
            f"ink_type_dim_ky: {ink_type_dim_ky}, arch_id: {arch_id}"
        )

        
        (pens_count,) = self._db_provider.get_pen_count(
            work_order_id, inv_item_dim_ky, self._filters_hue2_test_pen_count
        )
        work_order.pens_count = pens_count

        if (
            work_order.pens_count is None
            or work_order.pens_count < work_order.test_type.min_pen_ct
        ):
            self.logger.info(
                f"""get_work_order(): work order {work_order_id} has 
                {work_order.pens_count} pens"""
            )
            raise WorkOrderException(
                f"Work order has {work_order.pens_count} pens, minimum is {work_order.test_type.min_pen_ct}",
                WorkOrderRepositoryState.INVALID_PEN_COUNT,
                work_order,
            )

        # select constraints based on new logic
        constraints_to_test = ConstraintsToTest()
        # to track pen count for each constraint so that we don't have to query the db again
        constraints_pen_count = {}

        # Process each constraint from test_type.constraints
        for constraint_key, constraint in test_type_data["test_type_data"]["constraints"].items():
            self.logger.info(
                f"DEBUG: Work order {work_order_id} - Processing constraint {constraint_key}: "
                f"Table: {constraint['table_name']}, "
                f"Column: {constraint['column_name']}, "
                f"Color: {constraint['prod_color_dim_ky']}, "
                f"Criteria: {constraint['criteria_key']}"
            )
            # Determine which colors to process for this constraint
            colors_to_process = []
            if constraint['prod_color_dim_ky'] is None or constraint['prod_color_dim_ky'] == 'Any':   
                self.logger.debug(
                    f"DEBUG: processing constraint {constraint_key} prod_color_dim_ky {constraint['prod_color_dim_ky']} has {len(colors)} colors: {colors}"
                )
                
                # Generic constraint - process all colors found in parametric data
                colors_to_process = colors
                    
                # Check dual color validation for generic constraints
                if work_order.is_dual_color and len(colors) < 2:
                    self.logger.error(
                            f"DEBUG: Work order {work_order_id} not allowed to be tested - DUAL COLOR MISMATCH: "
                            f"Expected dual color but only {len(colors)} colors found in parametric data. "
                            f"Colors: {colors}, Constraint: {constraint_key}, Table: {constraint['table_name']}"
                    )
                    constraints_to_test.should_process_work_order = False
                    constraints_to_test.add_error(
                        f"Work order is dual color but only {len(colors)} colors found in parametric data. "
                        f"Colors: {colors}, Constraint: {constraint_key}, Table: {constraint['table_name']}"
                    )
                    continue
            else:
                # Specific color constraint - only process if that color exists in parametric data
                colors_to_process = [constraint['prod_color_dim_ky']]

               
            self.logger.debug(
                    f"DEBUG: Constraint {constraint_key} will process colors: {colors_to_process}"
                )
            
            # Check pen count for all colors to process
            constraint_has_sufficient_pens = True
            for color in colors_to_process:
                self.logger.debug(
                    f"DEBUG: Iterate constraint {constraint_key} prod_color_dim_ky {color}, table {constraint['table_name']}, column {constraint['column_name']}: "
                )
                # Process pen count for this constraint
                pen_count_from_db = self.get_pen_count_from_parametric_data(
                        work_order_id, constraint['table_name'], constraint['slot_type_cd'], color
                )
                
                # Handle both tuple and direct value returns from database
                if isinstance(pen_count_from_db, (list, tuple)) and len(pen_count_from_db) > 0:
                    pen_count = pen_count_from_db[0]
                elif pen_count_from_db is not None:
                    pen_count = pen_count_from_db  # Direct value (Decimal, int, etc.)
                else:
                    pen_count = 0
 
                
                if pen_count is None or pen_count < test_type.min_pen_ct:
                    self.logger.error(
                        f"DEBUG: Work order {work_order_id} not allowed to be tested - INSUFFICIENT PEN COUNT: "
                        f"Constraint {constraint_key} has pen count {pen_count if pen_count is not None else 'None'}, "
                        f"minimum required: {test_type.min_pen_ct}. "
                        f"Table: {constraint['table_name']}, Column: {constraint['column_name']}"
                    )
                    constraints_to_test.should_process_work_order = False
                    constraints_to_test.add_constraint_error(
                        constraint_key,
                        f"Pen count is {pen_count if pen_count is not None else 'None'}, "
                        f"minimum required: {test_type.min_pen_ct}, "
                        f"constraint key: {constraint_key}, "
                        f"table: {constraint['table_name']}, column: {constraint['column_name']}"
                    )
                    constraint_has_sufficient_pens = False
                    break  # No need to check other colors if one fails
            
            if constraint_has_sufficient_pens:
                constraints_to_test.add_constraint_key(constraint_key)
                # Add color information for this constraint as an array
                constraints_to_test.add_constraint_result(
                    constraint_key, {"work_order_color": colors_to_process}
                )

        constraints_to_test.sort_constraint_keys()
        
        # # Write constraints_to_test to JSON file
        # try:
        #     constraints_data = {
        #         "work_order_id": work_order_id,
        #         "should_process_work_order": constraints_to_test.should_process_work_order,
        #         "calculated_delta_e": constraints_to_test.calculated_delta_e,
        #         "work_order_status": constraints_to_test.work_order_status,
        #         "constraint_keys": constraints_to_test.constraint_keys,
        #         "constraints_errors": constraints_to_test.constraints_errors,
        #         "constraints_results": constraints_to_test.constraints_results,
        #         "errors": constraints_to_test.errors
        #     }
            
        #     filename = f"constraints_to_test_{work_order_id}_{ink_type_dim_ky}_{arch_id}.json"
        #     filepath = os.path.join("tmp", filename)
            
        #     with open(filepath, 'w') as f:
        #         json.dump(constraints_data, f, indent=2)
            
        #     self.logger.info(f"ConstraintsToTest data written to {filepath}")
            
        # except Exception as e:
        #     self.logger.error(f"Failed to write constraints_to_test to JSON file: {e}")
        
        work_order.constraints_to_test = constraints_to_test
        return work_order



    def get_pen_count_from_parametric_data(self, work_order_id, table_name,slot_type_cd,color ):
        return self._db_provider.get_pen_count_from_parametric_data(
            work_order_id,
            table_name,
            slot_type_cd,
            color,
        )

    """
        Get parametric data for a work order, work order must be an instance of WorkOrder
    """

    def get_test_parametric_data(self, work_order_id, table_name,column_name,slot_type_cd ,prod_color_dim_ky):
        self.logger.info(
            f"get_test_parametric_data() for {work_order_id} - {table_name}.{column_name}, {slot_type_cd},{prod_color_dim_ky}"
        )
        data = self._db_provider.get_test_parametric_data(
            work_order_id,
            table_name,
            column_name,
            slot_type_cd,
            prod_color_dim_ky
        )
        return [
            (row[0], float(row[1]) if row[1] is not None else None)
            for row in data
        ]

    def get_failure_reasons(self, work_order_id):
        self.logger.info(f"get_failure_reasons() for {work_order_id}")
        data = self._db_provider.get_failure_reasons(work_order_id)
        reasons = []
        for row in data:
            reason = row[0]
            if row[1]:
                reason += f" ({row[1]})"
            reasons.append(reason)
        return reasons

    def get_workorder(self, work_order_id=None):
        if work_order_id is not None:
            self.logger.info(f"run get_workorder({work_order_id})")
        else:
            self.logger.debug("run get_workorder() for all work orders in the last 15 days")

        #get all work orders in the last 15 days, if work_order_id is provided, filter by that id
        wo_rows = self._db_provider.get_all_work_orders(
            self._filters,
            work_order_id if work_order_id else None
        )

        if not wo_rows:
            msg = "No work orders found" if not work_order_id else f"Work order with ID '{work_order_id}' not found"
            self.logger.error(msg)
            return []

        work_order_models = []
        # select constraints based on new logic
        constraints_to_test = ConstraintsToTest()
        clou_count=0
        hue_count=0 
        noz_clou_count=0
        delta_e_count=0
        #for each work order, get the test types
        for wo_id, inv_item_dim_ky, close_dm in wo_rows:
            try:
                clou_count = self._db_provider.get_clou_count(wo_id)
                self.logger.debug(f"Work order {wo_id} - initial clou count: {clou_count}")
                if clou_count == 0:
                    self.logger.info(f"Work order {wo_id} has no clou count, reloading.")
                    clou_count=self._db_provider.rptds_func_fceolqt_upd_pen_slot_clous_fact(wo_id)
                    
                hue_count = self._db_provider.get_hue_count(wo_id)
                self.logger.debug(f"Work order {wo_id} - initial hue count: {hue_count}")
                if hue_count == 0:
                    self.logger.info(f"Work order {wo_id} has no hue count, reloading.")
                    hue_count=self._db_provider.rptds_func_fceolqt_upd_pen_slot_hue_2_fact(wo_id)
                    
                noz_clou_count = self._db_provider.get_noz_clou_count(wo_id)
                self.logger.debug(f"Work order {wo_id} - initial noz clou count: {noz_clou_count}")
                if noz_clou_count == 0:
                    self.logger.info(f"Work order {wo_id} has no noz clou count, reloading.")
                    noz_clou_count=self._db_provider.rptds_func_fceolqt_upd_pen_noz_clous_fact(wo_id)
                    self.logger.debug(f"Work order {wo_id} - updated noz clou count: {noz_clou_count}")

                delta_e_count = self._db_provider.get_delta_e_count(wo_id)
                self.logger.debug(f"Work order {wo_id}  - initial delta_e count: {delta_e_count}")
                if delta_e_count == 0:
                    self.logger.info(f"Work order {wo_id} has no delta_e_count, reloading.")
                    delta_e_count = self._db_provider.rptds_func_fceolqt_calc_delta_e_for_slottypecd_2(wo_id, None)
                    self.logger.debug(f"Work order {wo_id} - updated delta_e count: {delta_e_count}")

                self.logger.debug(f"before - count_noz_clou: {noz_clou_count}, clou_count:{clou_count}, delta_e_count:{delta_e_count}") 
               
                #get the data fro pen_slot_fact table
                wo_info = self._db_provider.get_arch_id_color_ink_slot_type_ky(wo_id) or []
                self.logger.debug(f"Work order {wo_id} - get_arch_id_color_ink_slot_type_ky returned {len(wo_info)} rows")
                if wo_info is None:
                    self.logger.info(f"get_workorder(): No data found in pen_slot_fact for work order {wo_id}")
                
                #need arch id and ink_type_dim_ky to get the test type
                first_arch_id = wo_info[0][0] if wo_info and wo_info[0][0] is not None else None
                first_ink_type_dim_ky = wo_info[0][2] if wo_info and wo_info[0][2] is not None else None

                prod_color_set = set()
                slot_type_set = set()
                for arch_id, prod_color_dim_ky, ink_type_dim_ky, slot_type_cd in wo_info:
                    if prod_color_dim_ky is not None:
                        prod_color_set.add(int(prod_color_dim_ky))
                    if slot_type_cd is not None and str(slot_type_cd).strip():
                        slot_type_set.add(str(slot_type_cd).strip())

                # these sets are from pen_slot_fact table
                distinct_prod_color_dim_keys = sorted(prod_color_set)
                distinct_slot_type_codes = sorted(slot_type_set)

                # close date normalization
                wo_close_dm = None
                if close_dm:
                    wo_close_dm = close_dm.split("T")[0] if "T" in close_dm else str(close_dm)

                # dual color lookup from DDB table is to get the number of colors defined by inv_item_dim_ky
                color_lookupDDB = self._inventory_color_lookup_service.get_color_lookup(inv_item_dim_ky)
                is_dual_color_DDB = bool(color_lookupDDB["dual_color"]) if color_lookupDDB else False
                if color_lookupDDB and color_lookupDDB.get("dual_color") and color_lookupDDB.get("transparent_fluid"):
                    is_dual_color_DDB = False

                # check the qty of color of the pen_slot_fact table has the number of pen color that match the qty of color from DDB
                if is_dual_color_DDB and len(distinct_prod_color_dim_keys)  < 2:
                    constraints_to_test.should_process_work_order = False
                    constraints_to_test.add_error(
                        f"""Work order is dual color but only 1 color is found in the parametric 
                    data. Colors found: {distinct_prod_color_dim_keys}"""
                    )

                # # check if we have pen counts to carry out the test type
                # pen_count_result = self._db_provider.get_clou_hue_delta_e_noz_clou_count(wo_id)
                # if pen_count_result:
                #     # Unpack the result tuple with meaningful names based on column order
                #     count_clou, count_hue, count_delta_e, count_noz_clou = pen_count_result[0]
                #     # Handle None values by defaulting to 0
                #     count_clou = count_clou or 0
                #     count_hue = count_hue or 0 
                #     count_delta_e = count_delta_e or 0
                #     count_noz_clou = count_noz_clou or 0
                # else:
                #     count_clou = count_hue = count_delta_e = count_noz_clou = 0

                # if no clou+hue, mark as SKIP (not exception-driven control flow)
                # if count_clou == 0 and count_hue == 0:
                #     work_order_model = WorkOrder(
                #         work_order_id=wo_id,
                #         inv_item_dim_ky=inv_item_dim_ky,
                #         ink_type_dim_ky=first_ink_type_dim_ky,
                #         arch_id=first_arch_id,
                #         test_type=None,
                #         pens_count=None,
                #         is_printed_at_falcap=False,
                #         wo_close_dm=wo_close_dm,
                #         wo_slot_type_cd=distinct_slot_type_codes,
                #         prod_color_dim_kys=distinct_prod_color_dim_keys,
                #         test_type_count=0,
                #         filtered_constraint_count=0,
                #         is_dual_color=is_dual_color_DDB,
                #     )
                #     work_order_model.log_workorder_status(
                #         LogStatusMessage.skip(),
                #         f"No clou/hue parametric data for {wo_id}, not testable."
                #     )
                #     work_order_models.append(work_order_model)
                #     continue

                # get test types
                test_types = self._test_repository.get_test_types(first_ink_type_dim_ky, first_arch_id)
                self.logger.info(
                    f"Work order {wo_id} - Retrieved test_types for ink_type={first_ink_type_dim_ky}, arch_id={first_arch_id}: {len(test_types)} test types found"
                )
                
       
                if not test_types:
                    work_order_model = WorkOrder(
                        work_order_id=wo_id,
                        inv_item_dim_ky=inv_item_dim_ky,
                        ink_type_dim_ky=first_ink_type_dim_ky,
                        arch_id=first_arch_id,
                        test_type=None,
                        pens_count=None,
                        is_printed_at_falcap=True,
                        wo_close_dm=wo_close_dm,
                        wo_slot_type_cd=distinct_slot_type_codes,
                        prod_color_dim_kys=distinct_prod_color_dim_keys,
                        test_type_count=0,
                        filtered_constraint_count=0,
                        is_dual_color=is_dual_color_DDB,
                    )
                    work_order_model.log_workorder_status(
                        LogStatusMessage.skip(),
                        f"No test types for ink_type={first_ink_type_dim_ky}, arch_id={first_arch_id}."
                    )
                    work_order_models.append(work_order_model)
                    continue

                test_type = test_types[0]
                #debug print the test type and constraints
                # if test_type:
                #     self.logger.debug(f"test_type_name: {test_type.name}")
                #     self.logger.debug(f"min_pen_ct: {test_type.min_pen_ct}")
                #     self.logger.debug(f"days_to_process_wo_ct: {test_type.days_to_process_wo_ct}")
                #     self.logger.debug(f"test_clou: {test_type.test_clou}")
                #     for constraint_id, constraint in test_type.constraints.items():
                #         self.logger.debug(f"Constraint {constraint_id}: name={constraint.criteria_name}, key={constraint.criteria_key}, slot_type_cd={constraint.slot_type_cd}")
                #         self.logger.debug(f"table = {constraint.table_name}, column = {constraint.column_name}, prod_color_dim_ky={constraint.prod_color_dim_ky}, lower_bound={constraint.lower_bound}, upper_bound={constraint.upper_bound}")

                # check if the color key in pen_slot_fact matches the color defined in DDB
                if color_lookupDDB:
                    color_results = self._db_provider.get_color_keys(
                        (color_lookupDDB["EVEN_INK_COLOR_NM"].strip(), color_lookupDDB["ODD_INK_COLOR_NM"].strip())
                    ) or []
                    colors_DDB = [row[0] for row in color_results] if color_results else []
                    if Counter(distinct_prod_color_dim_keys) != Counter(colors_DDB):
                        self.logger.debug(
                            f"Work order {wo_id} - Retrieved color keys discrepancy from param data {distinct_prod_color_dim_keys} and dynamodb {colors_DDB}"
                        )
                        # If mismatch should BLOCK in your rules, do it here
                        # (Leaving as debug-only unless you confirm it is a hard rule.)

                
                # filter constraints
                filtered_constraints = {}
                test_type.test_clou= True
                test_type.test_hue=True
                self.logger.debug(f"after - count_noz_clou: {noz_clou_count}, clou_count:{clou_count}, delta_e_count:{delta_e_count}") 
                
                self.logger.debug(f"Work order {wo_id} - Starting constraint processing")
                        
                #assign the prod_color_dim_ky for constraints with generic color (None or Any) to be the color keys from pen_slot_fact, so that the test will be carried out for all colors for those generic constraints
                self.logger.debug(f"Work order {wo_id} - Checking test_type.constraints: type={type(test_type.constraints)}, length={len(test_type.constraints) if test_type.constraints else 'None'}")
                if test_type.constraints and isinstance(test_type.constraints, dict):
                    self.logger.debug(f"Work order {wo_id} - Processing {len(test_type.constraints)} constraints")
                    for key, constraint in test_type.constraints.items():
                        self.logger.debug(f"Work order {wo_id} - Processing constraint key: {key}")
                        self.logger.debug(f"Work order {wo_id} - Constraint {key}: prod_color_dim_ky={constraint.prod_color_dim_ky}")
                        
                        if constraint.prod_color_dim_ky is None or constraint.prod_color_dim_ky in ("Any", ""):
                            self.logger.debug(f"Work order {wo_id} - Generic constraint {key} will process colors: {distinct_prod_color_dim_keys}")
                            # Create separate constraint instances for each color to maintain hashability
                            # This ensures each constraint has a single, hashable prod_color_dim_ky value
                            for i, color_key in enumerate(distinct_prod_color_dim_keys):
                                # Create a unique key for each color-specific constraint instance
                                # Use consistent format: {original_key}_color_{color_value}
                                color_specific_key = f"{key}_color_{color_key}"
                                
                                # Create a copy of the constraint with the specific color
                                import copy
                                color_constraint = copy.deepcopy(constraint)
                                color_constraint.prod_color_dim_ky = color_key
                                
                                filtered_constraints[color_specific_key] = color_constraint
                                self.logger.debug(f"Work order {wo_id} - Created color-specific constraint {color_specific_key} for color {color_key}")
                        elif isinstance(distinct_prod_color_dim_keys, list) and constraint.prod_color_dim_ky in distinct_prod_color_dim_keys:
                            self.logger.debug(f"Work order {wo_id} - Specific constraint {key} will process color: {constraint.prod_color_dim_ky}")
                            # Use consistent naming format for all constraints: {original_key}_color_{color_value}
                            color_specific_key = f"{key}_color_{constraint.prod_color_dim_ky}"
                            filtered_constraints[color_specific_key] = constraint
                            self.logger.debug(f"Work order {wo_id} - Added specific constraint {color_specific_key} for color {constraint.prod_color_dim_ky}")
                        else:
                            #bypass test ++
                            self.logger.info(
                                f"Filtered out constraint {key} - prod_color_dim_ky {constraint.prod_color_dim_ky} not found in pen_slot_fact's colors {distinct_prod_color_dim_keys}"
                            )


                        self.logger.debug(f"Work order {wo_id} - noz_clou_count {noz_clou_count}, clou_count {clou_count}")
                        try:
                            self.logger.debug(f"Work order {wo_id} - About to access constraint {key} properties")
                            table_name = constraint.table_name
                            column_name = constraint.column_name  
                            min_pen_ct = test_type.min_pen_ct
                            self.logger.debug(f"Work order {wo_id} - Successfully accessed constraint {key} properties: table={table_name}, column={column_name}")
                            self.logger.debug(f"Work order {wo_id} - Evaluating constraint {key} for table:{table_name}, column:{column_name}, min:{min_pen_ct}")
                        except Exception as property_access_error:
                            self.logger.error(f"Work order {wo_id} - Error accessing properties for constraint {key}: {property_access_error}")
                            raise 
                        
                        try:
                            if table_name == 'PEN_NOZZLE_COLUMN_FACT' and noz_clou_count < min_pen_ct:
                                self.logger.debug(f"Work order {wo_id} - Setting test_clou=False for constraint {key}")
                                test_type.test_clou =  False
                                self.logger.debug(
                                f"Work order {wo_id}, column name {column_name}, table name {table_name}, noz_clou_count {noz_clou_count} < {min_pen_ct}")
                            elif table_name == 'PEN_SLOT_FACT' and  column_name[:3] =='CAP' and clou_count < min_pen_ct:
                                self.logger.debug(f"Work order {wo_id} - Setting test_clou=False for constraint {key} (CAP column)")
                                test_type.test_clou =False 
                                self.logger.debug(
                                f"Work order {wo_id}, column name {column_name}, table name {table_name}, clou_count {clou_count} < {min_pen_ct}")
                            elif table_name == 'PEN_SLOT_FACT' and  column_name[:5] =='DELTA' and delta_e_count < min_pen_ct:
                                self.logger.debug(f"Work order {wo_id} - Setting test_hue=False for constraint {key} (DELTA column)")
                                test_type.test_hue = False 
                                self.logger.debug(
                                f"Work order {wo_id}, column name {column_name}, table name {table_name}, delta_e_count {delta_e_count} < {min_pen_ct}")
                        except Exception as constraint_eval_error:
                            self.logger.error(f"Work order {wo_id} - Error evaluating constraint {key}: {constraint_eval_error}")
                            
                    self.logger.debug(f"Work order {wo_id} -final - noz_clou_count {noz_clou_count}, clou_count {clou_count}")
                    self.logger.debug(f"Work order {wo_id} - About to assign filtered constraints. Count: {len(filtered_constraints)}")
                    test_type.constraints = filtered_constraints
                    self.logger.debug(f"Work order {wo_id} - Successfully assigned filtered constraints")

                    # for key, constraint in test_type.constraints.items():
                    #     constraint_data = {
                    #         'constraint_key': constraint.constraint_key,
                    #         'criteria_key': constraint.criteria_key,
                    #         'criteria_name': constraint.criteria_name, 
                    #         'prod_color_dim_ky': constraint.prod_color_dim_ky,
                    #         'upper_bound': constraint.upper_bound,
                    #         'lower_bound': constraint.lower_bound,
                    #         'centile_pct': constraint.centile_pct,
                    #         'slot_type_cd': constraint.slot_type_cd,
                    #         'table_name': constraint.table_name, 
                    #         'column_name': constraint.column_name,
                    #         'active': constraint.active
                    #     }
                    #     self.logger.info(f"  Constraint data: {constraint_data}")
  
                work_order_model = WorkOrder(
                    work_order_id=wo_id,
                    inv_item_dim_ky=inv_item_dim_ky,
                    ink_type_dim_ky=first_ink_type_dim_ky,
                    arch_id=first_arch_id,
                    test_type=test_type,
                    pens_count=None,
                    is_printed_at_falcap=True,
                    wo_close_dm=wo_close_dm,
                    wo_slot_type_cd=distinct_slot_type_codes,
                    prod_color_dim_kys=distinct_prod_color_dim_keys,
                    test_type_count=len(test_types),
                    filtered_constraint_count=len(filtered_constraints),
                    is_dual_color=is_dual_color_DDB,
                )

          

                # Skip priority 1 - no test type
                if work_order_model.test_type_count ==0:
                    work_order_model.log_workorder_status(
                        LogStatusMessage.skip(),
                        f"No test type found for arch_id={test_type.arch_id} and ink_type_dim_ky={test_type.ink_type_dim_ky}."
                    )
                    work_order_models.append(work_order_model)
                    continue

                 # Skip priority 2 -  pen sufficiency gate after reload attempts
                if not test_type.test_clou:
                    work_order_model.log_workorder_status(
                        LogStatusMessage.skip(),
                        f"Insufficient pen counts for clouseau attempts (min={test_type.min_pen_ct})."
                    )
                    work_order_models.append(work_order_model)
                    continue
                elif not test_type.test_hue:
                    self.logger.error(f"Insufficient historical pen counts for delta-e attempts (min={test_type.min_pen_ct}, ink_type={first_ink_type_dim_ky}, arch_id={first_arch_id})."
                    )
                    #work_order_models.append(work_order_model)
                    

                # otherwise GOOD
                work_order_model.log_workorder_status(LogStatusMessage.accept(), "Valid work order.")
                work_order_models.append(work_order_model)

            except Exception as e:
                self.logger.error(f"Error processing work order {wo_id}: {e}")

                wo_close_dm = None
                if close_dm:
                    wo_close_dm = close_dm.split("T")[0] if "T" in close_dm else str(close_dm)

                failed = WorkOrder(
                    work_order_id=wo_id,
                    inv_item_dim_ky=inv_item_dim_ky,
                    ink_type_dim_ky=None,
                    arch_id=None,
                    test_type=None,
                    pens_count=None,
                    is_printed_at_falcap=False,
                    wo_close_dm=wo_close_dm,
                    wo_slot_type_cd=[],
                    prod_color_dim_kys=[],
                    test_type_count=0,
                    filtered_constraint_count=0,
                    is_dual_color=False,
                )
                failed.log_workorder_status(
                    LogStatusMessage.skip(),
                    f"Exception encountered, skipping testing: {e}"
                )
                work_order_models.append(failed)

        return work_order_models
    
    @property
    def email_service(self):
        return self._email_service

    @email_service.setter
    def email_service(self, email_service):
        self._email_service = email_service

    @property
    def persist_work_order_to_aws(self):
        return self._persist_work_order_to_aws

    @persist_work_order_to_aws.setter
    def persist_work_order_to_aws(self, persist_work_order_to_aws):
        self._persist_work_order_to_aws = persist_work_order_to_aws

    @property
    def statistic_service(self):
        return self._statistic_service

    @statistic_service.setter
    def statistic_service(self, statistic_service):
        self._statistic_service = statistic_service
