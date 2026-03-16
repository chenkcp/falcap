import logging
import json
import math
import numpy as np

from constants.LogStatusMessage import LogStatusMessage
from constants.StatisticServiceState import StatisticServiceState
from datetime import date, datetime
from exceptions import WorkOrderException
from models.ConstraintsToTest import ConstraintsToTest
from models.WorkOrder import WorkOrder
from statistic_strategy.DatabasePercentileStrategy  import DatabasePercentileStrategy 
from statistic_strategy.NumpyPercentileStrategy import NumpyPercentileStrategy


"""
Statistic Service
This service is responsible for testing work orders, constraints and delta_e.
"""


class StatisticService:
    logger = logging.getLogger(__name__)
    HUE2_ASTAR_AV = "HUE2_ASTAR_AV"
    HUE2_BSTAR_AV = "HUE2_BSTAR_AV"
    HUE2_LSTAR_AV = "HUE2_LSTAR_AV"

    def __init__(
        self,
        db_provider,
        work_order_repository,
        wo_test_fact_repository,
        wo_result_fact_repository,
        pen_slot_fact_repository,
        percentile_strategy=None,
        should_update=False,
    ):
        self._db_provider = db_provider
        self._work_order_repository = work_order_repository
        self._wo_test_fact_repository = wo_test_fact_repository
        self._wo_result_fact_repository = wo_result_fact_repository
        self._pen_slot_fact_repository = pen_slot_fact_repository
        self._percentile_strategy = percentile_strategy
        self._should_update = should_update

    def test_work_order(self, workorder, trigger_type=""):
        
        if workorder is None:
            self.logger.error("test_work_order(): Received None work_order parameter")
            return None
            
        if not isinstance(workorder, WorkOrder):
            self.logger.error(f"test_work_order(): Expected WorkOrder model parameter, got {type(workorder)}")
            return None
            
        work_order_id = workorder.id
        if work_order_id is None:
            self.logger.error("test_work_order(): work_order_id not found in work_order parameter")
            return None
        
        self.logger.info(f"test_work_order(): {work_order_id} - Start testing work order")

        # Print WorkOrder model as JSON for debugging
        # try:
        #     import json
        #     from decimal import Decimal
        #     from datetime import datetime, date
            
        #     def clean_for_json(obj):
        #         """Convert objects for JSON serialization"""
        #         if isinstance(obj, dict):
        #             return {(str(k) if isinstance(k, Decimal) else k): clean_for_json(v) for k, v in obj.items()}
        #         elif isinstance(obj, list):
        #             return [clean_for_json(item) for item in obj]
        #         elif isinstance(obj, Decimal):
        #             return float(obj)
        #         elif isinstance(obj, (datetime, date)):
        #             return obj.isoformat()
        #         elif hasattr(obj, '__dict__'):
        #             # Handle custom objects by converting to dict
        #             return clean_for_json(obj.__dict__)
        #         else:
        #             return obj
            
        #     work_order_dict = {
        #         "work_order_id": workorder.id,
        #         "inv_item_dim_ky": workorder.inv_item_dim_ky,
        #         "wo_close_dm": workorder.wo_close_dm,
        #         "wo_slot_type_cd": workorder.wo_slot_type_cd,
        #         "arch_id": workorder.arch_id,
        #         "ink_type_dim_ky": workorder.ink_type_dim_ky,
        #         "prod_color_dim_kys": workorder.prod_color_dim_kys,
        #         "is_printed_at_falcap": workorder.is_printed_at_falcap,
        #         "is_dual_color": workorder.is_dual_color,
        #         "test_type_count": workorder.test_type_count,
        #         "filtered_constraint_count": workorder.filtered_constraint_count,
        #         "test_type": {
        #             "test_type_key": workorder.test_type.test_type_key,
        #             "min_pen_ct": workorder.test_type.min_pen_ct,
        #             "days_to_process_wo_ct": workorder.test_type.days_to_process_wo_ct,
        #             "test_clou": getattr(workorder.test_type, 'test_clou', None),
        #             "test_hue": getattr(workorder.test_type, 'test_hue', None),
        #             "constraints": clean_for_json(workorder.test_type.constraints) if workorder.test_type.constraints else {}
        #         } if workorder.test_type else None
        #     }
            
        #     cleaned_model = clean_for_json(work_order_dict)
        #     self.logger.info(f"DEBUG: WorkOrder model for {work_order_id}: {json.dumps(cleaned_model, indent=2)}")
            
        # except Exception as e:
        #     self.logger.error(f"Failed to print WorkOrder model JSON for {work_order_id}: {e}")

        # Use the WorkOrder model object directly
        work_order = workorder
        constraints_to_test = ConstraintsToTest()
        test_type_count = workorder.test_type_count
        #should not have test type count of 0, any workorder that coming to thi sfunction should have test 
        if test_type_count == 0:
            self.logger.info(f"test_work_order(): work_order_id: {work_order_id} - No test type count found, skipping testing for this work order")
            raise WorkOrderException(
                f"Work order has {test_type_count} test types",
                self._work_order_repository.WorkOrderRepositoryState.INVALID_TEST_TYPE,
                work_order,
            )

        test_type = workorder.test_type
       
        # # Get values from test_type model if it exists
        # test_clou = test_type.test_clou if test_type else False
        # test_hue = test_type.test_hue if test_type else False
        # min_pen_ct = test_type.min_pen_ct if test_type else 0


        # if not test_clou:
        #     self.logger.info(f"Work order id {work_order_id} has insufficient clou or noz clou data count of clou pen data, skipping.")
        #     #return StatisticServiceState.SKIP
        # elif not test_hue:  
        #     self.logger.info(f"Work order id {work_order_id} has insufficient hue or delta-e data count of hue pen data, skipping.")
        #     #return StatisticServiceState.SKIP
        
        # Initialize work order status
        work_order_passed_state = StatisticServiceState.PASS
        constraints_percentile_results = []
        delta_e_results = []
        # Process constraints from test_type model
        if test_type and test_type.constraints:
            constraints = test_type.constraints
            min_pen_ct= test_type.min_pen_ct if test_type.min_pen_ct is not None else 0
            
            for constraint_id, constraint in constraints.items():
                self.logger.debug(f"Processing constraint {constraint_id}")
                constraints_to_test.add_constraint_key(constraint.constraint_key)
                table_name = constraint.table_name
                column_name = constraint.column_name
                slot_type_cd= constraint.slot_type_cd
                prod_color_dim_ky= constraint.prod_color_dim_ky
                self.logger.debug(f"DEBUG: colors_to_process {constraint.prod_color_dim_ky}, slot_type_cd {constraint.slot_type_cd} - table_name: {table_name}, column_name: {column_name}")
                # colors_to_process = []
                # if constraint.prod_color_dim_ky is None or constraint.prod_color_dim_ky == 'Any':   
                #     # Generic constraint - process all colors found in parametric data
                #     colors_to_process = wo_prod_color_dim_kys
                # else:
                #     # Specific constraint - only process the specified color
                #     colors_to_process = [constraint.prod_color_dim_ky]
                #  # Check if no colors to process , assumption that any pen should have ink - return SKIP
                # if not colors_to_process:
                #     self.logger.info(f"Work order id {work_order_id} has no colors to process for constraint {constraint_id}, skipping.")
                #     return StatisticServiceState.SKIP
                
                # slot_code_to_process=[] # can accept NULL slot_cd
                # if constraint.slot_type_cd is not None and constraint.slot_type_cd != 'Any':
                #     slot_code_to_process = [constraint.slot_type_cd]
                # else:
                #     slot_code_to_process = wo_slot_type_cd
 
                # # Check if no slots to process - use NULL for database query
                # if not slot_code_to_process:
                #     slot_code_to_process = [None]
                #     self.logger.debug(f"DEBUG: Constraint {constraint_id} - Empty slot list, using NULL for query: {slot_code_to_process}")

                
                # Process single prod_color_dim_ky value (no iteration needed since each constraint now has a single color)
                color_dim_ky = prod_color_dim_ky  # Use the single color value from the constraint
                parametric_data_ret = self._work_order_repository.get_test_parametric_data(
                    work_order_id,
                    table_name,
                    column_name,
                    slot_type_cd,
                    color_dim_ky
                )
                #self.logger.debug(f"DEBUG: Retrieved parametric data for constraint {constraint_id}, color_dim_ky {color_dim_ky}, slot_type_cd {slot_type_cd} - raw data: {parametric_data_ret}")
                if parametric_data_ret:
                    pn_id, parametric_data = zip(*parametric_data_ret)
                    pn_id = list(pn_id)
                    parametric_data = list(parametric_data)
                else:
                    pn_id = []
                    parametric_data = []
                self.logger.info(f"""test_work_order(): work_order_id: {work_order_id} - Proceeding constraint {constraint_id} with slot type {slot_type_cd} and prod color {color_dim_ky}""")
                #the data should be more than min pen count when work order is running this test
                if  len(parametric_data) == 0 or len(parametric_data)< min_pen_ct:
                    self.logger.info(f"""test_work_order(): work_order_id: {work_order_id} - No parametric data found for constraint {constraint_id} with slot type {slot_type_cd} and prod color {color_dim_ky}, unable to test this constraint""")
                    work_order.log_test_status(
                        LogStatusMessage.constraint_skip(
                            constraint_id,
                            f"""No parametric data in {table_name}:{column_name}"""   
                        )
                    )
              
                    constraints_to_test.add_constraint_error(
                        constraint.constraint_key,
                        f"""Pen count for color {color_dim_ky} is 
                        { len(parametric_data) if  len(parametric_data) else "None"}
                        minimum pen: {test_type.min_pen_ct}, constraint key: {constraint.constraint_key},
                        criteria key: {constraint.criteria_key}, table name: {constraint.table_name},
                        column name: {constraint.column_name}""",
                    )
                    result = {"state": StatisticServiceState.SKIP, "reason": f"constrain name: {constraint.criteria_name} - No parametric data"}

                    constraints_to_test.add_constraint_result(f"""{constraint_id}_{color_dim_ky}""", result)
                    work_order.constraints_to_test= constraints_to_test

                else:
                    result = self.test_constraint(work_order_id, constraint, parametric_data)

                    #if this is trigger by frontend UI's test_statistic page
                    if trigger_type=="statistic_test":
                        self.logger.info(f"""test_work_order(): work_order_id: {work_order_id} - Test statistic flag is True""")
                        database_percentile_strategy = DatabasePercentileStrategy(self._db_provider)
                        self.percentile_strategy = (database_percentile_strategy)
                        result["manual"] = self.test_constraint(work_order.id, constraint, parametric_data)
                        result["parametric_data"] = [float(x) if x is not None else None for x in parametric_data]

                        # constraints_percentile_results.append(
                        #     {
                        #         "FCEOLQT_TEST_CRITERIA_DIM_KY": int(constraint.criteria_key) if constraint.criteria_key is not None else None,
                        #         "TABLE_NAME": constraint.table_name,
                        #         "COLUMN_NAME": constraint.column_name,
                        #         "TEST_CRITERIA_NAME": constraint.criteria_name,
                        #         "CONSTRAINT_UPPER_BOUND": float(constraint.upper_bound) if constraint.upper_bound is not None else None,
                        #         "CONSTRAINT_LOWER_BOUND": float(constraint.lower_bound) if constraint.lower_bound is not None else None,
                        #         "CONSTRAINT_CENTILE_PCT": float(constraint.centile_pct) if constraint.centile_pct is not None else None,
                        #         "SLOT_TYPE_CD": constraint.slot_type_cd,
                        #         "result": result,
                        #     }
                        # )
                        self.logger.info(f"""test_work_order(): work_order_id: {work_order_id} - Constraint {constraint.criteria_key} slot_type_cd: {constraint.slot_type_cd}""")
                        if constraint.criteria_key == 7:
                                delta_e_results = self.test_delta_e(
                                    work_order.id,
                                constraint.slot_type_cd, pn_id
                            )

                        #self.logger.debug(f"Adding constraint result for {constraint_id}: {result}")
                        # Format the result according to the required API specification
                        formatted_result = {
                            "lower_bound_value": result.get("lower_bound_value"),
                            "upper_bound_value": result.get("upper_bound_value"),
                            "difference": result.get("difference", 0),
                            "parametric_data": result.get("parametric_data", []),
                            "manual": {
                                "lower_bound_value": result.get("manual", {}).get("lower_bound_value"),
                                "upper_bound_value": result.get("manual", {}).get("upper_bound_value"),
                                "difference": result.get("manual", {}).get("difference", 0)
                            } if result.get("manual") else None
                        }
                        
                        constraints_percentile_results.append(
                            {
                                "FCEOLQT_TEST_CRITERIA_DIM_KY": str(constraint.criteria_key) if constraint.criteria_key is not None else None,
                                "TEST_CRITERIA_NAME": constraint.criteria_name,
                                "SLOT_TYPE_CD": constraint.slot_type_cd,
                                "result": formatted_result
                            }
                        )
     
                    #process the result from the test of constraint and log the test status of the work order based on the result of the constraint test
                    if result["state"] == StatisticServiceState.FAIL:
                        self.logger.info(
                            f"""test_work_order(): Work order: {work_order.id},
                            failed constraint - {constraint_id}"""
                        )

                        work_order.log_test_status(
                            LogStatusMessage.constraint_fail(
                                f"""{constraint_id}_{color_dim_ky}""",
                                f"""product_colour: {color_dim_ky}, lower_bound: {constraint.lower_bound}, upper_bound: {constraint.upper_bound},
                                lower_bound_value: {result['lower_bound_value']} 
                                upper_bound_value: {result['upper_bound_value']}""",
                            )
                        )

                    elif result["state"] == StatisticServiceState.PASS:
                        self.logger.info(
                            f"""test_work_order(): Work order: {work_order.id}, 
                            passed constraint - {constraint_id}"""
                        )
                        work_order.log_test_status(
                            LogStatusMessage.constraint_pass(
                                f"""product_colour: {color_dim_ky}{constraint.lower_bound} < {result['lower_bound_value']} 
                                            < {result['upper_bound_value']} < {constraint.upper_bound}""",
                            )
                        )
                    self.logger.debug(f"test_work_order(): work_order_id: {work_order_id} - Finished testing constraint {constraint_id}  for color {color_dim_ky} with result: {result}")
                    
                    # Extract the actual constraint code (e.g., "1203" from "1203_color_123") for database storage
                    actual_constraint_id = constraint_id.split('_color_')[0] if '_color_' in constraint_id else constraint_id
                    
                    #write to RPTDS fceolqt_wo_test_fact table for each constraint result
                    self._wo_test_fact_repository.update_wo_test_fact_falconDB(
                        work_order.id,
                        actual_constraint_id,
                        result["state"],
                        color_dim_ky
                    )

                    # prepare this for DDB table update with constraint key + color dim ky combination as unique key
                    constraints_to_test.add_constraint_result(f"""{constraint_id}""", result)
                    work_order.constraints_to_test= constraints_to_test
          

                    #debug log the work_order.constraints_to_test content
                    try:
                        constraints_to_test_content = {
                            "constraint_keys": getattr(work_order.constraints_to_test, 'constraint_keys', None),
                            "constraints_results": getattr(work_order.constraints_to_test, 'constraints_results', None),
                            "constraints_errors": getattr(work_order.constraints_to_test, 'constraints_errors', None),
                            "work_order_status": getattr(work_order.constraints_to_test, 'work_order_status', None),
                            "should_process_work_order": getattr(work_order.constraints_to_test, 'should_process_work_order', None),
                            "type": str(type(work_order.constraints_to_test))
                        }
                        #self.logger.debug(f"Current state of work_order.constraints_to_test content: {constraints_to_test_content}")
                    except Exception as e:
                        self.logger.debug(f"Error printing constraints_to_test content: {e}")
                        self.logger.debug(f"constraints_to_test object: {work_order.constraints_to_test}")
       
        self.logger.debug(f"Completed processing work order {work_order.id}")
        
        # Initialize constraint count variables
        total_constraints = 0
        passed_constraints = 0
        failed_constraints = 0
        skipped_constraints = 0
        blocked_constraints = 0

        # Check work order result vs individual constraint results
        if test_type and test_type.constraints and hasattr(work_order, 'constraints_to_test'):
            constraint_results = work_order.constraints_to_test.constraints_results if hasattr(work_order.constraints_to_test, 'constraints_results') else {}
            
            #self.logger.debug(f"DEBUG: constraint_results structure: {constraint_results}")
            #self.logger.debug(f"DEBUG: constraint_results type: {type(constraint_results)}")
            #self.logger.debug(f"DEBUG: work_order.constraints_to_test attributes: {dir(work_order.constraints_to_test)}")
            
            total_constraints = len(test_type.constraints)
            
            # Collect all constraint states for logging
            all_constraint_states = []
            for result in constraint_results.values():
                if isinstance(result, dict):
                    state = result.get('state')
                    all_constraint_states.append(state)
            
            passed_constraints = sum(1 for result in constraint_results.values() if isinstance(result, dict) and result.get('state') == StatisticServiceState.PASS)
            failed_constraints = sum(1 for result in constraint_results.values() if isinstance(result, dict) and result.get("state") ==  StatisticServiceState.FAIL)
            skipped_constraints = sum(1 for result in constraint_results.values() if isinstance(result, dict) and result.get('state') == StatisticServiceState.SKIP)
            blocked_constraints = sum(1 for result in constraint_results.values() if isinstance(result, dict) and result.get('state') == StatisticServiceState.BLOCKED) 
            
            self.logger.info(f"All constraint states: {all_constraint_states}")
            self.logger.info(f"Constraint counts - Total: {total_constraints}, Passed: {passed_constraints}, Failed: {failed_constraints}, Skipped: {skipped_constraints}, Blocked: {blocked_constraints}")     


            # Adjust work order result based on constraint results
            if total_constraints > 0:
                if failed_constraints > 0 or skipped_constraints > 0 or blocked_constraints > 0:
                    # Any failures → Work Order = FAIL
                    work_order_passed_state = StatisticServiceState.FAIL
                    self.logger.info(f"Work Order {work_order.id}: {failed_constraints} constraints failed, setting work order result to FAIL")

                elif passed_constraints == total_constraints:
                    # All passed → Work Order = PASS
                    work_order_passed_state = StatisticServiceState.PASS
                    self.logger.info(f"Work Order {work_order.id}: All {total_constraints} constraints passed, setting work order result to PASS")
                else:
                    # No results → Work Order = SKIP (safety fallback)
                    work_order_passed_state = StatisticServiceState.SKIP
                    self.logger.warning(f"Work Order {work_order.id}: No constraint results recorded, defaulting to SKIP")
            else:
                # No constraints defined
                self.logger.warning(f"Work Order {work_order.id}: No constraints defined, defaulting to SKIP")
                work_order_passed_state = StatisticServiceState.SKIP
            
            # write to rptds fceolqt_wo_result_fact table with final work order result
            if work_order_passed_state != StatisticServiceState.SKIP:
                self._wo_result_fact_repository.store_wo_result_falconDB(
                    work_order.id, work_order_passed_state
                )
            self.logger.info(f"Final work order result: {work_order_passed_state} based on constraint results - Total: {total_constraints}, Passed: {passed_constraints}, Failed: {failed_constraints}, Skipped: {skipped_constraints}")

        
        # self.logger.info(f"test_work_order(): Test Result - {work_order_passed_state}")
        work_order.constraints_to_test.work_order_status = work_order_passed_state
        work_order.log_workorder_result(work_order_passed_state, f"Total={total_constraints}, Passed={passed_constraints}, Failed={failed_constraints}, Skipped={skipped_constraints}")  # Also set result_fg field
        if work_order_passed_state != StatisticServiceState.SKIP:
            self._wo_result_fact_repository.store_wo_result_falconDB(
                 work_order.id, work_order_passed_state
            )



        # Print constraints_to_test data structure and data before returning
        self.logger.info(f"=== CONSTRAINTS_TO_TEST DATA FOR {work_order.id} ===")
        #self.logger.info(f"Type: {type(work_order.constraints_to_test)}")
        #self.logger.info(f"Dir: {dir(work_order.constraints_to_test)}")
        #self.logger.info(f"Data: {work_order.constraints_to_test.__dict__ if hasattr(work_order.constraints_to_test, '__dict__') else 'No __dict__ attribute'}")

        if trigger_type=="statistic_test":
            self.logger.info(f"""test_work_order(): work_order_id: {work_order_id} - Test statistic flag is True, returning detailed results""")
            ret = json.dumps(
                    {
                        "work_order_id": str(work_order.id) if work_order.id is not None else None,
                        "ink_type_dim_ky": str(work_order.ink_type_dim_ky) if work_order.ink_type_dim_ky is not None else None,
                        "arch_id": str(work_order.arch_id) if work_order.arch_id is not None else None,
                        "constraints_percentile_results": work_order.constraints_to_test.constraints_results ,
                        "delta_e_results": work_order.constraints_to_test.delta_e_results if hasattr(work_order.constraints_to_test, 'delta_e_results') else None,
                    },
                    indent=2,
                    default=str
                )
            #self.logger.info(f"""test_work_order(): work_order_id: {work_order_id} - Detailed results: {ret}""")
            

        return work_order #work_order_passed_state

    def test_constraint(self, work_order_id, constraint, parametric_data):
        self.logger.info(
            f"""test_constraint(): work_order_id: {work_order_id} 
            constraint: {constraint.constraint_key} - {constraint.column_name} - 
            using strategy: {self._percentile_strategy.name}"""
        )
        data = np.array(parametric_data)
        result = self._percentile_strategy.test(data, constraint)
        if result.get("state") == StatisticServiceState.FAIL:
            self.logger.info(
                f"""test_constraint(): work_order_id: {work_order_id} 
                constraint: {constraint.constraint_key} - {constraint.column_name} - FAIL"""
            )
        elif result.get("state") == StatisticServiceState.PASS:
            self.logger.info(
                f"""test_constraint(): work_order_id: {work_order_id}
                constraint: {constraint.constraint_key} - {constraint.column_name} - PASS"""
            )
        return result

    def test_delta_e(self, work_order_id, slot_type_cd, pen_ids=None):
        self.logger.info(f"test_delta_e(): work_order_id: {work_order_id}")

        prod_colors = [
            int(row[0])
            for row in self._pen_slot_fact_repository.get_prod_colors(
                work_order_id, slot_type_cd, pen_ids
            )
            if row and row[0] is not None
        ]

        results = []
        if not prod_colors:
            self.logger.info(f"DELTA_E - No prod colors found for work order {work_order_id} and slot type {slot_type_cd}, skipping delta E calculation")

        else:
            for prod_color in prod_colors:
                # Get historical coordinates (median a, b, L) for this prod_color
                historical_coords = self._pen_slot_fact_repository.get_delta_e_coordinates_for_slot_type_cd(
                    work_order_id,  slot_type_cd, prod_color, pen_ids
                )
                
                if len(historical_coords) == 0:
                    self.logger.info(
                        f"DELTA_E - No historical data from previous WO of current {work_order_id} type were available to calculate the median for prod_color {prod_color}"
                    )
                    continue

                # Form coordinates_hue2 from the returned median values (assuming 3 columns: median_a, median_b, median_l)
                coordinates_hue2 = {
                    "a": float(historical_coords[0][0]),  # median a
                    "b": float(historical_coords[0][1]),  # median b  
                    "L": float(historical_coords[0][2]),  # median L
                }
                
                # Check for None values in historical coordinates
                if coordinates_hue2["a"] is None or coordinates_hue2["b"] is None or coordinates_hue2["L"] is None:
                    self.logger.info(
                        f"DELTA_E - No valid historical median data available for prod_color {prod_color}: a={coordinates_hue2['a']}, b={coordinates_hue2['b']}, L={coordinates_hue2['L']}"
                    )
                    continue
                    
                self.logger.info(
                        f"DELTA_E References- A {coordinates_hue2['a']} B {coordinates_hue2['b']} L {coordinates_hue2['L']} for slot {slot_type_cd} prod_color {prod_color}"
                    )
                # Get pens for this work order and slot type
                pens_row = self._pen_slot_fact_repository.get_pen_slot_facts(
                    work_order_id, slot_type_cd, prod_color, pen_ids
                )
                
                for pen in pens_row:
                    pen_prod_color_dim_ky = int(pen[5]) if pen[5] is not None else None  # Get pen's prod_color_dim_ky
                    
                    # Only process pens that match the current prod_color
                    if pen_prod_color_dim_ky != prod_color:
                        continue

                    pen_hue2 = {
                        "a": float(pen[111]) if pen[111] is not None else None,
                        "b": float(pen[115]) if pen[115] is not None else None,
                        "L": float(pen[119]) if pen[119] is not None else None,
                    }
                    
                    # Check for None values in pen LAB coordinates
                    if pen_hue2["a"] is None or pen_hue2["b"] is None or pen_hue2["L"] is None:
                        self.logger.info(
                            f"DELTA_E - Skipping pen {pen[0]} due to null LAB values: a={pen_hue2['a']}, b={pen_hue2['b']}, L={pen_hue2['L']}"
                        )
                        continue
                    
                    # Calculate delta_e between current pen and historical median
                    # Convert all values to float to handle Decimal/float type mismatches
                    a_current = float(pen_hue2["a"])
                    b_current = float(pen_hue2["b"])
                    l_current = float(pen_hue2["L"])
                    a_star_mean = float(coordinates_hue2["a"])
                    b_star_mean = float(coordinates_hue2["b"])
                    l_star_mean = float(coordinates_hue2["L"])
                    
                    delta_e = math.sqrt(
                        (a_current - a_star_mean) ** 2 +
                        (b_current - b_star_mean) ** 2 +
                        (l_current - l_star_mean) ** 2
                    )

                    if self._pen_slot_fact_repository:
                        self._pen_slot_fact_repository.update_delta_e(
                            str(pen[0]) if pen[0] is not None else None, 
                            int(pen[1]) if pen[1] is not None else None, 
                            int(pen[2]) if pen[2] is not None else None, 
                            delta_e
                        )

                    results.append(
                        {
                            "slot_type_cd": slot_type_cd,
                            "delta_e": delta_e,
                            "pen": {
                                "pn_id": str(pen[0]) if pen[0] is not None else None,
                                "delta_e_vl_2": float(pen[124]) if pen[124] is not None else None,
                                "pen_hue2": pen_hue2
                            }
                        }
                    )

        return results

    @property
    def percentile_strategy(self):
        return self._percentile_strategy

    @percentile_strategy.setter
    def percentile_strategy(self, strategy):
        self._percentile_strategy = strategy
