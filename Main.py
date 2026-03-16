import json
import logging
import traceback
from decimal import Decimal


from LogHandler import LogHandler
from constants.LogStatusMessage import LogStatusMessage
from constants.StatisticServiceState import StatisticServiceState
from exceptions.WorkOrderException import WorkOrderException
from models import ConstraintsToTest
from repositories.PenSlotFactRepository import PenSlotFactRepository
from repositories.TestTypeRepository import TestTypeRepository
from repositories.WoResultFactRepository import WoResultFactRepository
from repositories.WoTestFactRepository import WoTestFactRepository
from repositories.WorkOrderRepository import WorkOrderRepository
from services.EmailService import EmailService
from services.ColorLookupDDBService import ColorLookupDDBService
from services.StatisticService import StatisticService
from services.DataPersistAWSService import DataPersistAWSService
from statistic_strategy.DatabasePercentileStrategy import DatabasePercentileStrategy 
from statistic_strategy.NumpyPercentileStrategy import NumpyPercentileStrategy


class Main:
    logger = logging.getLogger(__name__)
    
  
   
    def __init__(self, config, db_provider):
        self._config = config
  
        # -- START INIT ALL REQUIRED CLASSES -- #work_order_repository
        self._email_service = EmailService(config.email_config, config.falcap_web_url)
        self._test_type_repository = TestTypeRepository(db_provider)
        self._wo_result_fact_repository = WoResultFactRepository(db_provider)
        self._wo_test_fact_repository = WoTestFactRepository(db_provider)
        self._pen_slot_fact_repository = PenSlotFactRepository(db_provider)
        self._percentile_strategy = NumpyPercentileStrategy() #default to in-memory numpy percentile strategy, can be switched to database percentile strategy to use the database stored function for percentile calculation if needed
        self._persist_work_order_to_aws = DataPersistAWSService(
            self._config.dynamo_db_config, config.results_config, "cron"
        )
        self._inventory_color_lookup_service = ColorLookupDDBService(self._config.dynamo_db_config)
        self._work_order_repository = WorkOrderRepository(
            db_provider,
            self._test_type_repository,
            None,
            self._persist_work_order_to_aws,
            self._inventory_color_lookup_service,
            self._wo_result_fact_repository,
            config.filters,
            config.filters_hue2_test_pen_count,
        )
        self._statistic_service = StatisticService(
            db_provider,
            self._work_order_repository,
            self._wo_test_fact_repository,
            self._wo_result_fact_repository,
            self._pen_slot_fact_repository,
            self._percentile_strategy,
            config.db_config.should_update
        )
        # Set the statistic_service in work_order_repository after both are created
        self._work_order_repository.statistic_service = self._statistic_service
        # -- END INIT ALL REQUIRED CLASSES -- #

    def _convert_to_serializable(self, obj, visited=None):
        """Convert objects to JSON serializable format"""
        from decimal import Decimal
        from datetime import datetime, date
        
        if visited is None:
            visited = set()
        
        # Check for circular references
        obj_id = id(obj)
        if obj_id in visited:
            return f"<Circular reference to {type(obj).__name__}>"
        
        if obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            visited.add(obj_id)
            try:
                result = {k: self._convert_to_serializable(v, visited) for k, v in obj.items()}
            finally:
                visited.discard(obj_id)
            return result
        elif isinstance(obj, (list, tuple)):
            visited.add(obj_id)
            try:
                result = [self._convert_to_serializable(item, visited) for item in obj]
            finally:
                visited.discard(obj_id)
            return result
        elif hasattr(obj, '__dict__'):
            # Handle custom objects like WorkOrder by converting to dict
            visited.add(obj_id)
            try:
                result = {}
                for key, value in obj.__dict__.items():
                    # Remove private attribute prefix for cleaner output
                    clean_key = key.lstrip('_')
                    result[clean_key] = self._convert_to_serializable(value, visited)
                return result
            finally:
                visited.discard(obj_id)
        else:
            # Fallback to string representation
            return str(obj)

    def run(self, event):
        if "event" in event and event["event"] in [
            "daily-run",
            "web-ui",
            "web-ui-force-test",
        ]:
            if event["event"] == "web-ui":
                return self._run_web_ui(event["payload"])
            elif event["event"] == "web-ui-force-test":
                return self._run_web_ui_force_test(event["payload"])
            elif event["event"] == "daily-run":
                return self._run_daily_run()

        raise Exception(
            "event key is required, and must be daily-run, web-ui, or web-ui-force-test"
        )

    def _run_web_ui(self, payload):
        try:
            # get the work_order_id from the body
            work_order_id = payload.get("workOrderId", None)
            if work_order_id is None:
                self.logger.error("workOrderId is not provided, not proceeding.")
                return {
                    "statusCode": 400,
                    "body": json.dumps(
                        {
                            "error": "workOrderId is required",
                        }
                    ),
                }
            ret = self._run_falcap_process(work_order_id=work_order_id, trigger_type="statistic_test")

            # Parse the JSON result from the statistic service
            if ret:

                try:
                    # ret is a dictionary, convert Decimal values for JSON serialization
                    return {
                            "statusCode": 200,
                            "body": json.dumps(
                                {
                                    "work_order_id": str(ret["work_order_id"]) if ret["work_order_id"] is not None else "",
                                    "arch_id": str(ret["arch_id"]) if ret["arch_id"] is not None else "",
                                    "ink_type_dim_ky": str(ret["ink_type_dim_ky"]) if ret["ink_type_dim_ky"] is not None else "",
                                    "constraints_percentile_results": ret["constraints_percentile_results"],
                                    "delta_e_results": ret["delta_e_results"],
                                }
                            ),
                    }
                except (json.JSONDecodeError, TypeError):
                    # Check if ret is a dict-like object with SKIP status
                    if hasattr(ret, 'get') and (ret.get('wo_st') == 'SKIP' or ret.get('wo_st') == 'BLOCK') and ret.get('test_st') == []:
                        # Format SKIP response according to API specification
                        skip_response = {
                            "work_order_id": work_order_id,
                            "ink_type_dim_ky": "",
                            "arch_id": "",
                            "constraints_percentile_results": [],
                            "delta_e_results": []
                        }
                        response_body = json.dumps(skip_response)
                        self.logger.info(f"200 Response Body (SKIP): {response_body}")
                        return {
                            "statusCode": 200,
                            "body": response_body
                        }
                    
                    # If ret is not valid JSON and not SKIP, return error
                    self.logger.error(f"Invalid JSON result from falcap process: {ret}")
                    return {
                        "statusCode": 500,
                        "body": json.dumps({
                            "error": "Invalid result format from test process"
                        })
                    }
            else:
                return {
                    "statusCode": 404,
                    "body": json.dumps({
                        "error": "No result returned from test process"
                    })
                }
        except Exception as e:
            self.logger.error(e)
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "error": "Delta - e Error: " + str(e),
                    }
                ),
            }

    def _run_web_ui_force_test(self, payload):
        self.logger.info("START FORCE TEST.")
        try:
        
            # get the work_order_id from the body
            workorder_id = payload.get("workOrderId", None)
            if workorder_id is None:
                self.logger.error("workOrderId is not provided, not proceeding.")
                return {
                    "statusCode": 400,
                    "body": json.dumps(
                        {
                            "error": "workOrderId is required",
                        }
                    ),
                }
            ret=self._run_falcap_process(work_order_id=workorder_id, trigger_type="force_test")

            # Debug: Check the type and content of ret
            self.logger.info(f"Type of ret: {type(ret)}")
            self.logger.info(f"Content of ret: {ret}")
            
            # Check if work order was found and tested
            if not ret:
                error_msg = f"Work order with ID '{workorder_id}' not found"
                self.logger.error(error_msg)
                return {
                    "statusCode": 404,
                    "body": json.dumps(
                        {
                            "error": error_msg,
                        }
                    ),
                }
       
            self.logger.info(f"Work Order ID: {ret.get('work_order_id')}")
            
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "work_order_id":  ret["work_order_id"],
                        "result":  ret["status"]["result_fg"]
                    }
                ),
            }
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "error": "Force test Error: " + str(e),
                    }
                ),
            }

    def _run_daily_run(self):
           
            self.logger.info("START DAILY RUN.")
            ret=self._run_falcap_process(trigger_type="cron")
            if ret is not None:
                self.logger.info("END DAILY RUN PROCESS.")
           
    def _run_falcap_process(self,work_order_id=None, trigger_type=""):
        try:
            self.logger.info("START FALCAP PROCESS.")
            work_order_models = self._work_order_repository.get_workorder(work_order_id=work_order_id)  # This now returns WorkOrder model objects
            #work_order_models = self._work_order_repository.get_workorder() 
            # Check if work orders list is empty
      
            if not work_order_models or len(work_order_models) == 0:
                self.logger.info(f"No Work order to process")
                return None
                
            #filter by calls
                  # days-to-process gate
                # if wo_close_dm and work_order_model.test_type:
                #     wo_days_elapsed = (date.today() - datetime.fromisoformat(wo_close_dm).date()).days
                #     if wo_days_elapsed > work_order_model.test_type.days_to_process_wo_ct:
                #         work_order_model.log_workorder_status(
                #             LogStatusMessage.block(),
                #             f"Exceeded days_to_process requirement: {work_order_model.test_type.days_to_process_wo_ct} days."
                #         )
                #         work_order_models.append(work_order_model)
                #         continue

            self.logger.info(f"Retrieved {len(work_order_models)} WorkOrder model objects")
            passed_work_orders = []
            failed_blocked_work_orders = []
            skipped_work_orders = []
            all_work_orders=[]
            passed_failed_blocked_work_orders = []
            result = None  # Initialize result variable

            for work_order in work_order_models:
                # Add all Work order into db ++
                #self._persist_work_order_to_aws.add_work_order(work_order=work_order)
                # Check work order status before calling test_work_order
                current_status = work_order.status_result.get("wo_st", "")
                current_description = work_order.status_result.get("wo_desc", "")
                wo_close_dm = work_order.wo_close_dm or ""
                self.logger.info(f"Work Order {work_order.id} , Close DM: {wo_close_dm}")
                
                if current_status == "SKIP" or current_status == "BLOCK":
                    self.logger.info(f"Work Order {work_order.id} has {current_status} status: {current_description}")
                    result=current_status
                else:
                    self.logger.info(f"Work Order {work_order.id} has {current_status} status: {current_description}")
                    # run test and update db with result
                    # result for web_ui is big json string with detailed test result, result for cron is PASS, FAIL, BLOCK or SKIP  
                    result = self._statistic_service.test_work_order(work_order, trigger_type=trigger_type)
                    self.logger.info(f"Work Order: {work_order.id} - result: {result}.")
                    
                    # If test_work_order returns workorder model, read constraint test results
                    if hasattr(result, 'constraints_to_test') and result.constraints_to_test is not None:
                        self.logger.info(f"Reading constraint test results for Work Order: {work_order.id}")
                        
                        # Read constraint test results
                        constraint_results = result.constraints_to_test.constraints_results
                        if constraint_results:
                            #self.logger.info(f"Work Order {work_order.id} - Constraint test results: {constraint_results}")
                            for constraint_key, constraint_result in constraint_results.items():
                                self.logger.info(f"Work Order {work_order.id} - Constraint {constraint_key}")
                                #self.logger.info(f"Work Order {work_order.id} - Constraint {constraint_key}: {constraint_result}")
                        else:
                            self.logger.info(f"Work Order {work_order.id} - No constraint test results found")
                        
                        # Read constraint errors if any
                        constraint_errors = result.constraints_to_test.constraints_errors
                        if constraint_errors:
                            self.logger.info(f"Work Order {work_order.id} - Constraint test errors: {constraint_errors}")
                        
                        # Read test status from status_result
                        test_statuses = result.status_result.get("result_fg", [])
                        if test_statuses:
                            self.logger.info(f"Work Order {work_order.id} - Test statuses: {test_statuses}")
                        
                        # Read result summary
                        result_summary = result.status_result.get("result_summary", "")
                        if result_summary:
                            self.logger.info(f"Work Order {work_order.id} - Result summary: {result_summary}")
                    else:
                        self.logger.info(f"Work Order {work_order.id} - Result is not a WorkOrder model or has no constraints_to_test")
                
                #only send email for cron and force_test trigger
                self.logger.info(f"Work Order: {work_order.id} - Trigger type: {trigger_type}, Test result: {test_statuses}")
                if trigger_type in ["cron", "force_test"]:  
                    if test_statuses == StatisticServiceState.PASS:
                        self.logger.info(f"Work Order: {work_order.id} - passed.")
                        passed_work_orders.append(work_order)
                    elif test_statuses == StatisticServiceState.FAIL or test_statuses == StatisticServiceState.SKIP:
                        self.logger.info(f"Work Order: {work_order.id} - failed.")
                        # get the reason why the work order failed
                        reasons = work_order.status_result.get("result_summary", "insufficient data parameter")
                        self.logger.info(f"Work Order: {work_order.id} - failure reasons: {reasons}")
                        # send the failed email
                        self._email_service.send_failed_email(work_order, reasons)
                        work_order.log_email_sent()
                        # append to failed blocked work orders so that we can update the database later in a bulk
                        failed_blocked_work_orders.append(work_order)
                    elif test_statuses == StatisticServiceState.BLOCKED:
                        self.logger.info(f"Work Order: {work_order.id} - blocked.")
                        # send the rejected email
                        self._email_service.send_rejected_email(work_order)
                        work_order.log_email_sent()
                        # append to failed blocked work orders so that we can update the database later in a bulk
                        failed_blocked_work_orders.append(work_order)
                    # elif result == StatisticServiceState.SKIP:
                    #     self.logger.info(f"Work Order: {work_order.id} - skipped.")
                    #     skipped_work_orders.append(work_order)

                             
                    if trigger_type == "cron":
                        self._persist_work_order_to_aws.modified_nm = DataPersistAWSService.CRON
                    elif trigger_type == "force_test":
                        self._persist_work_order_to_aws.modified_nm = DataPersistAWSService.WEB
                            
                    self._work_order_repository.email_service = self._email_service
                    # update the work orders status to dynamodb and s3
                    all_work_orders = passed_work_orders + failed_blocked_work_orders + skipped_work_orders

            #Save all work order status to AWS in a bulk update
            self.logger.info(f"Updating {len(all_work_orders)} work orders to AWS.")
            for wo in work_order_models:
                self._persist_work_order_to_aws.add_work_order(work_order=wo)
                self._persist_work_order_to_aws.update()

            # # get other work orders that have not been sent an email and not in recently tested work orders
            # tested_work_orders_ids = [
            #     tested_work_order.id
            #     for tested_work_order in passed_work_orders + failed_blocked_work_orders
            # ]

            # work_orders_with_no_email_sent = (
            #     self._wo_result_fact_repository.get_work_orders_with_no_email_sent(
            #         tested_work_orders_ids
            #     )
            # )

            # self.logger.info(
            #     f"FalCAPAutomationCron(): found {len(work_orders_with_no_email_sent)} work orders with no email sent."
            # )

            # # loop through the work orders with no email sent
            # for work_order, status in work_orders_with_no_email_sent:
            #     work_order.log_status(LogStatusMessage.no_email_sent())
            #     work_order.log_result(status)
            #     # if the status is pass, then add it to pass email list so that we can include them in the email
            #     if status == StatisticServiceState.PASS:
            #         self.logger.info(f"Work Order: {work_order.id} - passed.")
            #         passed_work_orders.append(work_order)
            #     elif status == StatisticServiceState.FAIL:
            #         self.logger.info(f"Work Order: {work_order.id} - failed.")
            #         reasons = self._work_order_repository.get_failure_reasons(
            #             work_order.id
            #         )
            #         self._email_service.send_failed_email(work_order, reasons)
            #         work_order.log_email_sent()
            #         failed_blocked_work_orders.append(work_order)
            #     elif status == StatisticServiceState.BLOCKED:
            #         self.logger.info(f"Work Order: {work_order.id} - blocked.")
            #         self._email_service.send_rejected_email(work_order)
            #         work_order.log_email_sent()
            #         failed_blocked_work_orders.append(work_order)

            #     if len(passed_work_orders) != 0:
            #         self.logger.info("Sending passed work orders email.")
            #         self._email_service.send_passed_email(passed_work_orders)
            #         for work_order in passed_work_orders:
            #             work_order.log_email_sent()
            #     else:
            #         self.logger.info("No passed work orders to send email.")

            #     # concat the passed, failed and blocked work orders so that we can update the database in a bulk
            #     passed_failed_blocked_work_orders = (
            #         passed_work_orders + failed_blocked_work_orders
            #     )
            #     if len(passed_failed_blocked_work_orders) != 0:
            #         self.logger.info("Updating work orders email sent.")
            #         self._wo_result_fact_repository.set_email_sent(
            #             passed_failed_blocked_work_orders
            #         )
            #     else:
            #         self.logger.info("No work orders to update email sent.")
                    
 
            # If trigger_type is statistic_test, return the detailed JSON result
            if trigger_type=="statistic_test" and result:
                # Check if result is a work order object with constraints_to_test
                if hasattr(result, 'constraints_to_test') and result.constraints_to_test is not None:
                    return {
                            "work_order_id": work_order.id,
                            "arch_id": work_order.arch_id,
                            "ink_type_dim_ky": work_order.ink_type_dim_ky,
                            "constraints_percentile_results": result.constraints_to_test.constraints_results,
                            "delta_e_results": result.constraints_to_test.calculated_delta_e,
                        }
                else:
                    # If result is just a status string, return basic info
                    return {
                            "work_order_id": work_order.id,
                            "arch_id": work_order.arch_id,
                            "ink_type_dim_ky": work_order.ink_type_dim_ky,
                            "test_result": result,
                            "constraints_percentile_results": {},
                            "delta_e_results": {},
                        }
            elif trigger_type=="force_test":
                # For force test, return the test result and work order info
                if work_order_models and len(work_order_models) > 0:
                    work_order = work_order_models[0]  # Should be only one for force test
                    return {
                        "work_order_id": work_order.id,
                        "test_result": result if result is not None else "NO_RESULT",
                        "status": work_order.status_result
                    }
                else:
                    return None
            else:        
                # For other trigger types, return status_result if available
                if work_order_models and len(work_order_models) > 0:
                    return "COMPLETED"
                else:
                    return None
        
            
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            if self._email_service:
                self._email_service.send_error_email(LogHandler.get_log_buffer())


if __name__ == "__main__":
    Main().run()
