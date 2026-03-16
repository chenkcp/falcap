import json
import logging
from datetime import datetime
from decimal import Decimal
import boto3

from constants.LogStatusMessage import LogStatusMessage
from constants.StatisticServiceState import StatisticServiceState
from services.S3Service import S3Service


class DataPersistAWSService:
    logger = logging.getLogger(__name__)
    CRON = "cron"
    WEB = "web"

    def __init__(self, dynamo_db_config, results_config, modified_nm):
        # Debug AWS account and region information
        import boto3
        sts_client = boto3.client('sts')
        try:
            identity = sts_client.get_caller_identity()
            self.logger.info(f"AWS Account ID: {identity.get('Account')}")
            self.logger.info(f"AWS User/Role ARN: {identity.get('Arn')}")
            self.logger.info(f"AWS User ID: {identity.get('UserId')}")
        except Exception as e:
            self.logger.error(f"Failed to get AWS identity: {e}")
        
        self.logger.info(f"DynamoDB Region: {dynamo_db_config.region}")
        self.logger.info(f"DynamoDB Table: {dynamo_db_config.work_order_status_table_name}")
        
        client = boto3.resource("dynamodb", region_name=dynamo_db_config.region)
        self._table = client.Table(dynamo_db_config.work_order_status_table_name)
        self._dynamodb_config = dynamo_db_config
        self._results_config = results_config
        self._modified_nm = modified_nm
        self._work_orders = []
        
        # Debug S3 configuration
        if results_config and hasattr(results_config, 'save_result'):
            self.logger.info(f"S3 Save Results: {results_config.save_result}")
            if results_config.save_result:
                self.logger.info(f"S3 Region: {results_config.region}")
                self.logger.info(f"S3 Bucket: {results_config.bucket}")
                self.logger.info(f"S3 Path: {results_config.path}")

    def add_work_order(self, work_order=None, work_orders=None):
        if work_order:
            self._work_orders.append(work_order)

        if work_orders:
            self._work_orders.extend(work_orders)

    def update(self):
        self.logger.info("Updating work orders status to DDB and S3.")
        s3_work_orders_status_result = []

        for work_order in self._work_orders:
            status_result = work_order.status_result
            work_order_status = status_result["wo_st"]
            work_order_description = status_result["wo_desc"]
            work_order_test_status = status_result["test_st"]
            work_order_result_status = status_result["result_fg"]
            work_order_email_date = status_result["email_dt"]
            # this is to build a string of all work orders status
            s3_current_work_order_test_status = []
            work_order_initial_status = f"{work_order.id}: {work_order_status}"
            if work_order_description:
                work_order_initial_status += f" - {work_order_description}"
            s3_current_work_order_test_status.append(work_order_initial_status)

            ddb_current_work_order_test_status = {}

            if work_order.constraints_to_test and work_order.test_type:
                constraints_to_test = work_order.constraints_to_test
                ddb_current_work_order_test_status = {
                    "test_type_name": work_order.test_type.name,
                    "inv_item_dim_ky": work_order.inv_item_dim_ky,
                    "should_process_work_order": constraints_to_test.should_process_work_order,
                    "calculated_delta_e": constraints_to_test.calculated_delta_e,
                    "work_order_status": constraints_to_test.work_order_status,
                    "errors": constraints_to_test.errors,
                    "constraints_status": [],
                    "constraints_skip": [],
                }

                for constraint in work_order.test_type.constraints.values():
                    if (
                        constraint.constraint_key
                        not in constraints_to_test.constraint_keys
                    ):
                        ddb_current_work_order_test_status["constraints_skip"].append(
                            constraint.get_dict()
                        )

                for constraint_key in constraints_to_test.constraint_keys:
                    # Find the constraint with the matching base key (e.g., find "1203_color_123" for constraint_key "1203")
                    matching_constraint = None
                    for test_constraint_key, constraint in work_order.test_type.constraints.items():
                        if test_constraint_key.startswith(f"{constraint_key}_color_"):
                            matching_constraint = constraint
                            break
                    
                    # Fallback: try exact match for legacy constraint keys
                    if not matching_constraint:
                        matching_constraint = work_order.test_type.constraints.get(constraint_key)
                    
                    if matching_constraint:
                        constraint_status = {
                            "constraint": matching_constraint.get_dict(),
                            "result": constraints_to_test.constraints_results.get(
                                constraint_key, []
                            ),
                            "errors": constraints_to_test.constraints_errors.get(
                                constraint_key, []
                            ),
                        }

                        ddb_current_work_order_test_status["constraints_status"].append(
                            constraint_status
                        )

            for test_status in work_order_test_status:
                s3_current_work_order_test_status.append(test_status)

            if (
                work_order_result_status
                and work_order_result_status != StatisticServiceState.NA
            ):
                s3_current_work_order_test_status.append(
                    LogStatusMessage.test_result(work_order_result_status)
                )

            if work_order_email_date != "":
                s3_current_work_order_test_status.append(LogStatusMessage.email_sent())

            s3_work_orders_status_result.append(
                " | ".join(s3_current_work_order_test_status)
            )

            if work_order_result_status == StatisticServiceState.SKIP:
                work_order_result_status = StatisticServiceState.NA
                
            # print(type(ddb_current_work_order_test_status)) # check if this is a dict, 2025-04-25-Paul
            # for key, value in ddb_current_work_order_test_status.items():
            #     print(f"{key}: {type(value)}")
            # Convert Decimal to float for JSON serialization -2025-04-25, Paul
            json_string = json.dumps(
                ddb_current_work_order_test_status,
                default=lambda o: float(o) if isinstance(o, Decimal) else str(o)
            )

            # changed 2025-04-25-Paul
            self.update_dynamodb_status(
                work_order.id,
                work_order_status,
                work_order_description,
                json_string,
                work_order_result_status,
                work_order_email_date,
            )
        #     self.update_dynamodb_status(
        #         work_order.id,
        #         work_order_status,
        #         work_order_description,
        #         json_string json.dumps(ddb_current_work_order_test_status),
        #         work_order_result_status,
        #         work_order_email_date,
        #     )
            self.update_s3_status("\n".join(s3_work_orders_status_result))

    def update_dynamodb_status(
        self, work_order_id, wo_st, wo_desc, test_st, result_fg, email_dt
    ):
        self.logger.info(f"DynamoDB Update - WorkOrder: {work_order_id}, Status: {wo_st}, Result: {result_fg}")
        if not self._dynamodb_config.should_update_work_order_status:
            self.logger.info("DynamoDB updates are disabled in configuration")
            return
        # check if work order id already exist
        response = self._table.get_item(Key={"work_order_id": work_order_id})
        if "Item" in response:
            update_expression = "set modified_nm = :modified_nm, modified_ts = :modified_ts, wo_desc = :wo_desc,"
            expression_attributes_value = {
                ":modified_nm": self._modified_nm,
                ":modified_ts": datetime.utcnow().isoformat(),
                ":wo_desc": wo_desc,
            }
            if wo_st:
                update_expression += " wo_st = :wo_st,"
                expression_attributes_value[":wo_st"] = wo_st
            if test_st:
                update_expression += " test_st = :test_st,"
                expression_attributes_value[":test_st"] = test_st
            if result_fg:
                update_expression += " result_fg = :result_fg,"
                expression_attributes_value[":result_fg"] = result_fg
            if email_dt:
                update_expression += " email_dt = :email_dt,"
                expression_attributes_value[":email_dt"] = email_dt

            self._table.update_item(
                Key={"work_order_id": work_order_id},
                UpdateExpression=update_expression[:-1],
                ExpressionAttributeValues=expression_attributes_value,
            )

        else:
            self._table.put_item(
                Item={
                    "work_order_id": work_order_id,
                    "wo_st": wo_st,
                    "wo_desc": wo_desc,
                    "test_st": test_st,
                    "result_fg": result_fg,
                    "email_dt": email_dt,
                    "modified_nm": self._modified_nm,
                    "insert_ts": datetime.utcnow().isoformat(),
                    "modified_ts": datetime.utcnow().isoformat(),
                }
            )

    def update_s3_status(self, all_work_orders_status_result):
        self.logger.info(f"S3 Update Check - Save Result: {self._results_config.save_result if self._results_config else 'No config'}, Modified By: {self._modified_nm}")
        # if not save result or not cron, do not save to s3
        if not self._results_config.save_result or not self._modified_nm == self.CRON:
            self.logger.info("S3 update skipped - either save_result is False or not triggered by CRON")
            return

        if all_work_orders_status_result == "":
            self.logger.info("No results.")
            all_work_orders_status_result = "No results."

        self.logger.info("Saving results to S3.")
        self.logger.info(f"S3 Target - Region: {self._results_config.region}, Bucket: {self._results_config.bucket}, Path: {self._results_config.path}")
        self.upload_result(
            self._results_config.region,
            self._results_config.bucket,
            self._results_config.path,
            all_work_orders_status_result,
        )

    @property
    def modified_nm(self):
        return self._modified_nm

    @modified_nm.setter
    def modified_nm(self, value):
        self._modified_nm = value

    @classmethod
    def upload_result(cls, region, bucket, to, result_text):
        s3 = boto3.client("s3", region_name=region)
        # get today date
        file_name = f"results-{datetime.today().strftime('%Y-%m-%d')}.log"
        key = f"{to}/{file_name}"

        cls.logger.info(f"Uploading result file to bucket: {bucket}, key: {key}")
        s3.put_object(Body=result_text, Bucket=bucket, Key=key)
        cls.logger.info(f"Uploaded result file to bucket: {bucket}, key: {key}")
        s3.close()