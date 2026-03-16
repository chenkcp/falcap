import json
import logging
# Suppress boto3 and botocore logs
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

from exceptions.InvalidConfigException import InvalidConfigException
from .DBConfig import DBConfig
from .DynamoDBConfig import DynamoDBConfig
from .EmailConfig import EmailConfig
from .ResultsConfig import ResultsConfig


class Config:
    logger = logging.getLogger(__name__)

    def __init__(self, config_file_path=None):
        if not config_file_path:
            raise InvalidConfigException("Config.__init__(): config_file_path is None")

        if config_file_path:
            try:
                self.logger.info("Loading config from %s", config_file_path)
                with open(config_file_path) as config_file:
                    config = json.load(config_file)
                    # print(json.dumps(config, indent=4)) 
                    self._dynamo_db_config = DynamoDBConfig(
                        config["aws_dynamodb"]["region"],
                        config["aws_dynamodb"]["work_order_status_table_name"],
                        config["aws_dynamodb"]["should_update_work_order_status"],
                        config["aws_dynamodb"]["inventory_color_lookup_table_name"],
                    )
                    app_config = config["falcap_automation_job"]
                    
                    filters = app_config.get("filters", {})
                    self._filters = filters.get("work_orders", [])
                    self._filters_hue2_test_pen_count = filters.get(
                        "hue2_test_pen_count", []
                    )
                    
                    self._falcap_web_url = app_config.get("falcap_web_url", "#")
                    self._email_config = EmailConfig(app_config["email"])
                   
                    self._db_config = DBConfig(
                        config["database_schema"],
                        config["aws_secrets_manager"],
                        app_config.get("database_operations", {}),
                    )
                    
                    self._results_config = ResultsConfig(
                        app_config.get("aws_results", {})
                    )
              
            except Exception as e:
                self.logger.error("Error loading config file: %s", str(e))
                raise e

    @property
    def filters(self):
        return self._filters

    @property
    def falcap_web_url(self):
        return self._falcap_web_url

    @property
    def email_config(self):
        return self._email_config

    @property
    def db_config(self):
        return self._db_config

    @property
    def results_config(self):
        return self._results_config

    @property
    def filters_hue2_test_pen_count(self):
        return self._filters_hue2_test_pen_count

    @property
    def dynamo_db_config(self):
        return self._dynamo_db_config

    def remove_all_filters(self):
        self._filters = []
        self._filters_hue2_test_pen_count = []
