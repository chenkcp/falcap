import json
import logging
import os

import boto3
# Suppress boto3 and botocore logs
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

from exceptions.InvalidConfigException import InvalidConfigException


class DBConfig:
    logger = logging.getLogger(__name__)

    def __init__(
        self, db_schema_config, aws_secrets_manager_config, db_operations_config
    ):
        
        if db_schema_config is None:
            raise InvalidConfigException(
                "DBConfig.__init__(): db_schema_config is None"
            )

        self._schema = db_schema_config["rptds_schema"]
        self._should_update = db_operations_config.get("should_update", False)
        self._should_insert = db_operations_config.get("should_insert", False)
        self._host = None
        self._port = None
        self._user = None
        self._password = None
        self._database = None
        self._engine = None
  
        load_from = db_operations_config.get("load_from", "aws")
       
        if load_from == "local":
            self.load_from_local()
        elif load_from == "aws":
            self.load_from_aws(aws_secrets_manager_config)

    def load_from_local(self):
        self._host = os.getenv("DB_HOST")
        self._port = os.getenv("DB_PORT")
        self._user = os.getenv("DB_USER")
        self._password = os.getenv("DB_PASSWORD")
        self._database = os.getenv("DB_DATABASE")
        self._engine = os.getenv("DB_ENGINE")

    def load_from_aws(self, aws_secrets_manager_config):
    
        client = boto3.client(
            "secretsmanager",
            region_name=aws_secrets_manager_config["region"],
        )
        response = client.get_secret_value(
            SecretId=aws_secrets_manager_config["secret_name"]
        )
        secrets = json.loads(response["SecretString"])
        self._host = secrets["host"]
        self._port = secrets["port"]
        self._engine = secrets["engine"]
        self._database = secrets["dbname"]

        # extract user and password
        secret_password = secrets["password"]
  
        secret_password = secret_password.split("; ")
        self._user = secret_password[1].split(" = ")[1].strip()
        self._password = secret_password[0].split(" = ")[1].strip()
        self._engine = secrets["engine"] #2025-04-25, Paul
        self.logger.info(f"self._database = {secrets['dbname']}")

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def database(self):
        return self._database

    @property
    def schema(self):
        return self._schema

    @property
    def engine(self):
        return self._engine

    @property
    def should_update(self):
        return self._should_update

    @property
    def should_insert(self):
        return self._should_insert

    def turn_off_update_and_insert(self):
        self._should_update = False
        self._should_insert = False
