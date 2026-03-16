import json
import logging
import os, sys

from dotenv import load_dotenv

from LogHandler import LogHandler
from Main import Main
from configs.Config import Config
from db.OracleProvider import OracleProvider
from db.PostgresProvider import PostgresProvider
from services.S3Service import S3Service

db_provider = None

def lambda_handler(event, context):
    #print("CWD:", os.getcwd())
    #print("sys.path:", sys.path)
    # to check the event is in the event body, can be daily-run, web-ui, or web-ui-force-test
    # if not check the path, if the path is /test-statistics, then it is web-ui
    if "event" not in event:
        raise Exception("event must be daily-run, web-ui, or web-ui-force-test")

    if "event" in event and event["event"] not in [
        "daily-run",
        "web-ui",
        "web-ui-force-test",
    ]:
        raise Exception("event must be daily-run, web-ui, or web-ui-force-test")

    load_dotenv()
    log_handler = LogHandler()
    # remove lambda default logging handler
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    log_format = "[%(levelname)s][%(asctime)s][%(name)s] - %(message)s"
    if os.environ.get("AWS_EXECUTION_ENV") is not None:
        log_format = f"[RequestId: {context.aws_request_id}]{log_format}"

    logging.basicConfig(
        format=log_format,
        #level=logging.INFO,
        level=logging.DEBUG,
        handlers=[log_handler],
    )
    logger = logging.getLogger(__name__)
    try:
        load_config_from = os.getenv("CONFIG_LOAD")
        logger.info("Loading config from %s", load_config_from)
        config_file_path = None
        if load_config_from == "aws":
            bucket = os.getenv("CONFIG_AWS_BUCKET")
            key = os.getenv("CONFIG_AWS_CONFIG_KEY")
            region = os.getenv("CONFIG_AWS_REGION")
            
            logger.info(f"Attempting to download config from S3: bucket={bucket}, key={key}, region={region}")
            try:
                config_file_path = S3Service.download_config(
                    region, bucket, key, os.getenv("CONFIG_DOWNLOAD_FOLDER", "/tmp")
                )
            except Exception as s3_error:
                logger.error(f"Failed to download config from S3: {s3_error}")
                logger.error(f"Check Lambda execution role has s3:GetObject permission for arn:aws:s3:::{bucket}/{key}")
                
                # Try fallback to local config if available
                fallback_config = os.getenv("CONFIG_FILE_PATH", "tmp/config.json")
                if os.path.exists(fallback_config):
                    logger.warning(f"Using fallback config file: {fallback_config}")
                    config_file_path = fallback_config
                else:
                    logger.error("No fallback config available")
                    raise Exception(f"S3 config download failed and no fallback available: {s3_error}")
        else:
            config_file_path = os.getenv("CONFIG_FILE_PATH")

        logger.info("Config file path: %s", config_file_path)
        config = Config(config_file_path)

        # # if it is web-ui, it should remove all filters and turn off update and insert
        if "event" in event and event["event"] == "web-ui":
            config.remove_all_filters()
            config.db_config.turn_off_update_and_insert()

        global db_provider
        if db_provider is None or not db_provider.is_connected():
            # Get database engine from secrets manager to determine provider type
            engine = config.db_config.engine.lower() if config.db_config.engine else 'oracle'
            logger.info(f"Database engine from secrets manager: {engine}")
            
            if engine in ['postgresql', 'postgres']:
                logger.info("Using PostgresProvider")
                db_provider = PostgresProvider(
                    user=config.db_config.user,
                    password=config.db_config.password,
                    host=config.db_config.host,
                    port=config.db_config.port,
                    dbname=config.db_config.database,
                    schema=config.db_config.schema,
                    should_update=config.db_config.should_update,
                    should_insert=config.db_config.should_insert,
                )
            elif engine in ['oracle']:
                logger.info("Using OracleProvider")
                db_provider = OracleProvider(
                    config.db_config.user,
                    config.db_config.password,
                    config.db_config.host,
                    config.db_config.port,
                    config.db_config.database,
                    config.db_config.schema,
                    config.db_config.should_update,
                    config.db_config.should_insert,
                )
            else:
                logger.error(f"Unsupported database engine: {engine}")
                raise Exception(f"Unsupported database engine: {engine}. Supported engines: postgresql, oracle")
            
            # Add connection debugging for network issues
            logger.info("Attempting database connection initialization...")
            try:
                db_provider.init_connection()
                logger.info("Database connection initialized successfully")
            except Exception as conn_error:
                logger.error(f"Database connection failed: {conn_error}")
                logger.error("Network troubleshooting info:")
                logger.error(f"  Database host: {config.db_config.host}")
                logger.error(f"  Database port: {config.db_config.port}")
                logger.error(f"  Database engine: {config.db_config.engine}")
                
                # Check if Lambda is in VPC
                if os.getenv("CONFIG_LOAD") == "aws":
                    vpc_id = os.environ.get('AWS_LAMBDA_FUNCTION_VPC_ID')
                    if vpc_id:
                        logger.error(f"  Lambda VPC ID Found: {vpc_id} - Check same VPC as RDS")
                    else:
                        logger.error("  Lambda is NOT in a VPC - Configure Lambda to run in same VPC as RDS")
                
                raise conn_error
        else:
            db_provider.set_database_operations(
                config.db_config.should_update, config.db_config.should_insert
            )

        main = Main(config, db_provider)
        return main.run(event)
    except Exception as e:
        logger.error(e)
        if event["event"] == "web-ui" or event["event"] == "web-ui-force-test":
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "error": "Internal Server Error",
                    }
                ),
            }
