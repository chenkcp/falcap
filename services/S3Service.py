import logging
import os
from datetime import datetime

import boto3


class S3Service:
    logger = logging.getLogger(__name__)

    @classmethod
    def download_config(cls, region, bucket, key, to_path="tmp"):
        s3 = boto3.client("s3", region_name=region)
        # get file name from key
        key_split = key.split("/")
        filename = key_split[len(key_split) - 1]
        download_path = f"{to_path}/{filename}"
        cls.logger.info(
            f"Downloading config file from bucket:{bucket}, key: {key} to {download_path}"
        )
        # check default save path tmp directory exists
        if not os.path.exists(to_path):
            os.makedirs(to_path)

        # Use get_object instead of download_file to avoid HeadObject permission requirement
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            with open(download_path, 'wb') as f:
                f.write(response['Body'].read())
            cls.logger.info(
                f"Downloaded config file from bucket:{bucket}, key: {key} to {download_path}"
            )
        except Exception as e:
            cls.logger.error(f"Failed to download from S3: {e}")
            raise
        s3.close()
        return download_path


