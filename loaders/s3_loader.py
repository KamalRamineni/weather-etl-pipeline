import json
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class S3Loader:
    """
    Archives raw API responses to S3 before transformation.

    Key purposes:
    - Audit trail: know exactly what the API returned for any run
    - Replay: re-run transform/load without hitting the API again
    - Cheap storage: raw JSON in S3 costs almost nothing

    S3 key layout:
        raw/YYYY/MM/DD/batch_YYYYMMDD_HHMMSS.json
    """

    def __init__(self):
        self.bucket = os.getenv('S3_BUCKET_NAME')
        self.region = os.getenv('AWS_REGION', 'us-east-1')

        if not self.bucket:
            raise ValueError("S3_BUCKET_NAME not set in environment variables.")

        self.client = boto3.client('s3', region_name=self.region)

    def upload_raw_data(self, raw_data: list, run_timestamp: datetime = None) -> str:
        """
        Upload raw API JSON list to S3.

        Args:
            raw_data: List of raw API response dicts from the extractor.
            run_timestamp: UTC datetime for the run (used in the S3 key path).

        Returns:
            S3 key where the data was stored.
        """
        if run_timestamp is None:
            run_timestamp = datetime.utcnow()

        key = (
            f"raw/{run_timestamp.strftime('%Y/%m/%d')}/"
            f"batch_{run_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        )

        payload = json.dumps(raw_data, default=str, indent=2).encode('utf-8')

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=payload,
                ContentType='application/json',
            )
            logger.info(
                "Archived %d raw records to s3://%s/%s",
                len(raw_data), self.bucket, key,
            )
            return key

        except ClientError as e:
            logger.error("S3 upload failed: %s", e)
            raise
