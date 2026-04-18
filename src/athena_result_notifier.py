"""
Athena Query Result Notifier
============================
Triggered by S3 ObjectCreated events on Athena query result files.
Publishes a concise notification to SNS for downstream subscribers.
"""

import json
import logging
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _to_human_size(size_bytes: int) -> str:
    """Format bytes into a compact human-readable string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{size_bytes} B"


def _build_message(bucket: str, key: str, size_bytes: int) -> dict:
    """Create a structured payload for SNS subscribers."""
    return {
        "event_type": "athena_query_result_created",
        "bucket": bucket,
        "key": key,
        "size_bytes": size_bytes,
        "s3_uri": f"s3://{bucket}/{key}",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_email_message(payload: dict, region: str) -> str:
    """Create a clean, readable SNS email body for humans."""
    bucket = payload["bucket"]
    key = payload["key"]
    file_name = key.split("/")[-1]
    s3_console_key = urllib.parse.quote(key, safe="/")
    s3_console_url = (
        f"https://{region}.console.aws.amazon.com/s3/object/{bucket}"
        f"?region={region}&prefix={s3_console_key}"
    )

    return (
        "ShopMart Athena Query Result Ready\n"
        "================================\n\n"
        "Your Athena query output has been generated and stored in S3.\n\n"
        f"File name: {file_name}\n"
        f"File size: {_to_human_size(int(payload['size_bytes']))} ({payload['size_bytes']} bytes)\n"
        f"Bucket: {bucket}\n"
        f"Object key: {key}\n"
        f"S3 URI: {payload['s3_uri']}\n"
        f"Created at (UTC): {payload['created_at']}\n\n"
        "Quick links\n"
        "-----------\n"
        f"Open in S3 Console: {s3_console_url}\n\n"
        "This notification was sent automatically by the ShopMart data pipeline."
    )


def lambda_handler(event: dict, context) -> dict:
    """
    Triggered by S3 PutObject on Athena results prefix.

    Required environment variables
    -------------------------------
    SNS_TOPIC_ARN  — ARN of the SNS topic to publish notifications to
    """
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    if not sns_topic_arn:
        logger.warning("SNS_TOPIC_ARN is not configured; skipping notifications")
        return {"published": 0, "skipped": 0}

    sns_client = boto3.client("sns")
    published = 0
    skipped = 0

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            skipped += 1
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size_bytes = int(record["s3"]["object"].get("size", 0))

        # Athena commonly writes metadata files (.txt). Prefix/suffix filtering
        # should already reduce noise, but we keep a final defensive check.
        if not key.endswith(".csv"):
            skipped += 1
            continue

        payload = _build_message(bucket=bucket, key=key, size_bytes=size_bytes)
        region = os.environ.get("AWS_REGION", "us-east-1")
        email_message = _build_email_message(payload=payload, region=region)

        sns_message = {
            # For non-email subscribers, keep the machine-readable JSON payload.
            "default": json.dumps(payload, indent=2),
            # For email subscribers, send a cleaner human-friendly message.
            "email": email_message,
        }

        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=f"[ShopMart] Athena result ready | {key.split('/')[-1]}",
            MessageStructure="json",
            Message=json.dumps(sns_message),
        )
        published += 1
        logger.info("Published Athena result notification for s3://%s/%s", bucket, key)

    return {"published": published, "skipped": skipped}
