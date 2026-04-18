"""
Sales Data Pipeline Processor
==============================
Processes raw store CSV files: validates schema, cleans rows,
computes revenue metrics, and (in Lambda mode) writes results to S3.

Entry points
------------
- process_file()     — pure function, no AWS dependencies; used in tests
- lambda_handler()   — AWS Lambda entry point triggered by S3 PutObject
"""

import io
import json
import logging
import os
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "payment_status",
}

VALID_PAYMENT_STATUSES = {"paid", "pending", "failed"}


# Glue-compatible dtype mapping (float64 → double, str → string)
PARQUET_DTYPES = {
    "order_id": "string",
    "customer_id": "string",
    "product_id": "string",
    "order_date": "string",
    "quantity": "float64",
    "unit_price": "float64",
    "payment_status": "string",
}

# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def validate_schema(df: pd.DataFrame) -> Tuple[bool, list]:
    """Return (ok, list_of_error_messages) for column-level schema check."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    extra = set(df.columns) - REQUIRED_COLUMNS
    errors = []
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")
    if extra:
        errors.append(f"Extra columns not allowed: {sorted(extra)}")
    return (len(errors) == 0), errors


# ---------------------------------------------------------------------------
# Row-level validation and cleaning
# ---------------------------------------------------------------------------

def clean_and_validate_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split rows into good and bad.

    Bad rows: duplicates, missing required string fields, missing/negative
              quantity, missing/non-positive unit_price, invalid payment_status.

    Returns (good_df, bad_df) where bad_df gains a 'validation_errors' column.
    """
    df = df.copy()
    errors = pd.Series([""] * len(df), index=df.index, dtype=str)

    # Duplicate rows (keep first occurrence)
    dup_mask = df.duplicated(keep="first")
    errors[dup_mask] += "duplicate_row;"

    # Missing required string columns
    for col in ["order_id", "customer_id", "product_id", "order_date", "payment_status"]:
        mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        errors[mask] += f"missing_{col};"

    # Quantity — coerce to numeric
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    errors[df["quantity"].isna()] += "missing_quantity;"
    errors[(df["quantity"].notna()) & (df["quantity"] <= 0)] += "non_positive_quantity;"

    # unit_price — coerce to numeric
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    errors[df["unit_price"].isna()] += "missing_unit_price;"
    errors[(df["unit_price"].notna()) & (df["unit_price"] <= 0)] += "non_positive_unit_price;"

    # payment_status allowlist
    invalid_status = ~df["payment_status"].astype(str).str.lower().isin(VALID_PAYMENT_STATUSES)
    errors[invalid_status] += "invalid_payment_status;"

    bad_mask = errors.str.len() > 0
    good_df = df[~bad_mask].copy()
    bad_df = df[bad_mask].copy()
    bad_df = bad_df.assign(validation_errors=errors[bad_mask].str.rstrip(";"))

    return good_df, bad_df


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def compute_metrics(good_df: pd.DataFrame) -> dict:
    """
    Compute per-row line_revenue and aggregated business metrics.

    Returns a dict with:
        line_revenue_df         — good_df with added line_revenue column
        daily_revenue           — list[{order_date, daily_revenue}]
        top_products            — list[{product_id, total_revenue}] top 10
        orders_per_customer     — list[{customer_id, order_count}]
        payment_success_rate    — float [0, 1]
    """
    df = good_df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["line_revenue"] = df["quantity"] * df["unit_price"]

    daily_revenue = (
        df.groupby("order_date")["line_revenue"]
        .sum()
        .reset_index()
        .rename(columns={"line_revenue": "daily_revenue"})
    )
    daily_revenue["order_date"] = daily_revenue["order_date"].astype(str)

    top_products = (
        df.groupby("product_id")["line_revenue"]
        .sum()
        .reset_index()
        .rename(columns={"line_revenue": "total_revenue"})
        .sort_values("total_revenue", ascending=False)
        .head(10)
    )

    orders_per_customer = (
        df.groupby("customer_id")["order_id"]
        .nunique()
        .reset_index()
        .rename(columns={"order_id": "order_count"})
    )

    total_rows = len(df)
    paid_rows = int((df["payment_status"].astype(str).str.lower() == "paid").sum())
    payment_success_rate = round(paid_rows / total_rows, 4) if total_rows > 0 else 0.0

    return {
        "line_revenue_df": df,
        "daily_revenue": daily_revenue.to_dict(orient="records"),
        "top_products": top_products.to_dict(orient="records"),
        "orders_per_customer": orders_per_customer.to_dict(orient="records"),
        "payment_success_rate": payment_success_rate,
    }


def _prepare_good_df_for_storage(good_df: pd.DataFrame) -> pd.DataFrame:
    """Select exactly the 7 canonical input columns and enforce Glue-compatible dtypes."""
    df = good_df[list(PARQUET_DTYPES.keys())].copy()
    for col, dtype in PARQUET_DTYPES.items():
        if dtype == "float64":
            df[col] = pd.to_numeric(df[col], errors="raise").astype("float64")
        else:
            df[col] = df[col].astype(str)
    return df


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def _build_summary(
    source_key: str,
    total_rows: int,
    good_count: int,
    bad_count: int,
    metrics: dict,
    duration_ms: float,
) -> dict:
    return {
        "source_key": source_key,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": total_rows,
        "good_rows": good_count,
        "bad_rows": bad_count,
        "bad_row_rate": round(bad_count / total_rows, 4) if total_rows > 0 else 0.0,
        "payment_success_rate": metrics["payment_success_rate"],
        "daily_revenue": metrics["daily_revenue"],
        "top_products": metrics["top_products"],
        "orders_per_customer": metrics["orders_per_customer"],
        "duration_ms": round(duration_ms, 2),
    }


# ---------------------------------------------------------------------------
# Core (pure) processing function
# ---------------------------------------------------------------------------

def process_file(
    csv_content: str,
    source_key: str = "unknown",
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Process raw CSV content.

    Parameters
    ----------
    csv_content : str    Raw CSV text.
    source_key  : str    Logical identifier (S3 key or filename) for logging.

    Returns
    -------
    good_df  : pd.DataFrame  — valid rows with line_revenue added
    bad_df   : pd.DataFrame  — invalid rows with validation_errors column
    summary  : dict          — processing metadata and aggregated metrics

    Raises
    ------
    ValueError  if schema validation fails (wrong columns).
    """
    start = datetime.now(timezone.utc)

    df = pd.read_csv(io.StringIO(csv_content))
    total_rows = len(df)

    ok, schema_errors = validate_schema(df)
    if not ok:
        raise ValueError(f"Schema validation failed: {schema_errors}")

    good_df, bad_df = clean_and_validate_rows(df)

    if not good_df.empty:
        metrics = compute_metrics(good_df)
        good_df = _prepare_good_df_for_storage(metrics.pop("line_revenue_df"))
    else:
        metrics = {
            "daily_revenue": [],
            "top_products": [],
            "orders_per_customer": [],
            "payment_success_rate": 0.0,
        }

    duration_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000

    summary = _build_summary(
        source_key, total_rows, len(good_df), len(bad_df), metrics, duration_ms
    )

    logger.info(
        "processed source_key=%s total=%d good=%d bad=%d payment_success_rate=%.4f "
        "duration_ms=%.1f",
        source_key,
        total_rows,
        len(good_df),
        len(bad_df),
        metrics["payment_success_rate"],
        duration_ms,
    )

    return good_df, bad_df, summary


# ---------------------------------------------------------------------------
# AWS Lambda entry point
# ---------------------------------------------------------------------------

def _parse_filename(key: str) -> Tuple[str, str]:
    """Extract (store_id, YYYY-MM-DD) from raw/store_{id}_{YYYYMMDD}.csv."""
    name = key.rsplit("/", 1)[-1]
    match = re.fullmatch(r"store_(\d+)_(\d{8})\.csv", name)
    if not match:
        raise ValueError(
            f"Filename does not match store_{{id}}_{{YYYYMMDD}}.csv: {name!r}"
        )

    store_id = match.group(1)
    raw_date = match.group(2)
    return store_id, f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"


def _build_sns_message(default_payload: dict, email_body: str) -> str:
    """Build protocol-specific SNS message payload."""
    return json.dumps(
        {
            "default": json.dumps(default_payload, indent=2, default=str),
            "email": email_body,
        }
    )


def _build_high_error_email(
    source_bucket: str,
    source_key: str,
    summary: dict,
    error_threshold: float,
    region: str,
) -> str:
    """Create a human-friendly high error-rate alert email."""
    source_console_key = urllib.parse.quote(source_key, safe="/")
    source_console_url = (
        f"https://{region}.console.aws.amazon.com/s3/object/{source_bucket}"
        f"?region={region}&prefix={source_console_key}"
    )

    return (
        "ShopMart Pipeline Alert: High Error Rate\n"
        "=======================================\n\n"
        "A raw upload crossed the configured error threshold during validation.\n\n"
        f"Source object: s3://{source_bucket}/{source_key}\n"
        f"Bad row rate: {summary['bad_row_rate']:.2%}\n"
        f"Threshold: {error_threshold:.2%}\n"
        f"Bad rows: {summary['bad_rows']}\n"
        f"Total rows: {summary['total_rows']}\n"
        f"Processed at (UTC): {datetime.now(timezone.utc).isoformat()}\n\n"
        "Quick links\n"
        "-----------\n"
        f"Open source file in S3 Console: {source_console_url}\n\n"
        "Recommended action: Inspect bad rows in the processed bucket under errors/."
    )


def _build_failure_email(source_bucket: str, source_key: str, error_message: str, region: str) -> str:
    """Create a human-friendly pipeline failure alert email."""
    source_console_key = urllib.parse.quote(source_key, safe="/")
    source_console_url = (
        f"https://{region}.console.aws.amazon.com/s3/object/{source_bucket}"
        f"?region={region}&prefix={source_console_key}"
    )

    return (
        "ShopMart Pipeline Alert: Processing Failure\n"
        "==========================================\n\n"
        "The processor Lambda failed while handling an incoming raw file.\n\n"
        f"Source object: s3://{source_bucket}/{source_key}\n"
        f"Failure time (UTC): {datetime.now(timezone.utc).isoformat()}\n"
        f"Error: {error_message}\n\n"
        "Quick links\n"
        "-----------\n"
        f"Open source file in S3 Console: {source_console_url}\n\n"
        "Recommended action: Check CloudWatch logs for detailed traceback and retry if needed."
    )

def lambda_handler(event: dict, context) -> dict:
    """
    Triggered by S3 PutObject on the raw/ prefix.

    Required environment variables
    -------------------------------
    PROCESSED_BUCKET  — S3 bucket for processed/ and errors/ output
    SNS_TOPIC_ARN     — ARN of the SNS alert topic (optional)
    ERROR_THRESHOLD   — bad_row_rate above which an SNS alert fires (default 0.20)
    """
    import boto3  # imported here to keep unit tests free of AWS SDK calls

    s3 = boto3.client("s3")
    sns_client = boto3.client("sns")

    processed_bucket = os.environ["PROCESSED_BUCKET"]
    error_threshold = float(os.environ.get("ERROR_THRESHOLD", "0.20"))
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    region = os.environ.get("AWS_REGION", "us-east-1")

    results = []

    for record in event.get("Records", []):
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        logger.info("Starting s3://%s/%s", source_bucket, source_key)

        try:
            obj = s3.get_object(Bucket=source_bucket, Key=source_key)
            csv_content = obj["Body"].read().decode("utf-8")

            good_df, bad_df, summary = process_file(csv_content, source_key)

            # Partition from filename metadata so Athena/Glue see stable store/day paths.
            store_id, file_date = _parse_filename(source_key)
            partition = f"store={store_id}/date={file_date}"

            # Write good data under a dedicated dataset prefix so Glue creates
            # a stable table named good_data (instead of processed).
            good_key = f"processed/good_data/{partition}/part-0000.parquet"
            buf = io.BytesIO()
            good_df.to_parquet(buf, index=False, engine="pyarrow")
            s3.put_object(Bucket=processed_bucket, Key=good_key, Body=buf.getvalue())

            # Write bad rows as CSV for human review
            if not bad_df.empty:
                bad_key = f"errors/{partition}/bad_data.csv"
                s3.put_object(
                    Bucket=processed_bucket,
                    Key=bad_key,
                    Body=bad_df.to_csv(index=False).encode("utf-8"),
                )

            # Keep summaries outside the crawled analytics prefix so Glue/Athena
            # only see Parquet files under processed/.
            summary_key = f"metadata/{partition}/summary.json"
            s3.put_object(
                Bucket=processed_bucket,
                Key=summary_key,
                Body=json.dumps(summary, default=str).encode("utf-8"),
            )

            # Alert if bad-row rate exceeds threshold (BR-5)
            if sns_topic_arn and summary["bad_row_rate"] >= error_threshold:
                default_payload = {
                    "event_type": "high_error_rate",
                    "source_bucket": source_bucket,
                    "source_key": source_key,
                    "bad_row_rate": summary["bad_row_rate"],
                    "threshold": error_threshold,
                    "bad_rows": summary["bad_rows"],
                    "total_rows": summary["total_rows"],
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                }
                email_body = _build_high_error_email(
                    source_bucket=source_bucket,
                    source_key=source_key,
                    summary=summary,
                    error_threshold=error_threshold,
                    region=region,
                )
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Subject=f"[ShopMart] Pipeline alert | High error rate | {source_key}",
                    MessageStructure="json",
                    Message=_build_sns_message(default_payload, email_body),
                )

            results.append({"source_key": source_key, "status": "success", "summary": summary})

        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Failed to process %s", source_key)
            if sns_topic_arn:
                default_payload = {
                    "event_type": "pipeline_failure",
                    "source_bucket": source_bucket,
                    "source_key": source_key,
                    "error": str(exc),
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                }
                email_body = _build_failure_email(
                    source_bucket=source_bucket,
                    source_key=source_key,
                    error_message=str(exc),
                    region=region,
                )
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Subject=f"[ShopMart] Pipeline alert | Processing failure | {source_key}",
                    MessageStructure="json",
                    Message=_build_sns_message(default_payload, email_body),
                )
            results.append({"source_key": source_key, "status": "error", "error": str(exc)})

    return {"results": results}
