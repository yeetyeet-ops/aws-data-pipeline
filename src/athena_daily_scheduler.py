"""
Athena Daily Scheduler
======================
Triggered by EventBridge once per day.
Runs all queries from a SQL file in a single batch.
"""

import logging
import os
import re
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _sanitize_name(name: str) -> str:
    """Normalize a query name for S3 prefix safety and readability."""
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-_"
    lowered = name.strip().lower().replace(" ", "_")
    normalized = "".join(ch for ch in lowered if ch in allowed)
    return normalized or "query"


def _load_named_queries(sql_file_path: str) -> list[tuple[str, str]]:
    """Load semicolon-terminated SQL statements and optional -- name: labels."""
    with open(sql_file_path, "r", encoding="utf-8") as sql_file:
        sql_text = sql_file.read()

    statements: list[tuple[str, str]] = []
    for index, chunk in enumerate(sql_text.split(";"), start=1):
        statement = chunk.strip()
        if not statement:
            continue

        query_name = f"query_{index:02d}"
        lines = statement.splitlines()
        if lines:
            first_line = lines[0].strip()
            match = re.match(r"^--\s*name\s*:\s*(.+)$", first_line, flags=re.IGNORECASE)
            if match:
                query_name = _sanitize_name(match.group(1))
                # Remove the naming comment from the query text.
                statement = "\n".join(lines[1:]).strip()

        if not statement:
            continue

        statements.append((query_name, f"{statement};"))

    return statements


def lambda_handler(event: dict, context) -> dict:
    """
    Run all daily Athena queries in one batch execution.
    """
    athena = boto3.client("athena")

    database = os.environ.get("ATHENA_DATABASE", "shopmart_sales")
    results_bucket = os.environ["ATHENA_RESULTS_BUCKET"]
    results_prefix = os.environ.get("ATHENA_RESULTS_PREFIX", "athena-results/daily-cron/")
    sql_file_path = os.environ.get("ATHENA_SQL_FILE", "/var/task/athena_daily_queries.sql")
    workgroup = os.environ.get("ATHENA_WORKGROUP", "")

    named_queries = _load_named_queries(sql_file_path)
    if not named_queries:
        raise ValueError(
            f"No SQL statements found in {sql_file_path}"
        )

    execution_ids: list[str] = []
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base_prefix = results_prefix.strip("/")

    for index, (query_name, query) in enumerate(named_queries, start=1):
        output_location = (
            f"s3://{results_bucket}/{base_prefix}/"
            f"query_name={query_name}/run_date={run_date}/"
        )

        start_args = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": database},
            "ResultConfiguration": {
                "OutputLocation": output_location
            },
        }
        if workgroup:
            start_args["WorkGroup"] = workgroup

        response = athena.start_query_execution(**start_args)
        query_execution_id = response["QueryExecutionId"]
        execution_ids.append(query_execution_id)

        logger.info(
            "Started daily batch query %d/%d name=%s query_execution_id=%s output=%s",
            index,
            len(named_queries),
            query_name,
            query_execution_id,
            output_location,
        )

    return {
        "status": "started",
        "queries_started": len(execution_ids),
        "query_execution_ids": execution_ids,
        "query_names": [query_name for query_name, _ in named_queries],
        "database": database,
        "output_location_base": f"s3://{results_bucket}/{base_prefix}/",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
