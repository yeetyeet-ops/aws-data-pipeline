"""
Tests for src/processor.py
Run with:  pytest tests/ -v
"""

import io
import textwrap
from pathlib import Path

import pandas as pd
import pytest

from src.processor import (
    clean_and_validate_rows,
    compute_metrics,
    process_file,
    validate_schema,
)

# ---------------------------------------------------------------------------
# Helpers / shared CSV strings
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures"
SCENARIO_DIR = FIXTURE_DIR / "scenarios"
FIXTURE_DATE = "20260418"

VALID_CSV = """\
order_id,customer_id,product_id,order_date,quantity,unit_price,payment_status
ORD001,CUST001,PROD101,2024-01-15,2,29.99,paid
ORD002,CUST002,PROD102,2024-01-15,3,15.50,paid
ORD003,CUST003,PROD103,2024-01-16,1,49.99,pending
"""

DIRTY_CSV = """\
order_id,customer_id,product_id,order_date,quantity,unit_price,payment_status
ORD001,CUST001,PROD101,2024-01-15,2,29.99,paid
ORD001,CUST001,PROD101,2024-01-15,2,29.99,paid
ORD002,CUST002,PROD102,2024-01-15,,15.50,paid
ORD003,CUST003,PROD103,2024-01-15,1,,paid
ORD004,CUST004,PROD104,2024-01-15,-5,10.00,paid
ORD005,CUST005,PROD105,2024-01-15,1,10.00,NOT_VALID
ORD006,CUST006,PROD106,2024-01-15,3,9.99,paid
"""


def _df(csv_text: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(csv_text))


# ===========================================================================
# Part A — Schema validation
# ===========================================================================

class TestValidateSchema:
    def test_passes_with_all_required_columns(self):
        ok, errors = validate_schema(_df(VALID_CSV))
        assert ok is True
        assert errors == []

    def test_fails_when_column_is_missing(self):
        df = _df(VALID_CSV).drop(columns=["unit_price"])
        ok, errors = validate_schema(df)
        assert ok is False
        assert any("unit_price" in e for e in errors)

    def test_fails_with_multiple_missing_columns(self):
        df = _df(VALID_CSV).drop(columns=["order_id", "payment_status"])
        ok, errors = validate_schema(df)
        assert ok is False
        assert any("order_id" in e or "payment_status" in e for e in errors)

    def test_fails_with_extra_unexpected_columns(self):
        df = _df(VALID_CSV)
        df["store_id"] = "STORE_001"  # extra column — not allowed
        ok, errors = validate_schema(df)
        assert ok is False
        assert any("store_id" in e for e in errors)


# ===========================================================================
# Part B — Row-level cleaning and flagging
# ===========================================================================

class TestCleanAndValidateRows:
    def test_exact_duplicate_is_flagged(self):
        _, bad = clean_and_validate_rows(_df(DIRTY_CSV))
        assert bad["validation_errors"].str.contains("duplicate_row").any()

    def test_missing_quantity_is_flagged(self):
        _, bad = clean_and_validate_rows(_df(DIRTY_CSV))
        assert bad["validation_errors"].str.contains("missing_quantity").any()

    def test_missing_unit_price_is_flagged(self):
        _, bad = clean_and_validate_rows(_df(DIRTY_CSV))
        assert bad["validation_errors"].str.contains("missing_unit_price").any()

    def test_negative_quantity_is_flagged(self):
        _, bad = clean_and_validate_rows(_df(DIRTY_CSV))
        assert bad["validation_errors"].str.contains("non_positive_quantity").any()

    def test_invalid_payment_status_is_flagged(self):
        _, bad = clean_and_validate_rows(_df(DIRTY_CSV))
        assert bad["validation_errors"].str.contains("invalid_payment_status").any()

    def test_good_rows_all_pass_through_clean_data(self):
        good, bad = clean_and_validate_rows(_df(VALID_CSV))
        assert len(bad) == 0
        assert len(good) == 3

    def test_dirty_csv_produces_correct_split(self):
        """DIRTY_CSV: 7 rows — 1 duplicate + 1 missing_qty + 1 missing_price
        + 1 negative_qty + 1 invalid_status = 5 bad; 2 good (ORD001 first +
        ORD006)."""
        good, bad = clean_and_validate_rows(_df(DIRTY_CSV))
        assert len(good) == 2
        assert len(bad) == 5

    def test_bad_df_has_validation_errors_column(self):
        _, bad = clean_and_validate_rows(_df(DIRTY_CSV))
        assert "validation_errors" in bad.columns
        assert bad["validation_errors"].notna().all()
        assert (bad["validation_errors"].str.len() > 0).all()

    def test_zero_quantity_is_flagged(self):
        csv = (
            "order_id,customer_id,product_id,order_date,quantity,unit_price,payment_status\n"
            "ORD001,CUST001,PROD101,2024-01-15,0,29.99,paid\n"
        )
        _, bad = clean_and_validate_rows(_df(csv))
        assert bad["validation_errors"].str.contains("non_positive_quantity").any()


# ===========================================================================
# Part C — Metrics computation
# ===========================================================================

class TestComputeMetrics:
    def _good(self) -> pd.DataFrame:
        good, _ = clean_and_validate_rows(_df(VALID_CSV))
        return good

    def test_line_revenue_is_quantity_times_unit_price(self):
        metrics = compute_metrics(self._good())
        row = metrics["line_revenue_df"][metrics["line_revenue_df"]["order_id"] == "ORD001"].iloc[0]
        assert abs(row["line_revenue"] - 2 * 29.99) < 0.001

    def test_daily_revenue_sums_correctly(self):
        metrics = compute_metrics(self._good())
        daily = {r["order_date"]: r["daily_revenue"] for r in metrics["daily_revenue"]}
        # 2024-01-15: ORD001 (2×29.99=59.98) + ORD002 (3×15.50=46.50) = 106.48
        assert abs(daily["2024-01-15"] - 106.48) < 0.01

    def test_top_products_sorted_by_revenue_descending(self):
        metrics = compute_metrics(self._good())
        revenues = [r["total_revenue"] for r in metrics["top_products"]]
        assert revenues == sorted(revenues, reverse=True)

    def test_payment_success_rate_all_paid(self):
        df = _df(VALID_CSV)
        df["payment_status"] = "paid"
        good, _ = clean_and_validate_rows(df)
        metrics = compute_metrics(good)
        assert metrics["payment_success_rate"] == 1.0

    def test_payment_success_rate_mixed(self):
        # VALID_CSV: ORD001=paid, ORD002=paid, ORD003=pending → 2/3
        metrics = compute_metrics(self._good())
        assert abs(metrics["payment_success_rate"] - round(2 / 3, 4)) < 0.0001

    def test_orders_per_customer_unique_order_count(self):
        metrics = compute_metrics(self._good())
        cust_map = {r["customer_id"]: r["order_count"] for r in metrics["orders_per_customer"]}
        assert cust_map["CUST001"] == 1
        assert cust_map["CUST002"] == 1


# ===========================================================================
# Part D — Full pipeline (process_file)
# ===========================================================================

class TestProcessFile:
    def test_happy_path_returns_correct_shapes(self):
        good_df, bad_df, summary = process_file(VALID_CSV, "store_01_20240115.csv")
        assert len(bad_df) == 0
        assert summary["good_rows"] == 3
        assert summary["bad_rows"] == 0

    def test_line_revenue_column_not_in_parquet_output(self):
        good_df, _, _ = process_file(VALID_CSV, "store_01_20240115.csv")
        assert "line_revenue" not in good_df.columns

    def test_process_file_normalizes_numeric_columns_for_storage(self):
        good_df, _, _ = process_file(VALID_CSV, "store_01_20240115.csv")
        assert pd.api.types.is_float_dtype(good_df["quantity"])
        assert pd.api.types.is_float_dtype(good_df["unit_price"])

    def test_dirty_data_produces_nonzero_bad_row_rate(self):
        _, bad_df, summary = process_file(DIRTY_CSV, "store_02_20240115.csv")
        assert summary["bad_rows"] > 0
        assert summary["bad_row_rate"] > 0.0

    def test_raises_value_error_on_invalid_schema(self):
        bad_schema = "order_id,customer_id\nORD001,CUST001\n"
        with pytest.raises(ValueError, match="Schema validation failed"):
            process_file(bad_schema)

    def test_summary_keys_are_complete(self):
        _, _, summary = process_file(VALID_CSV)
        expected_keys = {
            "source_key", "processed_at", "total_rows", "good_rows", "bad_rows",
            "bad_row_rate", "payment_success_rate", "daily_revenue",
            "top_products", "orders_per_customer", "duration_ms",
        }
        assert expected_keys.issubset(summary.keys())

    def test_all_bad_rows_gives_empty_good_df(self):
        all_bad = (
            "order_id,customer_id,product_id,order_date,quantity,unit_price,payment_status\n"
            "ORD001,CUST001,PROD101,2024-01-15,-1,29.99,paid\n"
            "ORD002,CUST002,PROD102,2024-01-15,2,-10.00,paid\n"
        )
        good_df, bad_df, summary = process_file(all_bad)
        assert len(good_df) == 0
        assert summary["bad_rows"] == 2
        assert summary["payment_success_rate"] == 0.0

    def test_fixture_valid_file_loads_cleanly(self):
        csv_text = (SCENARIO_DIR / f"store_99_{FIXTURE_DATE}.csv").read_text()
        good_df, bad_df, summary = process_file(csv_text, f"store_99_{FIXTURE_DATE}.csv")
        assert summary["good_rows"] == 5
        assert summary["bad_rows"] == 0

    def test_fixture_dirty_file_catches_all_bad_rows(self):
        csv_text = (SCENARIO_DIR / f"store_13_{FIXTURE_DATE}.csv").read_text()
        good_df, bad_df, summary = process_file(csv_text, f"store_13_{FIXTURE_DATE}.csv")
        # dirty_data.csv: 7 rows — duplicate, missing_qty, missing_price,
        # negative_qty, invalid_status = 5 bad; ORD001(first) + ORD006 = 2 good
        assert summary["bad_rows"] == 5
        assert summary["good_rows"] == 2

    def test_process_file_default_source_key(self):
        """source_key defaults to 'unknown' without raising."""
        good_df, _, summary = process_file(VALID_CSV)
        assert summary["source_key"] == "unknown"
