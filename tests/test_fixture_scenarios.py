from pathlib import Path

import pytest

from src.processor import process_file

SCENARIO_DIR = Path(__file__).parent / "fixtures" / "scenarios"
FIXTURE_DATE = "20260418"


def _read(name: str) -> str:
    return (SCENARIO_DIR / name).read_text()


@pytest.mark.parametrize(
    "filename,expected_good,expected_bad",
    [
        (f"store_01_{FIXTURE_DATE}.csv", 3, 0),   # clean_small
        (f"store_02_{FIXTURE_DATE}.csv", 6, 0),   # clean_multi_day
        (f"store_03_{FIXTURE_DATE}.csv", 2, 8),   # high_error_rate
        (f"store_04_{FIXTURE_DATE}.csv", 3, 3),   # duplicate_heavy
        (f"store_05_{FIXTURE_DATE}.csv", 2, 4),   # missing_quantity_price
        (f"store_06_{FIXTURE_DATE}.csv", 4, 2),   # invalid_status_mixedcase
        (f"store_07_{FIXTURE_DATE}.csv", 2, 4),   # zero_and_negative_values
        (f"store_08_{FIXTURE_DATE}.csv", 0, 5),   # all_bad
        (f"store_10_{FIXTURE_DATE}.csv", 2, 3),   # whitespace_missing_fields
        (f"store_11_{FIXTURE_DATE}.csv", 4, 0),   # metrics_known_values
    ],
)
def test_scenario_row_split(filename: str, expected_good: int, expected_bad: int):
    good_df, bad_df, summary = process_file(_read(filename), filename)
    assert len(good_df) == expected_good
    assert len(bad_df) == expected_bad
    assert summary["good_rows"] == expected_good
    assert summary["bad_rows"] == expected_bad


def test_scenario_schema_error_raises_value_error():
    with pytest.raises(ValueError, match="Schema validation failed"):
        process_file(_read(f"store_12_{FIXTURE_DATE}.csv"), f"store_12_{FIXTURE_DATE}.csv")


def test_scenario_extra_columns_raises_value_error():
    """store_09 has extra columns (store_id, promo_code) — must be rejected."""
    with pytest.raises(ValueError, match="Schema validation failed"):
        process_file(_read(f"store_09_{FIXTURE_DATE}.csv"), f"store_09_{FIXTURE_DATE}.csv")


def test_high_error_rate_scenario_is_over_50_percent():
    _, _, summary = process_file(_read(f"store_03_{FIXTURE_DATE}.csv"), f"store_03_{FIXTURE_DATE}.csv")
    assert summary["bad_row_rate"] > 0.5


def test_metrics_known_values_exact_revenue_and_payment_rate():
    _, _, summary = process_file(_read(f"store_11_{FIXTURE_DATE}.csv"), f"store_11_{FIXTURE_DATE}.csv")

    # 2024-02-14: (2 * 10.00) + (1 * 20.00) = 40.00
    # 2024-02-15: (3 * 5.00) + (4 * 2.50) = 25.00
    daily = {item["order_date"]: item["daily_revenue"] for item in summary["daily_revenue"]}
    assert abs(daily["2024-02-14"] - 40.0) < 0.0001
    assert abs(daily["2024-02-15"] - 25.0) < 0.0001

    # paid, paid, pending, failed -> 2/4 = 0.5
    assert summary["payment_success_rate"] == 0.5
