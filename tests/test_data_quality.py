import csv
from pathlib import Path

DATA_FILE = Path(__file__).parents[1] / "data" / "customers.csv"


def test_sample_csv_has_expected_header_and_rows():
    with DATA_FILE.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert set(rows[0]) == {"customer_id", "full_name", "email", "city", "salary"}
    assert len(rows) == 5


def test_sample_customer_ids_are_unique():
    with DATA_FILE.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    ids = [row["customer_id"] for row in rows]
    assert len(ids) == len(set(ids))


def test_sample_salaries_are_non_negative_numeric():
    with DATA_FILE.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert all(float(row["salary"]) >= 0 for row in rows)
