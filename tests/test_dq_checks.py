"""
tests/test_dq_checks.py
=======================
Unit tests for the ETL DQ Framework using pytest + PySpark.

Run all tests:
    pytest tests/ -v

Run one test:
    pytest tests/test_dq_checks.py::TestPrimaryKeyChecker -v

Coverage report:
    pytest tests/ --cov=checkers --cov-report=html
"""

import pytest
from pyspark.sql import SparkSession

# Import all checkers
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checkers.all_checkers import (
    AllowedValuesChecker, CaseChecker, CompletenessRatioChecker,
    DataTypeChecker, DefaultValueChecker, LengthChecker,
    MandatoryFieldChecker, OutlierChecker, PatternChecker,
    PrimaryKeyChecker, UniquenessChecker, ValueRangeChecker,
    WhitespaceChecker,
)
from core.base import CheckResult
from core.exceptions import DQConfigError


# ==============================================================================
# Shared Spark session — created ONCE for all tests (faster)
# ==============================================================================

@pytest.fixture(scope="session")
def spark():
    """
    Creates a single Spark session shared by all tests.
    scope="session" means it is created once and reused — much faster.
    """
    session = (
        SparkSession.builder
        .appName("DQ_Unit_Tests")
        .master("local[2]")          # 2 cores is enough for unit tests
        .config("spark.ui.enabled", "false")          # disable Spark UI during tests
        .config("spark.sql.shuffle.partitions", "2")  # small shuffle for test data
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ==============================================================================
# Helper — builds a DataFrame from a list of row dicts
# ==============================================================================

def make_df(spark, rows: list[dict]):
    """Creates a Spark DataFrame from a list of Python dicts."""
    return spark.createDataFrame(rows)


# ==============================================================================
# TestPrimaryKeyChecker
# ==============================================================================

class TestPrimaryKeyChecker:

    def test_clean_data_passes(self, spark):
        """All unique, non-null IDs should PASS both checks."""
        df = make_df(spark, [
            {"id": "1"}, {"id": "2"}, {"id": "3"}
        ])
        results = PrimaryKeyChecker(df, "id", "test").run()
        statuses = {r.check_name: r.status for r in results}
        assert statuses["PK Null"]      == "PASS"
        assert statuses["PK Duplicate"] == "PASS"

    def test_null_pk_fails(self, spark):
        """A NULL in the PK column should FAIL the null check."""
        df = make_df(spark, [
            {"id": "1"}, {"id": None}, {"id": "3"}
        ])
        results = PrimaryKeyChecker(df, "id", "test").run()
        null_result = next(r for r in results if r.check_name == "PK Null")
        assert null_result.status     == "FAIL"
        assert null_result.violations == 1

    def test_duplicate_pk_fails(self, spark):
        """Duplicate PK values should FAIL the duplicate check."""
        df = make_df(spark, [
            {"id": "1"}, {"id": "1"}, {"id": "3"}
        ])
        results = PrimaryKeyChecker(df, "id", "test").run()
        dup_result = next(r for r in results if r.check_name == "PK Duplicate")
        assert dup_result.status     == "FAIL"
        assert dup_result.violations == 1   # 2 total - 1 unique = 1 extra

    def test_missing_column_returns_error(self, spark):
        """Asking for a non-existent column should return ERROR, not crash."""
        df = make_df(spark, [{"name": "Alice"}])
        results = PrimaryKeyChecker(df, "nonexistent_col", "test").run()
        assert results[0].status == "ERROR"


# ==============================================================================
# TestMandatoryFieldChecker
# ==============================================================================

class TestMandatoryFieldChecker:

    def test_no_nulls_passes(self, spark):
        df = make_df(spark, [
            {"name": "Alice", "dept": "HR"},
            {"name": "Bob",   "dept": "IT"},
        ])
        results = MandatoryFieldChecker(df, ["name", "dept"], "test").run()
        assert all(r.status == "PASS" for r in results)

    def test_null_in_required_field_fails(self, spark):
        df = make_df(spark, [
            {"name": "Alice", "dept": "HR"},
            {"name": None,    "dept": "IT"},
        ])
        results = MandatoryFieldChecker(df, ["name"], "test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1


# ==============================================================================
# TestDataTypeChecker
# ==============================================================================

class TestDataTypeChecker:

    def test_valid_integers_pass(self, spark):
        df = make_df(spark, [{"salary": "50000"}, {"salary": "75000"}])
        results = DataTypeChecker(df, {"salary": "integer"}, dataset_name="test").run()
        assert results[0].status == "PASS"

    def test_non_integer_fails(self, spark):
        df = make_df(spark, [{"salary": "50000"}, {"salary": "abc"}])
        results = DataTypeChecker(df, {"salary": "integer"}, dataset_name="test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1

    def test_valid_dates_pass(self, spark):
        df = make_df(spark, [{"dt": "2024-01-15"}, {"dt": "2023-06-30"}])
        results = DataTypeChecker(df, {"dt": "date"}, dataset_name="test").run()
        assert results[0].status == "PASS"

    def test_invalid_date_fails(self, spark):
        df = make_df(spark, [{"dt": "2024-01-15"}, {"dt": "99-99-9999"}])
        results = DataTypeChecker(df, {"dt": "date"}, dataset_name="test").run()
        assert results[0].status == "FAIL"


# ==============================================================================
# TestPatternChecker
# ==============================================================================

class TestPatternChecker:

    def test_valid_emails_pass(self, spark):
        df = make_df(spark, [
            {"email": "alice@gmail.com"},
            {"email": "bob@company.org"},
        ])
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        results = PatternChecker(df, {"email": pattern}, "test").run()
        assert results[0].status == "PASS"

    def test_invalid_email_fails(self, spark):
        df = make_df(spark, [
            {"email": "alice@gmail.com"},
            {"email": "not_an_email"},
        ])
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        results = PatternChecker(df, {"email": pattern}, "test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1


# ==============================================================================
# TestValueRangeChecker
# ==============================================================================

class TestValueRangeChecker:

    def test_values_in_range_pass(self, spark):
        df = make_df(spark, [{"score": "85"}, {"score": "72"}])
        results = ValueRangeChecker(df, {"score": {"min": 0, "max": 100}}, "test").run()
        assert results[0].status == "PASS"

    def test_values_out_of_range_fail(self, spark):
        df = make_df(spark, [{"score": "85"}, {"score": "150"}])
        results = ValueRangeChecker(df, {"score": {"min": 0, "max": 100}}, "test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1


# ==============================================================================
# TestAllowedValuesChecker
# ==============================================================================

class TestAllowedValuesChecker:

    def test_all_allowed_pass(self, spark):
        df = make_df(spark, [{"dept": "HR"}, {"dept": "IT"}, {"dept": "Finance"}])
        results = AllowedValuesChecker(df, {"dept": ["HR", "IT", "Finance"]}, "test").run()
        assert results[0].status == "PASS"

    def test_not_allowed_fails(self, spark):
        df = make_df(spark, [{"dept": "HR"}, {"dept": "MAGIC"}])
        results = AllowedValuesChecker(df, {"dept": ["HR", "IT", "Finance"]}, "test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1


# ==============================================================================
# TestWhitespaceChecker
# ==============================================================================

class TestWhitespaceChecker:

    def test_clean_strings_pass(self, spark):
        df = make_df(spark, [{"name": "Alice"}, {"name": "Bob"}])
        results = WhitespaceChecker(df, ["name"], "test").run()
        assert results[0].status == "PASS"

    def test_spaces_fail(self, spark):
        df = make_df(spark, [{"name": "Alice"}, {"name": "  Bob  "}])
        results = WhitespaceChecker(df, ["name"], "test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1


# ==============================================================================
# TestCompletenessRatioChecker
# ==============================================================================

class TestCompletenessRatioChecker:

    def test_above_threshold_passes(self, spark):
        """95% filled > 90% threshold — should PASS."""
        rows = [{"name": f"User{i}"} for i in range(95)]
        rows += [{"name": None}] * 5    # 5% null
        df = spark.createDataFrame(rows)
        results = CompletenessRatioChecker(df, ["name"], 90.0, "test").run()
        assert results[0].status == "PASS"

    def test_below_threshold_fails(self, spark):
        """50% filled < 90% threshold — should FAIL."""
        rows = [{"name": f"User{i}"} for i in range(50)]
        rows += [{"name": None}] * 50   # 50% null
        df = spark.createDataFrame(rows)
        results = CompletenessRatioChecker(df, ["name"], 90.0, "test").run()
        assert results[0].status == "FAIL"

    def test_invalid_min_pct_raises(self, spark):
        """min_pct > 100 should raise DQConfigError at construction time."""
        df = make_df(spark, [{"name": "Alice"}])
        with pytest.raises(DQConfigError):
            CompletenessRatioChecker(df, ["name"], 150.0, "test")


# ==============================================================================
# TestCaseChecker
# ==============================================================================

class TestCaseChecker:

    def test_correct_upper_case_passes(self, spark):
        df = make_df(spark, [{"dept": "HR"}, {"dept": "IT"}])
        results = CaseChecker(df, {"dept": "upper"}, "test").run()
        assert results[0].status == "PASS"

    def test_wrong_case_fails(self, spark):
        df = make_df(spark, [{"dept": "HR"}, {"dept": "it"}])
        results = CaseChecker(df, {"dept": "upper"}, "test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1


# ==============================================================================
# TestDefaultValueChecker
# ==============================================================================

class TestDefaultValueChecker:

    def test_no_placeholders_passes(self, spark):
        df = make_df(spark, [{"name": "Alice"}, {"name": "Bob"}])
        results = DefaultValueChecker(df, {"name": ["N/A", "null", "test"]}, "test").run()
        assert results[0].status == "PASS"

    def test_placeholder_detected(self, spark):
        df = make_df(spark, [{"name": "Alice"}, {"name": "N/A"}])
        results = DefaultValueChecker(df, {"name": ["N/A", "null", "test"]}, "test").run()
        assert results[0].status     == "FAIL"
        assert results[0].violations == 1

    def test_case_insensitive_detection(self, spark):
        """Placeholders should be detected regardless of case."""
        df = make_df(spark, [{"name": "Alice"}, {"name": "NULL"}, {"name": "n/a"}])
        results = DefaultValueChecker(df, {"name": ["N/A", "null"]}, "test").run()
        assert results[0].violations == 2


# ==============================================================================
# TestCheckResult
# ==============================================================================

class TestCheckResult:

    def test_passed_property(self):
        r = CheckResult("test", "col", "PASS", 0)
        assert r.passed is True
        assert r.failed is False

    def test_failed_property(self):
        r = CheckResult("test", "col", "FAIL", 10)
        assert r.passed is False
        assert r.failed is True

    def test_to_dict(self):
        r = CheckResult("Null Check", "salary", "FAIL", 5, "5 nulls found", "employees")
        d = r.to_dict()
        assert d["check_name"]  == "Null Check"
        assert d["status"]      == "FAIL"
        assert d["violations"]  == 5
        assert d["dataset"]     == "employees"
