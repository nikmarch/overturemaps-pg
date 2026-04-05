"""Tests for scripts/import.py logic."""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

# import.py uses a reserved keyword as its filename, so load it via importlib.
# PG* env vars and sys.path are configured in conftest.py before this runs.
_spec = importlib.util.spec_from_file_location(
    "import_module", Path(__file__).parent.parent / "scripts" / "import.py"
)
_import_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_import_module)

execute_sql = _import_module.execute_sql
table_exists = _import_module.table_exists


# ---------------------------------------------------------------------------
# execute_sql
# ---------------------------------------------------------------------------

class TestExecuteSql:
    def _mock_con(self):
        return MagicMock()

    def test_executes_single_statement(self):
        con = self._mock_con()
        execute_sql(con, "SELECT 1")
        con.execute.assert_called_once_with("SELECT 1")

    def test_executes_multiple_statements(self):
        con = self._mock_con()
        execute_sql(con, "SELECT 1; SELECT 2")
        assert con.execute.call_count == 2
        con.execute.assert_any_call("SELECT 1")
        con.execute.assert_any_call("SELECT 2")

    def test_skips_drop_table_when_drop_false(self):
        con = self._mock_con()
        execute_sql(con, "DROP TABLE t; SELECT 1", drop=False)
        con.execute.assert_called_once_with("SELECT 1")

    def test_executes_drop_table_when_drop_true(self):
        con = self._mock_con()
        execute_sql(con, "DROP TABLE t; SELECT 1", drop=True)
        assert con.execute.call_count == 2
        con.execute.assert_any_call("DROP TABLE t")

    def test_ignores_empty_statements(self):
        con = self._mock_con()
        execute_sql(con, "  ;  ; SELECT 1 ;  ")
        con.execute.assert_called_once_with("SELECT 1")

    def test_empty_sql_executes_nothing(self):
        con = self._mock_con()
        execute_sql(con, "")
        con.execute.assert_not_called()

    def test_whitespace_only_sql_executes_nothing(self):
        con = self._mock_con()
        execute_sql(con, "   ;   ")
        con.execute.assert_not_called()

    def test_drop_table_default_is_false(self):
        con = self._mock_con()
        execute_sql(con, "DROP TABLE t")
        con.execute.assert_not_called()

    def test_drop_table_check_is_case_insensitive(self):
        con = self._mock_con()
        execute_sql(con, "drop table t", drop=False)
        # lowercase variant is still recognised as a DROP TABLE and skipped
        con.execute.assert_not_called()

    def test_statements_executed_in_order(self):
        con = self._mock_con()
        execute_sql(con, "SELECT 1; SELECT 2; SELECT 3")
        assert con.execute.call_args_list == [
            call("SELECT 1"),
            call("SELECT 2"),
            call("SELECT 3"),
        ]

    def test_multiline_statement_executed_as_one(self):
        con = self._mock_con()
        sql = "SELECT a,\n  b\nFROM t"
        execute_sql(con, sql)
        con.execute.assert_called_once_with("SELECT a,\n  b\nFROM t")


# ---------------------------------------------------------------------------
# table_exists
# ---------------------------------------------------------------------------

class TestTableExists:
    def _mock_con(self, count):
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (count,)
        return con

    def test_returns_true_when_count_is_one(self):
        con = self._mock_con(1)
        assert table_exists(con, "my_table") is True

    def test_returns_false_when_count_is_zero(self):
        con = self._mock_con(0)
        assert table_exists(con, "my_table") is False

    def test_table_name_included_in_query(self):
        con = self._mock_con(1)
        table_exists(con, "places")
        sql_used = con.execute.call_args[0][0]
        assert "places" in sql_used

    def test_uses_postgres_query_function(self):
        con = self._mock_con(0)
        table_exists(con, "divisions")
        sql_used = con.execute.call_args[0][0]
        assert "postgres_query" in sql_used

    def test_queries_information_schema(self):
        con = self._mock_con(0)
        table_exists(con, "t")
        sql_used = con.execute.call_args[0][0]
        assert "information_schema.tables" in sql_used
