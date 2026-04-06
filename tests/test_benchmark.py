"""Tests for scripts/benchmark.py logic."""

import csv
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from benchmark import (
    find_latest_results,
    load_completed_ids,
    output_to_md_table,
    parse_column_names,
    parse_description,
    split_sql_statements,
    write_markdown_report,
)


# ---------------------------------------------------------------------------
# split_sql_statements
# ---------------------------------------------------------------------------

class TestSplitSqlStatements:
    def test_single_statement_no_semicolon(self):
        assert split_sql_statements("SELECT 1") == ["SELECT 1"]

    def test_single_statement_with_semicolon(self):
        assert split_sql_statements("SELECT 1;") == ["SELECT 1"]

    def test_two_statements(self):
        result = split_sql_statements("SELECT 1; SELECT 2")
        assert result == ["SELECT 1", "SELECT 2"]

    def test_trailing_semicolon_does_not_produce_empty(self):
        result = split_sql_statements("SELECT 1; SELECT 2;")
        assert result == ["SELECT 1", "SELECT 2"]

    def test_whitespace_only_between_semicolons_is_dropped(self):
        result = split_sql_statements("SELECT 1;;SELECT 2")
        assert result == ["SELECT 1", "SELECT 2"]

    def test_leading_and_trailing_whitespace_stripped(self):
        result = split_sql_statements("  SELECT 1  ;  SELECT 2  ")
        assert result == ["SELECT 1", "SELECT 2"]

    def test_multiline_statement(self):
        sql = "SELECT a,\n  b\nFROM t"
        assert split_sql_statements(sql) == ["SELECT a,\n  b\nFROM t"]

    def test_empty_string_returns_empty_list(self):
        assert split_sql_statements("") == []

    def test_only_semicolons_returns_empty_list(self):
        assert split_sql_statements(";;;") == []

    def test_three_statements(self):
        result = split_sql_statements("SELECT 1; SELECT 2; SELECT 3")
        assert result == ["SELECT 1", "SELECT 2", "SELECT 3"]


# ---------------------------------------------------------------------------
# parse_description
# ---------------------------------------------------------------------------

class TestParseDescription:
    def test_first_line_description(self):
        sql = "-- description: My cool query\nSELECT 1"
        assert parse_description(sql) == "My cool query"

    def test_description_with_leading_space(self):
        sql = "-- description:   extra spaces   \nSELECT 1"
        assert parse_description(sql) == "extra spaces"

    def test_no_description_returns_empty_string(self):
        sql = "-- columns: a, b\nSELECT 1"
        assert parse_description(sql) == ""

    def test_description_not_on_first_line(self):
        sql = "-- columns: a\n-- description: later desc\nSELECT 1"
        assert parse_description(sql) == "later desc"

    def test_inline_comment_after_sql_not_matched(self):
        # A description embedded after SQL on the same line is not a header comment
        sql = "SELECT 1 -- description: not a header"
        assert parse_description(sql) == ""

    def test_empty_description_value(self):
        sql = "-- description:\nSELECT 1"
        assert parse_description(sql) == ""

    def test_description_with_colon_in_value(self):
        sql = "-- description: ratio: 1:2\nSELECT 1"
        assert parse_description(sql) == "ratio: 1:2"


# ---------------------------------------------------------------------------
# parse_column_names
# ---------------------------------------------------------------------------

class TestParseColumnNames:
    def test_columns_comment_present(self):
        sql = "-- columns: cold, warm\nSELECT 1; SELECT 2"
        result = parse_column_names(sql, "q", 2)
        assert result == ["q_cold", "q_cold_time", "q_warm", "q_warm_time"]

    def test_columns_fallback_to_s_indices(self):
        sql = "SELECT 1; SELECT 2"
        result = parse_column_names(sql, "q", 2)
        assert result == ["q_s1", "q_s1_time", "q_s2", "q_s2_time"]

    def test_single_column(self):
        sql = "-- columns: result\nSELECT 1"
        result = parse_column_names(sql, "myfile", 1)
        assert result == ["myfile_result", "myfile_result_time"]

    def test_columns_whitespace_stripped(self):
        sql = "-- columns:  a ,  b ,  c \nSELECT 1; SELECT 2; SELECT 3"
        result = parse_column_names(sql, "f", 3)
        assert result == ["f_a", "f_a_time", "f_b", "f_b_time", "f_c", "f_c_time"]

    def test_file_stem_used_as_prefix(self):
        sql = "-- columns: x\nSELECT 1"
        result = parse_column_names(sql, "spatial_query", 1)
        assert result[0] == "spatial_query_x"

    def test_returns_pairs_result_and_time(self):
        sql = "-- columns: alpha, beta\nSELECT 1; SELECT 2"
        result = parse_column_names(sql, "f", 2)
        # Every odd index is a _time column
        assert result[1].endswith("_time")
        assert result[3].endswith("_time")

    def test_fallback_three_statements(self):
        result = parse_column_names("SELECT 1; SELECT 2; SELECT 3", "f", 3)
        assert result == [
            "f_s1", "f_s1_time",
            "f_s2", "f_s2_time",
            "f_s3", "f_s3_time",
        ]


# ---------------------------------------------------------------------------
# output_to_md_table
# ---------------------------------------------------------------------------

class TestOutputToMdTable:
    def test_empty_string_returns_no_output(self):
        assert output_to_md_table("") == "_no output_"

    def test_whitespace_only_returns_no_output(self):
        assert output_to_md_table("   \n  ") == "_no output_"

    def test_single_line_no_separator_backtick(self):
        result = output_to_md_table("just a line")
        assert result == "`just a line`"

    def test_header_and_separator_only(self):
        output = "col1 | col2\n-----+-----"
        result = output_to_md_table(output)
        assert "col1" in result
        assert "col2" in result
        assert "---" in result

    def test_full_table(self):
        output = "id | name\n---+----\n1 | Alice\n2 | Bob"
        result = output_to_md_table(output)
        lines = result.split("\n")
        assert lines[0] == "| id | name |"
        assert lines[1] == "| --- | --- |"
        assert "Alice" in result
        assert "Bob" in result

    def test_single_column(self):
        output = "count\n-----\n42"
        result = output_to_md_table(output)
        assert "count" in result
        assert "42" in result

    def test_strips_header_whitespace(self):
        output = "  col1  |  col2  \n---------+---------\n val1 | val2"
        result = output_to_md_table(output)
        assert "| col1 | col2 |" == result.split("\n")[0]


# ---------------------------------------------------------------------------
# find_latest_results
# ---------------------------------------------------------------------------

class TestFindLatestResults:
    def test_returns_none_when_no_files(self, tmp_path):
        assert find_latest_results(tmp_path) is None

    def test_returns_single_file(self, tmp_path):
        f = tmp_path / "results_2024-01-01_120000.csv"
        f.write_text("id\n1")
        assert find_latest_results(tmp_path) == f

    def test_returns_most_recent_by_name(self, tmp_path):
        old = tmp_path / "results_2024-01-01_120000.csv"
        new = tmp_path / "results_2024-06-15_080000.csv"
        old.write_text("id\n1")
        new.write_text("id\n2")
        assert find_latest_results(tmp_path) == new

    def test_ignores_non_matching_files(self, tmp_path):
        (tmp_path / "other.csv").write_text("id\n1")
        assert find_latest_results(tmp_path) is None

    def test_three_files_picks_latest(self, tmp_path):
        files = [
            "results_2024-03-01_000000.csv",
            "results_2024-01-01_000000.csv",
            "results_2024-06-01_000000.csv",
        ]
        for name in files:
            (tmp_path / name).write_text("id\n1")
        result = find_latest_results(tmp_path)
        assert result.name == "results_2024-06-01_000000.csv"


# ---------------------------------------------------------------------------
# load_completed_ids
# ---------------------------------------------------------------------------

class TestLoadCompletedIds:
    def test_reads_ids_from_csv(self, tmp_path):
        f = tmp_path / "results.csv"
        f.write_text("id,value\nabc,1\ndef,2\n")
        assert load_completed_ids(f) == {"abc", "def"}

    def test_empty_csv_header_only(self, tmp_path):
        f = tmp_path / "results.csv"
        f.write_text("id,value\n")
        assert load_completed_ids(f) == set()

    def test_single_row(self, tmp_path):
        f = tmp_path / "results.csv"
        f.write_text("id,other\nmy-id,x\n")
        assert load_completed_ids(f) == {"my-id"}

    def test_does_not_include_other_columns(self, tmp_path):
        f = tmp_path / "results.csv"
        f.write_text("id,name\n1,Alice\n2,Bob\n")
        result = load_completed_ids(f)
        assert "Alice" not in result
        assert "Bob" not in result
        assert result == {"1", "2"}


# ---------------------------------------------------------------------------
# write_markdown_report
# ---------------------------------------------------------------------------

class TestWriteMarkdownReport:
    def _make_sql_contents(self, file_path, col_names, description=""):
        """Helper to build sql_contents dict as benchmark.py expects."""
        return {file_path: ("SELECT 1", ["SELECT 1"], col_names, description)}

    def test_creates_md_file_beside_csv(self, tmp_path):
        csv_file = tmp_path / "results_2024-01-01_000000.csv"
        sql_file = tmp_path / "q.sql"
        config_items = [{"id": "1", "name": "test"}]
        col_names = ["q_cold", "q_cold_time"]
        sql_contents = self._make_sql_contents(sql_file, col_names)
        results = {"1": {"q_cold": "count\n---\n5", "q_cold_time": "123.4"}}

        write_markdown_report(csv_file, config_items, sql_contents, results)

        md_file = tmp_path / "results_2024-01-01_000000.md"
        assert md_file.exists()

    def test_md_contains_benchmark_results_header(self, tmp_path):
        csv_file = tmp_path / "results.csv"
        sql_file = tmp_path / "q.sql"
        config_items = [{"id": "1", "name": "mytest"}]
        col_names = ["q_result", "q_result_time"]
        sql_contents = self._make_sql_contents(sql_file, col_names)
        results = {"1": {"q_result": "", "q_result_time": "50.0"}}

        write_markdown_report(csv_file, config_items, sql_contents, results)

        content = csv_file.with_suffix(".md").read_text()
        assert "# Benchmark Results" in content

    def test_md_contains_config_section(self, tmp_path):
        csv_file = tmp_path / "results.csv"
        sql_file = tmp_path / "q.sql"
        config_items = [{"id": "42", "city": "Seattle"}]
        col_names = ["q_r", "q_r_time"]
        sql_contents = self._make_sql_contents(sql_file, col_names)
        results = {"42": {"q_r": "", "q_r_time": "10.0"}}

        write_markdown_report(csv_file, config_items, sql_contents, results)

        content = csv_file.with_suffix(".md").read_text()
        assert "Seattle" in content

    def test_md_contains_description_when_provided(self, tmp_path):
        csv_file = tmp_path / "results.csv"
        sql_file = tmp_path / "q.sql"
        config_items = [{"id": "1", "name": "x"}]
        col_names = ["q_r", "q_r_time"]
        sql_contents = self._make_sql_contents(sql_file, col_names, description="My description")
        results = {"1": {"q_r": "", "q_r_time": "10.0"}}

        write_markdown_report(csv_file, config_items, sql_contents, results)

        content = csv_file.with_suffix(".md").read_text()
        assert "My description" in content

    def test_md_omits_config_not_in_results(self, tmp_path):
        csv_file = tmp_path / "results.csv"
        sql_file = tmp_path / "q.sql"
        config_items = [{"id": "1", "name": "done"}, {"id": "2", "name": "missing"}]
        col_names = ["q_r", "q_r_time"]
        sql_contents = self._make_sql_contents(sql_file, col_names)
        results = {"1": {"q_r": "", "q_r_time": "10.0"}}

        write_markdown_report(csv_file, config_items, sql_contents, results)

        content = csv_file.with_suffix(".md").read_text()
        assert "done" in content
        assert "missing" not in content

    def test_timing_included_in_section_header(self, tmp_path):
        csv_file = tmp_path / "results.csv"
        sql_file = tmp_path / "q.sql"
        config_items = [{"id": "1", "name": "x"}]
        col_names = ["q_mycol", "q_mycol_time"]
        sql_contents = self._make_sql_contents(sql_file, col_names)
        results = {"1": {"q_mycol": "", "q_mycol_time": "999.9"}}

        write_markdown_report(csv_file, config_items, sql_contents, results)

        content = csv_file.with_suffix(".md").read_text()
        assert "999.9ms" in content
