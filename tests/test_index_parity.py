"""Index-parity tests for the jsonb-vs-cols experiment.

`places_jsonb` and `places_jsonb_lowthr` exist to isolate ONE variable: the
`toast_tuple_target` reloption. For the comparison to be fair they must carry
the *same indexes* — otherwise a query can pick a different plan for reasons
unrelated to TOAST. These tests parse the import DDL and enforce that invariant
at the source, so the two tables can't silently drift apart again.

Pure-text tests (no live DB), matching the style of test_import.py.
"""

import re
from pathlib import Path

import pytest

IMPORT_DIR = Path(__file__).parent.parent / "import"

# CREATE INDEX [IF NOT EXISTS] <name> ON <table> <definition>
# (the DDL lives inside CALL postgres_execute('pg', '...'); each statement is
#  on a single line, and the trailing "'" closes the SQL string literal.)
_INDEX_RE = re.compile(
    r"CREATE INDEX(?: IF NOT EXISTS)? (\w+) ON (\w+)\s+(.+?)'\s*\)",
    re.IGNORECASE,
)


def index_roles(sql_path: Path, table: str) -> dict[str, str]:
    """Map each index's role -> its definition for `table` in a SQL file.

    The "role" is the index name with the table prefix stripped
    (places_jsonb_basic_category_idx -> basic_category_idx), so the same
    logical index on two tables compares equal. The definition (columns /
    expression / USING clause) is already table-independent.
    """
    text = sql_path.read_text()
    roles: dict[str, str] = {}
    for name, tbl, definition in _INDEX_RE.findall(text):
        if tbl != table:
            continue
        role = name[len(table) + 1:] if name.startswith(table + "_") else name
        roles[role] = " ".join(definition.split())  # normalise whitespace
    return roles


# ---------------------------------------------------------------------------
# parity between the two JSONB tables
# ---------------------------------------------------------------------------

class TestJsonbLowthrIndexParity:
    def _roles(self):
        jsonb = index_roles(IMPORT_DIR / "places_jsonb.sql", "places_jsonb")
        lowthr = index_roles(
            IMPORT_DIR / "places_jsonb_lowthr.sql", "places_jsonb_lowthr"
        )
        return jsonb, lowthr

    def test_both_define_some_indexes(self):
        jsonb, lowthr = self._roles()
        assert jsonb, "no indexes parsed from places_jsonb.sql"
        assert lowthr, "no indexes parsed from places_jsonb_lowthr.sql"

    def test_same_set_of_index_roles(self):
        jsonb, lowthr = self._roles()
        assert set(jsonb) == set(lowthr), (
            "places_jsonb and places_jsonb_lowthr must carry the same indexes; "
            f"only in jsonb: {set(jsonb) - set(lowthr)}; "
            f"only in lowthr: {set(lowthr) - set(jsonb)}"
        )

    def test_matching_roles_have_matching_definitions(self):
        jsonb, lowthr = self._roles()
        for role in set(jsonb) & set(lowthr):
            assert jsonb[role] == lowthr[role], (
                f"index '{role}' differs between the two tables: "
                f"jsonb={jsonb[role]!r} lowthr={lowthr[role]!r}"
            )


# ---------------------------------------------------------------------------
# the one intended difference, and the dropped trigram index
# ---------------------------------------------------------------------------

class TestIntendedDifferences:
    def test_lowthr_is_the_only_table_with_low_toast_target(self):
        lowthr = (IMPORT_DIR / "places_jsonb_lowthr.sql").read_text()
        jsonb = (IMPORT_DIR / "places_jsonb.sql").read_text()
        assert "toast_tuple_target = 128" in lowthr
        assert "toast_tuple_target" not in jsonb

    @pytest.mark.parametrize(
        "sql_file",
        ["places.sql", "places_jsonb.sql", "places_jsonb_lowthr.sql"],
    )
    def test_no_trigram_index(self, sql_file):
        # The name trigram GIN index is unused by every experiment, so it was
        # dropped from all three tables. Guard against accidental re-add.
        text = (IMPORT_DIR / sql_file).read_text().lower()
        assert "gin_trgm_ops" not in text
        assert "pg_trgm" not in text
