"""Pytest configuration — add the scripts directory to sys.path once."""

import os
import sys
from pathlib import Path

# Allow `import benchmark` and similar module imports from the scripts directory.
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

# import.py reads PG* env vars at module level when building the DSN string.
# Provide safe defaults so it loads cleanly in a test environment.
for _var, _val in [
    ("PGDATABASE", "test"),
    ("PGUSER", "test"),
    ("PGPASSWORD", "test"),
    ("PGHOST", "localhost"),
    ("PGPORT", "5432"),
]:
    os.environ.setdefault(_var, _val)
