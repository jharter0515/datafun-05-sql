"""case_duckdb_library.py - Project script (library domain).

Author: Denise Case
Date: 2026-02

Purpose:
- Read csv files into a DuckDB database.
- Use Python to automate SQL scripts (stored in files).
- Log the pipeline process.

Paths (relative to repo root):
   SQL:  sql/duckdb/*.sql
   CSV:  data/library/branch.csv
   CSV:  data/library/checkout.csv
   DB:   artifacts/duckdb/library.duckdb

OBS:
  This is my custom script for the library domain (based on the retail example).
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import duckdb
from datafun_toolkit.logger import get_logger, log_header

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P05", level="DEBUG")

# === PATH CONSTANTS ===

def find_repo_root(start: Path) -> Path:
    """Walk upward until we find pyproject.toml (repo root marker)."""
    cur = start.resolve()
    for _ in range(10):
        if (cur / "pyproject.toml").exists():
            return cur
        cur = cur.parent
    return start.resolve()  # fallback

ROOT_DIR: Final[Path] = find_repo_root(Path(__file__).resolve().parent)
DATA_DIR: Final[Path] = ROOT_DIR / "data" / "library"
SQL_DIR: Final[Path] = ROOT_DIR / "sql" / "duckdb"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts" / "duckdb"
DB_PATH: Final[Path] = ARTIFACTS_DIR / "library.duckdb"

CREATE_TABLES_SQL: Final[Path] = SQL_DIR / "create_library_tables.sql"
LOAD_DATA_SQL: Final[Path] = SQL_DIR / "load_library_data.sql"
COUNTS_SQL: Final[Path] = SQL_DIR / "query_library_counts.sql"
KPI_SQL: Final[Path] = SQL_DIR / "query_library_kpi.sql"


# === HELPERS ===


def read_sql(sql_path: Path) -> str:
    return sql_path.read_text(encoding="utf-8")


def run_sql_script(con: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    LOG.info(f"RUN SQL script: {sql_path}")
    con.execute(read_sql(sql_path))


def run_sql_query(con: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    LOG.info("")
    LOG.info(f"RUN SQL query: {sql_path}")
    result = con.execute(read_sql(sql_path))
    rows = result.fetchall()
    cols = [c[0] for c in result.description]

    LOG.info("====================================")
    LOG.info(sql_path.name)
    LOG.info("====================================")
    LOG.info(", ".join(cols))

    for row in rows:
        LOG.info(", ".join(str(v) for v in row))


# === MAIN PIPELINE ===


def main() -> None:
    log_header(LOG, "P05 DuckDB Pipeline — Library Domain")

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    try:
        run_sql_script(con, CREATE_TABLES_SQL)
        run_sql_script(con, LOAD_DATA_SQL)
        run_sql_query(con, COUNTS_SQL)
        run_sql_query(con, KPI_SQL)
    finally:
        con.close()


if __name__ == "__main__":
    main()
