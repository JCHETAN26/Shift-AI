#!/usr/bin/env python3
"""Create the Snowflake warehouse, database, and schemas Shift.ai needs.

Run once (idempotent) before the first load + dbt run. Uses
snowflake-connector-python (bundled with dbt-snowflake), so it runs in the dbt
container.
"""
from __future__ import annotations

import os
import sys

import snowflake.connector


def _connect_kwargs() -> dict:
    """Prefer key-pair auth (works with MFA-enforced accounts); else password."""
    kwargs = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        kwargs["private_key_file"] = key_path
    elif os.environ.get("SNOWFLAKE_PASSWORD"):
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise SystemExit("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD in .env")
    return kwargs


def main() -> int:
    for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"):
        if not os.environ.get(k):
            print(f"Missing env var: {k} (set it in .env)", file=sys.stderr)
            return 1

    db = os.environ.get("SNOWFLAKE_DATABASE", "SHIFT")
    wh = os.environ.get("SNOWFLAKE_WAREHOUSE", "SHIFT_WH")

    conn = snowflake.connector.connect(**_connect_kwargs())
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE WAREHOUSE IF NOT EXISTS {wh} "
            "WAREHOUSE_SIZE=XSMALL AUTO_SUSPEND=60 AUTO_RESUME=TRUE "
            "INITIALLY_SUSPENDED=TRUE"
        )
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        for schema in ("RAW", "STAGING", "GOLD"):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
        print(f"Bootstrapped warehouse {wh}, database {db}, schemas RAW/STAGING/GOLD.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
