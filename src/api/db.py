"""Tiny Postgres access helper for the API (one short-lived connection per call)."""
from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

DSN = os.environ.get("POSTGRES_DSN", "postgresql://shift:shift_dev_password@localhost:5435/shift")


@contextmanager
def cursor():
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            yield cur


def query(sql: str, params: tuple = ()) -> list[dict]:
    with cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_one(sql: str, params: tuple = ()) -> dict | None:
    with cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()
