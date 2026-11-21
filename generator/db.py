"""Postgres connection shared by the generator and the tools."""
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def connect():
    # Real environment variables win over .env, so the same code works in Docker later.
    load_dotenv(PROJECT_ROOT / ".env")
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        application_name="payments-generator",  # visible in pg_stat_activity
        autocommit=True,  # each statement is its own transaction unless we open one
    )
