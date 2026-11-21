"""Source-data generator for the payments database.

    python -m generator seed     # reference data + 30 days of history (run once)
    python -m generator run      # live stream of payment activity (Ctrl+C to stop)
    python -m generator stats    # summary of what's in the database
"""
import argparse
import logging
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from .seed import seed
from .simulate import Simulator
from .stats import print_stats

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


def main():
    parser = argparse.ArgumentParser(prog="python -m generator", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_seed = sub.add_parser("seed", help="load reference data and payment history (once)")
    p_seed.add_argument("--merchants", type=int, default=300)
    p_seed.add_argument("--customers", type=int, default=20_000)
    p_seed.add_argument("--history-days", type=int, default=30)
    p_seed.add_argument("--orders-per-day", type=int, default=20_000)
    p_seed.add_argument("--random-seed", type=int, default=42,
                        help="same seed, same data distribution (timestamps follow today's date)")

    p_run = sub.add_parser("run", help="stream live payment activity")
    p_run.add_argument("--rate", type=float, default=1.0, help="new orders per second")
    p_run.add_argument("--dim-rate", type=float, default=0.5,
                       help="reference-data changes per second (signups, plan changes, ...)")
    p_run.add_argument("--duration", type=float, help="stop after this many seconds")

    sub.add_parser("stats", help="print a summary of the database")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    with connect() as conn:
        if args.command == "seed":
            seed(conn, merchants=args.merchants, customers=args.customers,
                 history_days=args.history_days, orders_per_day=args.orders_per_day,
                 random_seed=args.random_seed)
        elif args.command == "run":
            Simulator(conn, rate=args.rate, dim_rate=args.dim_rate).run(duration=args.duration)
        else:
            print_stats(conn)


if __name__ == "__main__":
    main()
