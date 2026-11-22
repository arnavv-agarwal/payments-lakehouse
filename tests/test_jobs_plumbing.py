"""Running the same jobs locally and on Glue (lakehouse_env.py), and the export bookmark."""
import argparse
import sys

import pytest

import lakehouse_env
from export_bronze import new_rows_condition


def parser_with_flag():
    parser = argparse.ArgumentParser()
    lakehouse_env.add_flag(parser, "--check")
    return parser


@pytest.mark.parametrize("argv, expected", [
    ([], False),
    (["--check"], True),            # local style
    (["--check", "true"], True),    # Glue style: every argument has a value
    (["--check", "false"], False),
])
def test_flags_work_in_both_styles(argv, expected):
    assert parser_with_flag().parse_args(argv).check is expected


def test_flag_rejects_nonsense():
    with pytest.raises(SystemExit):
        parser_with_flag().parse_args(["--check", "maybe"])


def test_unknown_arguments_fail_locally_but_are_ignored_on_glue(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["job.py", "--check", "--JOB_RUN_ID", "jr_1"])
    monkeypatch.setattr(lakehouse_env, "ON_GLUE", False)
    with pytest.raises(SystemExit):
        lakehouse_env.parse_args(parser_with_flag())
    monkeypatch.setattr(lakehouse_env, "ON_GLUE", True)
    assert lakehouse_env.parse_args(parser_with_flag()).check is True


def test_finish_only_exits_on_failure():
    assert lakehouse_env.finish(failed=False) is None  # Glue treats sys.exit(0) as a failure
    with pytest.raises(SystemExit) as exit_info:
        lakehouse_env.finish(failed=True)
    assert exit_info.value.code == 1


def test_secret_prefers_the_environment(monkeypatch):
    monkeypatch.setenv("SOME_KEY", "from-env")
    assert lakehouse_env.secret("SOME_KEY", "/never/called") == "from-env"
    monkeypatch.delenv("SOME_KEY")
    with pytest.raises(KeyError):
        lakehouse_env.secret("SOME_KEY")


def test_export_takes_rows_above_the_bookmark_per_partition(spark):
    rows = spark.createDataFrame([(p, o) for p in (0, 1, 2) for o in range(6)],
                                 "kafka_partition int, kafka_offset bigint")
    picked = {(r[0], r[1]) for r in rows.where(new_rows_condition({"0": 3, "1": 5})).collect()}
    assert picked == {(0, 4), (0, 5)} | {(2, o) for o in range(6)}  # partition 2 is new: all of it
    assert rows.where(new_rows_condition({})).count() == 18         # first export: everything
