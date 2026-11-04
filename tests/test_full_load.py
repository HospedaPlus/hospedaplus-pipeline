"""Recovery rules of the nightly snapshot (bronze_neon_full_load) and how silver picks it."""

from spark_jobs.session import complete_days


def test_only_finished_snapshots_count():
    keys = ["bronze/x/fecha=2026-11-03/part-00000.jsonl", "bronze/x/fecha=2026-11-03/_SUCCESS",
            "bronze/x/fecha=2026-11-04/part-00000.jsonl"]  # 4-nov failed half way
    assert complete_days(keys) == ["2026-11-03"]


def test_legacy_folders_without_marker_still_count():
    keys = ["bronze/x/fecha=2026-11-02/part-00000.jsonl", "bronze/x/fecha=2026-11-03/part-00000.jsonl"]
    assert complete_days(keys) == ["2026-11-02", "2026-11-03"]


def test_stale_parts_and_old_marker_are_removed():
    import importlib.util
    import pathlib
    import sys
    sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "dags"))
    root = pathlib.Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location("full", root / "dags/bronze_neon_full_load.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    d = "bronze/x/fecha=2026-11-04/"
    keys = [d + f"part-{i:05d}.jsonl" for i in range(5)] + [d + "_SUCCESS"]
    assert mod.stale_keys(keys, 3) == [d + "part-00003.jsonl", d + "part-00004.jsonl", d + "_SUCCESS"]
