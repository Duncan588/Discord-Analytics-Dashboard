"""一次性备份/合并脚本中的纯逻辑：backup_2025.py 与 merge_script.py。"""
import json
import os

import pytest

from Preparation_Before_Use import backup_2025, merge_script


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("普通标题", "普通标题"),
        ('a<b>c:d"e/f\\g|h?i*j', "a_b_c_d_e_f_g_h_i_j"),
        # 控制字符先被替换成下划线，之后的 strip() 不会再去掉这些下划线。
        ("line\nbreak\ttab\r", "line_break_tab_"),
        ("   ", "unknown_thread"),
        ("", "unknown_thread"),
    ],
)
def test_sanitize_filename(raw, expected):
    assert backup_2025.sanitize_filename(raw) == expected


def test_sanitize_filename_truncates_long_titles():
    result = backup_2025.sanitize_filename("x" * 200)
    assert result == "x" * 60 + "..."


def test_setup_logging_writes_to_log_file(tmp_path, monkeypatch):
    log_file = tmp_path / "backup_log.txt"
    monkeypatch.setattr(backup_2025, "LOG_FILE", str(log_file))
    logger = backup_2025.setup_logging()
    added = logger.handlers[-2:]
    try:
        logger.info("hello")
        for handler in added:
            handler.flush()
        assert "hello" in log_file.read_text(encoding="utf-8")
    finally:
        for handler in added:
            logger.removeHandler(handler)
            handler.close()


def test_processed_ids_checkpoint_round_trip(tmp_path, monkeypatch):
    checkpoint = tmp_path / "processed_ids_2025.txt"
    monkeypatch.setattr(backup_2025, "CHECKPOINT_FILE", str(checkpoint))

    assert backup_2025.load_processed_ids() == set()

    backup_2025.mark_as_processed("1")
    backup_2025.mark_as_processed("2")
    backup_2025.mark_as_processed("2")
    assert backup_2025.load_processed_ids() == {"1", "2"}


def test_load_processed_ids_ignores_blank_lines(tmp_path, monkeypatch):
    checkpoint = tmp_path / "ids.txt"
    checkpoint.write_text("1\n\n  \n2\n", encoding="utf-8")
    monkeypatch.setattr(backup_2025, "CHECKPOINT_FILE", str(checkpoint))
    assert backup_2025.load_processed_ids() == {"1", "2"}


def test_mark_as_processed_survives_write_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(backup_2025, "CHECKPOINT_FILE", str(tmp_path / "missing-dir" / "ids.txt"))
    backup_2025.mark_as_processed("1")


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("标题 [123456].json", "123456"),
        ("no-id.json", None),
        ("[123].txt", None),
        ("[abc].json", None),
    ],
)
def test_get_id_from_filename(filename, expected):
    assert merge_script.get_id_from_filename(filename) == expected


def _write_export(path, thread_id, timestamp, guild=True):
    payload = {
        "channel": {"id": thread_id, "category": "Forum"},
        "messages": [{"id": f"m{thread_id}", "timestamp": timestamp}],
    }
    if guild:
        payload["guild"] = {"id": "500", "name": "Guild"}
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_merge_with_csv_logic_filters_by_csv_and_sorts(tmp_path, monkeypatch):
    (tmp_path / "backup").mkdir()
    _write_export(tmp_path / "backup" / "later [1].json", "1", "2026-02-01T00:00:00+00:00")
    _write_export(tmp_path / "backup" / "earlier [2].json", "2", "2026-01-01T00:00:00+00:00", guild=False)
    _write_export(tmp_path / "backup" / "excluded [3].json", "3", "2026-03-01T00:00:00+00:00")
    (tmp_path / "backup" / "broken [4].json").write_text("{not json", encoding="utf-8")
    (tmp_path / "all_threads.csv").write_text("id\n1\n2\n4\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(merge_script, "CSV_FILE_PATH", "all_threads.csv")
    monkeypatch.setattr(merge_script, "JSON_FILES_PATTERN", "backup/**/*.json")
    monkeypatch.setattr(merge_script, "OUTPUT_FILENAME", "merged.json")

    merge_script.merge_with_csv_logic()

    merged = json.loads((tmp_path / "merged.json").read_text(encoding="utf-8"))
    assert merged["messageCount"] == 2
    assert [m["id"] for m in merged["messages"]] == ["m2", "m1"]
    assert merged["channel"]["id"] == merge_script.TARGET_CATEGORY_ID
    assert merged["channel"]["name"] == "2025合并存档-Forum"


def test_merge_with_csv_logic_without_csv(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(merge_script, "CSV_FILE_PATH", "missing.csv")
    merge_script.merge_with_csv_logic()
    assert "读取 CSV 失败" in capsys.readouterr().out


def test_merge_with_csv_logic_without_matching_files(tmp_path, monkeypatch, capsys):
    (tmp_path / "all_threads.csv").write_text("id\n1\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(merge_script, "CSV_FILE_PATH", "all_threads.csv")
    monkeypatch.setattr(merge_script, "JSON_FILES_PATTERN", "backup/**/*.json")
    monkeypatch.setattr(merge_script, "OUTPUT_FILENAME", "merged.json")

    merge_script.merge_with_csv_logic()

    assert "没有读取到任何有效的 JSON 数据" in capsys.readouterr().out
    assert not os.path.exists(tmp_path / "merged.json")
