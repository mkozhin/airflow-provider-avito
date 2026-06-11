from __future__ import annotations

import csv
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from airflow_provider_avito.hooks.avito import AvitoHook
from airflow_provider_avito.operators.calls import AvitoCallsOperator, _CSV_FIELDS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_operator(**kwargs) -> AvitoCallsOperator:
    defaults = dict(
        avito_conn_id="avito_test",
        date_from="2026-06-01",
        date_to="2026-06-03",
        task_id="test_task",
    )
    defaults.update(kwargs)
    return AvitoCallsOperator(**defaults)


def _make_context(run_id: str = "manual__2026-06-01T00:00:00+00:00") -> dict:
    return {"run_id": run_id}


def _make_record(date: str, call_id: int = 1) -> dict:
    return {
        "id": call_id,
        "buyer_phone": "+70000000001",
        "seller_phone": "+70000000002",
        "virtual_phone": "+70000000003",
        "create_time": f"{date}T10:00:00+03:00",
        "start_time": f"{date}T10:01:00+03:00",
        "date": date,
        "duration": 60,
        "waiting_duration": 5.0,
        "price": 10000,
        "price_rub": 100.0,
        "status_id": 0,
        "status": "Целевой",
        "item_id": 999,
        "group_title": "Test Group",
        "is_arbitrage_available": False,
        "record_url": "https://example.com/record.mp3",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_operator_execute_single_day(tmp_path):
    op = _make_operator(base_dir=str(tmp_path))
    records = [_make_record("2026-06-01", 1), _make_record("2026-06-01", 2)]

    with patch.object(AvitoHook, "get_calls", return_value=records):
        result = op.execute(_make_context())

    assert len(result) == 1
    assert result[0]["date"] == "2026-06-01"
    assert os.path.exists(result[0]["path"])


def test_operator_execute_multi_day(tmp_path):
    op = _make_operator(base_dir=str(tmp_path))
    # Deliberately pass records in reverse date order to exercise sorted() in execute()
    records = [
        _make_record("2026-06-03", 3),
        _make_record("2026-06-01", 1),
        _make_record("2026-06-02", 2),
    ]

    with patch.object(AvitoHook, "get_calls", return_value=records):
        result = op.execute(_make_context())

    assert len(result) == 3
    dates = [r["date"] for r in result]
    assert dates == ["2026-06-01", "2026-06-02", "2026-06-03"]
    for entry in result:
        assert os.path.exists(entry["path"])


def test_operator_execute_empty(tmp_path):
    op = _make_operator(base_dir=str(tmp_path))

    with patch.object(AvitoHook, "get_calls", return_value=[]):
        result = op.execute(_make_context())

    assert result == []


def test_operator_skip_empty_days(tmp_path):
    op = _make_operator(base_dir=str(tmp_path), date_from="2026-06-01", date_to="2026-06-05")
    records = [
        _make_record("2026-06-01", 1),
        _make_record("2026-06-03", 2),
    ]

    with patch.object(AvitoHook, "get_calls", return_value=records):
        result = op.execute(_make_context())

    assert len(result) == 2
    assert {r["date"] for r in result} == {"2026-06-01", "2026-06-03"}


def test_operator_json_output(tmp_path):
    op = _make_operator(base_dir=str(tmp_path), output_format="json")
    record = _make_record("2026-06-01", 1)

    with patch.object(AvitoHook, "get_calls", return_value=[record]):
        result = op.execute(_make_context())

    path = result[0]["path"]
    lines = open(path, encoding="utf-8").readlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["id"] == 1
    assert parsed["date"] == "2026-06-01"


def test_operator_csv_output(tmp_path):
    op = _make_operator(base_dir=str(tmp_path), output_format="csv")
    record = _make_record("2026-06-01", 42)

    with patch.object(AvitoHook, "get_calls", return_value=[record]):
        result = op.execute(_make_context())

    path = result[0]["path"]
    assert path.endswith(".csv")
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["id"] == "42"
    assert rows[0]["date"] == "2026-06-01"
    # Verify all CSV fields are present as headers
    assert set(reader.fieldnames) == set(_CSV_FIELDS)


def test_operator_invalid_format():
    with pytest.raises(ValueError, match="output_format"):
        _make_operator(output_format="xlsx")


def test_operator_path_with_account_id(tmp_path):
    op = _make_operator(base_dir=str(tmp_path), account_id="my_account")
    record = _make_record("2026-06-01", 1)

    with patch.object(AvitoHook, "get_calls", return_value=[record]):
        result = op.execute(_make_context(run_id="manual__2026"))

    path = result[0]["path"]
    assert "my_account" in path


def test_operator_path_without_account_id(tmp_path):
    op = _make_operator(base_dir=str(tmp_path), account_id=None)
    record = _make_record("2026-06-01", 1)

    with patch.object(AvitoHook, "get_calls", return_value=[record]):
        result = op.execute(_make_context())

    path = result[0]["path"]
    # Path should not contain an account segment (base_dir/safe_run_id/date.ext)
    relative = os.path.relpath(path, str(tmp_path))
    parts = relative.split(os.sep)
    # Expect: safe_run_id / date.json — 2 parts, not 3
    assert len(parts) == 2


def test_csv_fields_match_mapped_record():
    """_CSV_FIELDS must match the keys produced by AvitoHook._map_record."""
    from airflow_provider_avito.hooks.avito import AvitoHook as _Hook

    hook = _Hook.__new__(_Hook)
    raw = {
        "id": 1,
        "buyerPhone": "+7000",
        "sellerPhone": "+7001",
        "virtualPhone": "+7002",
        "createTime": "2026-06-01T09:00:00+03:00",
        "startTime": "2026-06-01T09:01:00+03:00",
        "duration": 30,
        "waitingDuration": 2.5,
        "price": 5000,
        "statusId": 0,
        "itemId": 123,
        "groupTitle": "Group",
        "isArbitrageAvailable": True,
        "recordUrl": "https://example.com/rec.mp3",
    }
    record = hook._map_record(raw)
    assert set(_CSV_FIELDS) == set(record.keys())


def test_operator_passes_account_id_to_hook(tmp_path):
    """account_id must be forwarded to AvitoHook constructor."""
    op = _make_operator(base_dir=str(tmp_path), account_id="my_acc")
    with patch("airflow_provider_avito.operators.calls.AvitoHook") as MockHook:
        mock_instance = MockHook.return_value
        mock_instance.get_calls.return_value = []
        op.execute(_make_context())
    MockHook.assert_called_once_with(avito_conn_id="avito_test", account_id="my_acc")


def test_operator_drops_records_with_date_none(tmp_path):
    """Records where date is None (no startTime) must be silently skipped."""
    op = _make_operator(base_dir=str(tmp_path))
    records = [
        {"id": 1, "date": None, "buyer_phone": None, "seller_phone": None,
         "virtual_phone": None, "create_time": None, "start_time": None,
         "duration": None, "waiting_duration": None, "price": None,
         "price_rub": None, "status_id": None, "status": None, "item_id": None,
         "group_title": None, "is_arbitrage_available": None, "record_url": None},
        _make_record("2026-06-01", 2),
    ]
    with patch.object(AvitoHook, "get_calls", return_value=records):
        result = op.execute(_make_context())

    assert len(result) == 1
    assert result[0]["date"] == "2026-06-01"


def test_operator_write_overwrites_existing_file(tmp_path):
    """_write must replace a pre-existing file at the same path."""
    op = _make_operator(base_dir=str(tmp_path), output_format="json")
    path = str(tmp_path / "run" / "2026-06-01.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("stale content\n")

    records = [_make_record("2026-06-01", 99)]
    op._write(records, path)

    with open(path, encoding="utf-8") as f:
        data = json.loads(f.readline())
    assert data["id"] == 99
