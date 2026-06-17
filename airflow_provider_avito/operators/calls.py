from __future__ import annotations

import csv
import json
import os
import re
from collections import defaultdict
from typing import TypedDict

from airflow.models import BaseOperator

from airflow_provider_avito.hooks.avito import AvitoHook

_OUTPUT_FORMATS = ("json", "csv")


class CallRecord(TypedDict):
    date: str
    path: str
    snapshot_ts: str | None


_CSV_FIELDS = [
    "id",
    "buyer_phone",
    "seller_phone",
    "virtual_phone",
    "create_time",
    "start_time",
    "date",
    "duration",
    "waiting_duration",
    "price",
    "price_rub",
    "status_id",
    "status",
    "item_id",
    "group_title",
    "is_arbitrage_available",
    "record_url",
]


class AvitoCallsOperator(BaseOperator):
    template_fields = ("date_from", "date_to", "avito_conn_id")
    ui_color = "#e8f4fd"

    def __init__(
        self,
        *,
        avito_conn_id: str = "avito_default",
        date_from: str,
        date_to: str,
        base_dir: str = "/tmp/avito",
        output_format: str = "json",
        account_id: str | None = None,
        add_snapshot_ts: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if output_format not in _OUTPUT_FORMATS:
            raise ValueError(f"output_format must be one of {_OUTPUT_FORMATS}, got {output_format!r}")
        self.avito_conn_id = avito_conn_id
        self.date_from = date_from
        self.date_to = date_to
        self.base_dir = base_dir
        self.output_format = output_format
        self.account_id = account_id
        self.add_snapshot_ts = add_snapshot_ts

    def _build_path(self, run_id: str, date: str, account_id: str | None = None) -> str:
        safe_run_id = re.sub(r"[^\w-]", "_", run_id)
        ext = "json" if self.output_format == "json" else "csv"
        if account_id is not None:
            return os.path.join(self.base_dir, account_id, safe_run_id, f"{date}.{ext}")
        return os.path.join(self.base_dir, safe_run_id, f"{date}.{ext}")

    def _write(self, records: list[dict], path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if self.output_format == "json":
            with open(path, "w", encoding="utf-8") as f:
                for row in records:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        else:
            with open(path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(records)

    def execute(self, context) -> list[CallRecord]:
        hook = AvitoHook(avito_conn_id=self.avito_conn_id, account_id=self.account_id)
        calls = hook.get_calls(self.date_from, self.date_to)

        snapshot_ts = (
            context["logical_date"].strftime("%Y-%m-%dT%H:%M:%S") if self.add_snapshot_ts else None
        )

        by_date: dict[str, list[dict]] = defaultdict(list)
        for record in calls:
            date = record.get("date")
            if date:
                by_date[date].append(record)

        run_id = context["run_id"]
        result: list[CallRecord] = []

        for date, records in sorted(by_date.items()):
            if not records:
                continue
            path = self._build_path(run_id, date, self.account_id)
            if snapshot_ts and self.output_format == "json":
                records = [{**row, "snapshot_ts": snapshot_ts} for row in records]
            self._write(records, path)
            result.append(CallRecord(date=date, path=path, snapshot_ts=snapshot_ts))

        return result
