# airflow-provider-avito

Apache Airflow provider for [Avito CPA](https://developers.avito.ru/api-catalog/cpa/documentation) — collect call statistics from the Avito advertising platform.

---

*Powered by [Claude Code](https://claude.ai/code)*

---

## Installation

```bash
pip install airflow-provider-avito
```

Requires Python 3.10+ and `apache-airflow>=2.9.1`.

## Connection

Create an Airflow connection of type **HTTP** with `conn_id = avito_default` (or any name you pass to the operator).

Only the **Extra** field is used. `login` and `password` are ignored.

### Single account

```json
{
  "client_id": "your_client_id",
  "client_secret": "your_client_secret"
}
```

### Multiple accounts

```json
{
  "accounts": [
    {"id": "main",    "client_id": "id1", "client_secret": "secret1"},
    {"id": "agency",  "client_id": "id2", "client_secret": "secret2"}
  ]
}
```

Use `account_id` parameter on the operator to select which account to use.

## Quick start

```python
from airflow.decorators import dag
from airflow.models.param import Param
from airflow_provider_avito.operators.calls import AvitoCallsOperator

@dag(schedule=None, params={"date_from": Param("2026-06-01"), "date_to": Param("2026-06-07")})
def avito_calls_example():
    AvitoCallsOperator(
        task_id="collect_calls",
        avito_conn_id="avito_default",
        date_from="{{ params.date_from }}",
        date_to="{{ params.date_to }}",
        base_dir="/tmp/avito",
        output_format="json",   # or "csv"
        add_snapshot_ts=True,   # optional, see "Snapshot versioning" below
    )

avito_calls_example()
```

The operator writes one JSONL (or CSV) file per date to `{base_dir}/{safe_run_id}/{date}.json` and returns a `list[dict]` with `{"date": ..., "path": ..., "snapshot_ts": ...}` entries (`snapshot_ts` is `None` unless `add_snapshot_ts=True`).

### Snapshot versioning (`add_snapshot_ts`)

By default, each DAG run writes to the same per-date path, so re-running the DAG overwrites previous output and any history of call-status changes is lost.

Set `add_snapshot_ts=True` to inject `snapshot_ts` — the DAG run's `logical_date`, formatted as `YYYY-MM-DDTHH:MM:SS` — into every JSON record and into the operator's returned `snapshot_ts` key. This lets a downstream task build a unique, non-overwriting path per run (e.g. an S3 key suffixed with the snapshot timestamp) and lets ClickHouse/Spark queries pick the latest snapshot or trace status history over time:

```sql
-- ClickHouse: latest snapshot only
SELECT * FROM s3('s3://bucket/prefix/**/*.json', 'JSONEachRow')
WHERE toDateTime(snapshot_ts) = (
    SELECT MAX(toDateTime(snapshot_ts)) FROM s3('s3://bucket/prefix/**/*.json', 'JSONEachRow')
)
```

`add_snapshot_ts` only applies to `output_format="json"`; it is ignored when `output_format="csv"` (the CSV column schema is fixed).

## Output record schema

Each record contains 17 fields:

| Field | Type | Description |
|---|---|---|
| `id` | int | Call ID |
| `buyer_phone` | str | Buyer phone |
| `seller_phone` | str | Seller phone |
| `virtual_phone` | str | Virtual (masked) phone |
| `create_time` | str | Creation time (RFC3339) |
| `start_time` | str | Call start time (RFC3339) |
| `date` | str | Date (YYYY-MM-DD) derived from `start_time` |
| `duration` | int | Call duration, seconds |
| `waiting_duration` | float | Wait time before answer, seconds |
| `price` | int | Price in kopecks |
| `price_rub` | float | Price in rubles (`price / 100`) |
| `status_id` | int | Status code |
| `status` | str | Status label (e.g. "Целевой") |
| `item_id` | int | Ad ID |
| `group_title` | str | Campaign name |
| `is_arbitrage_available` | bool | Whether arbitrage is available |
| `record_url` | str | Call recording URL |

When `add_snapshot_ts=True` and `output_format="json"`, an 18th field is added to every record:

| Field | Type | Description |
|---|---|---|
| `snapshot_ts` | str | DAG run's `logical_date`, ISO 8601 (`YYYY-MM-DDTHH:MM:SS`). Only present when `add_snapshot_ts=True` and `output_format="json"`. |

## Examples

Full production examples with BigQuery + S3 upload are in [`examples/`](examples/):

- [`bq_and_s3_multi_account_dag.py`](examples/bq_and_s3_multi_account_dag.py) — multiple accounts in parallel

## License

MIT
