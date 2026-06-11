"""
DAG: сбор звонков Avito CPA для нескольких кабинетов → BigQuery + S3.

## Структура

```
get_dates ──┐
            ├─→ [cabinet_{id}] TaskGroup (по одному на каждый аккаунт)
ensure_gcs_bucket ─┘
```

Внутри каждого `TaskGroup` (`cabinet_{account_id}`):

```
collect → make_gcs_params → upload_gcs → load_bq  ↘
          make_s3_params  → upload_s3              cleanup
```

## Каждый кабинет

- хранит файлы в отдельной папке: `{BASE_DIR}/{account_id}/{run_id}/{date}.json`
- кладёт промежуточные файлы в GCS: `{GCS_PREFIX}/{account_id}/{run_id}/{date}.json`
- загружает в отдельную BQ таблицу: `{BQ_TABLE}_{account_id}${YYYYMMDD}`
- кладёт в S3: `{S3_PREFIX}/{account_id}/_year=.../_month=.../_day=.../_date=.../{date}.json`
- чистит только свою папку после завершения загрузок

## Формат extra в Airflow connection (мульти-аккаунт)

```json
{
  "accounts": [
    {"id": "cabinet_a", "client_id": "...", "client_secret": "..."},
    {"id": "cabinet_b", "client_id": "...", "client_secret": "..."}
  ]
}
```

При пустом или отсутствующем коннекторе (`get_accounts` возвращает `[]`):
DAG импортируется без TaskGroup-ов и без ошибок.

## Параллелизм

`max_active_tasks` ограничивает параллельность внутри одного DAG-рана.
"""

import os
import re
import shutil
from datetime import date, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow_provider_avito.hooks.avito import Account, get_accounts
from airflow_provider_avito.operators.calls import AvitoCallsOperator

# BQ schema — 17 fields matching AvitoHook._map_record output.
BQ_SCHEMA = [
    {"name": "id",                     "type": "STRING",    "mode": "NULLABLE"},
    {"name": "buyer_phone",            "type": "STRING",    "mode": "NULLABLE"},
    {"name": "seller_phone",           "type": "STRING",    "mode": "NULLABLE"},
    {"name": "virtual_phone",          "type": "STRING",    "mode": "NULLABLE"},
    {"name": "create_time",            "type": "STRING",    "mode": "NULLABLE"},
    {"name": "start_time",             "type": "STRING",    "mode": "NULLABLE"},
    {"name": "date",                   "type": "DATE",      "mode": "NULLABLE"},
    {"name": "duration",               "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "waiting_duration",       "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "price",                  "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "price_rub",              "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "status_id",              "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "status",                 "type": "STRING",    "mode": "NULLABLE"},
    {"name": "item_id",                "type": "STRING",    "mode": "NULLABLE"},
    {"name": "group_title",            "type": "STRING",    "mode": "NULLABLE"},
    {"name": "is_arbitrage_available", "type": "BOOLEAN",   "mode": "NULLABLE"},
    {"name": "record_url",             "type": "STRING",    "mode": "NULLABLE"},
]


def safe_id(run_id):
    return re.sub(r"[^\w-]", "_", run_id or "")


# ── Конфигурация ──────────────────────────────────────────────────────────────

AVITO_CONN_ID    = "avito_default"
BASE_DIR         = "/tmp/avito"

GCP_CONN_ID      = "google_cloud_default"
GCS_BUCKET       = "my-gcs-bucket"
GCS_PREFIX       = "avito/staging"
BQ_PROJECT       = "my-gcp-project"
BQ_DATASET       = "avito"
BQ_TABLE         = "calls"

S3_CONN_ID       = "aws_default"
S3_BUCKET        = "my-s3-bucket"
S3_PREFIX        = "raw/placements/cpa/avito/calls"

MAX_ACTIVE_TASKS = 5

# ── default_args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":             "analytics",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="avito_to_bq_and_s3_multi_account",
    doc_md=__doc__,
    schedule=None,
    start_date=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    default_args=DEFAULT_ARGS,
    params={
        "date_from": Param(
            (date.today() - timedelta(days=30)).isoformat(),
            type="string",
            description="Начальная дата (включительно), YYYY-MM-DD",
        ),
        "date_to": Param(
            (date.today() - timedelta(days=1)).isoformat(),
            type="string",
            description="Конечная дата (включительно), YYYY-MM-DD",
        ),
    },
    tags=["avito", "bigquery", "s3", "multi-account"],
)
def avito_to_bq_and_s3_multi_account():

    @task
    def ensure_gcs_bucket() -> None:
        client = GCSHook(gcp_conn_id=GCP_CONN_ID).get_conn()
        bucket = client.bucket(GCS_BUCKET)
        if not bucket.exists():
            bucket = client.create_bucket(GCS_BUCKET)
        bucket.lifecycle_rules = [{"action": {"type": "Delete"}, "condition": {"age": 1}}]
        bucket.patch()

    @task
    def make_gcs_params(results: list[dict], account_id: str, **context) -> list[dict]:
        sid = safe_id(context["run_id"])
        return [
            {"src": r["path"], "dst": f"{GCS_PREFIX}/{account_id}/{sid}/{r['date']}.json"}
            for r in results
        ]

    @task
    def make_bq_params(results: list[dict], account_id: str, **context) -> list[dict]:
        sid = safe_id(context["run_id"])
        return [
            {
                "source_objects": [f"{GCS_PREFIX}/{account_id}/{sid}/{r['date']}.json"],
                "destination_project_dataset_table": (
                    f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_{account_id}"
                    f"${r['date'].replace('-', '')}"
                ),
            }
            for r in results
        ]

    @task
    def make_s3_params(results: list[dict], account_id: str) -> list[dict]:
        params = []
        for r in results:
            d = r["date"]
            year, month, day = d.split("-")
            date_compact = d.replace("-", "")
            params.append({
                "filename": r["path"],
                "dest_key": (
                    f"{S3_PREFIX}/{account_id}"
                    f"/_year={year}/_month={month}/_day={day}"
                    f"/_date={date_compact}/{d}.json"
                ),
            })
        return params

    @task(trigger_rule="all_done")
    def cleanup(results: list[dict], account_id: str, **context) -> None:
        if not results:
            return
        sid = safe_id(context["run_id"])
        run_dir = os.path.join(BASE_DIR, account_id, sid)
        if not os.path.isdir(run_dir):
            return
        shutil.rmtree(run_dir)

    def make_cabinet_group(account: Account, bucket_ready) -> None:
        acc_id = account.id
        with TaskGroup(group_id=f"cabinet_{acc_id}"):
            collect = AvitoCallsOperator(
                task_id="collect",
                avito_conn_id=AVITO_CONN_ID,
                account_id=acc_id,
                base_dir=BASE_DIR,
                output_format="json",
                date_from="{{ params.date_from }}",
                date_to="{{ params.date_to }}",
            )

            upload_gcs = LocalFilesystemToGCSOperator.partial(
                task_id="upload_gcs",
                gcp_conn_id=GCP_CONN_ID,
                bucket=GCS_BUCKET,
            )

            load_bq = GCSToBigQueryOperator.partial(
                task_id="load_bq",
                gcp_conn_id=GCP_CONN_ID,
                bucket=GCS_BUCKET,
                schema_fields=BQ_SCHEMA,
                source_format="NEWLINE_DELIMITED_JSON",
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning={"type": "DAY", "field": "date"},
            )

            upload_s3 = LocalFilesystemToS3Operator.partial(
                task_id="upload_s3",
                aws_conn_id=S3_CONN_ID,
                dest_bucket=S3_BUCKET,
                replace=True,
            )

            results      = collect.output
            gcs_params   = make_gcs_params(results, account_id=acc_id)
            bq_params    = make_bq_params(results, account_id=acc_id)
            s3_params    = make_s3_params(results, account_id=acc_id)

            gcs_done = upload_gcs.expand_kwargs(gcs_params)
            bq_done  = load_bq.expand_kwargs(bq_params)
            s3_done  = upload_s3.expand_kwargs(s3_params)

            bucket_ready >> collect >> [gcs_params, bq_params, s3_params]
            gcs_done >> bq_done
            [bq_done, s3_done] >> cleanup(results, account_id=acc_id)

    bucket_ready = ensure_gcs_bucket()
    accounts = get_accounts(AVITO_CONN_ID)

    for account in accounts:
        make_cabinet_group(account, bucket_ready)


avito_to_bq_and_s3_multi_account()
