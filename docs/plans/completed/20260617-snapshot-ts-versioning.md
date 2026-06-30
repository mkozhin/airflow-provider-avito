# Snapshot-TS Versioning for AvitoCallsOperator

## Overview

Добавить опциональный параметр `add_snapshot_ts: bool = False` в `AvitoCallsOperator`.
Когда включён — оператор инъецирует поле `snapshot_ts` в каждую JSON-запись и возвращает его
в результирующем списке, что позволяет DAG-у строить версионированные пути в S3.

**Проблема:** текущий DAG перезаписывает данные при каждом запуске. Нельзя отследить историю
изменений статуса звонка между выгрузками.

**Решение:** каждая выгрузка получает метку времени (`snapshot_ts`), которая:
1. добавляется в каждую JSON-строку файла — для Spark и ClickHouse
2. возвращается в `result` — чтобы DAG мог строить уникальное имя файла в S3

**Ключевые решения из брейншторма:**
- Значение: `context["logical_date"].strftime("%Y-%m-%dT%H:%M:%S")` — ISO 8601 без таймзоны
- Источник: `logical_date` стабилен при ретраях и одинаков для всех тасков одного DAG-рана
- Только JSON-формат (CSV не меняется — нет нужды, структура фиксирована)
- По умолчанию `False` — обратно совместимо, поведение не меняется

## Context (from discovery)

- **Провайдер:** `apache-airflow>=2.9.1,<3.0` — `logical_date` доступен с Airflow 2.2 ✓
- **Основной файл:** `airflow_provider_avito/operators/calls.py`
  - `AvitoCallsOperator._write()` — место инъекции `snapshot_ts` в записи
  - `AvitoCallsOperator.execute()` — место вычисления `snapshot_ts` и возврата в result
- **Тесты:** `tests/test_operator.py` — 12 существующих тестов, паттерны понятны
- **Смежный DAG:** `airflow_examples/dags/avito_provider/avito_to_bq_and_s3_multi_account.py`
  — образец для нового DAG с версионированием

## Development Approach

- **Testing approach:** Regular (код → тесты)
- Полная обратная совместимость: `add_snapshot_ts=False` — поведение идентично текущему
- CSV-формат не трогаем
- Новый DAG — отдельный файл, существующий DAG не меняется

## Testing Strategy

- **Unit tests:** в `tests/test_operator.py` по паттернам существующих тестов
  - `add_snapshot_ts=False` — поле НЕ появляется в файле, `snapshot_ts` в result равен `None`
  - `add_snapshot_ts=True` — поле присутствует в каждой JSON-строке, правильный формат
  - CSV с `add_snapshot_ts=True` — поле НЕ добавляется в CSV
  - значение берётся из `context["logical_date"]`, не из `datetime.now()`

## Solution Overview

```
AvitoCallsOperator(add_snapshot_ts=True)
    │
    ├── execute(context)
    │     snapshot_ts = context["logical_date"].strftime("%Y-%m-%dT%H:%M:%S")
    │     ...для каждой даты...
    │     _write(records, path, snapshot_ts)     ← инъекция в JSON
    │     result.append({..., "snapshot_ts": snapshot_ts})
    │
    └── вернуть result с snapshot_ts

DAG avito_to_s3_versioned
    │
    ├── AvitoCallsOperator(add_snapshot_ts=True)
    │     result = [{"date": ..., "path": ..., "snapshot_ts": "2024-06-17T14:30:22"}, ...]
    │
    └── make_s3_params(results)
          S3 key: .../_date=YYYYMMDD/calls_20240617T143022.json   ← уникальный per-snapshot
          replace=False                                            ← не перезаписывать
```

## Technical Details

**Формат `snapshot_ts`:**
- В JSON-поле: `"2024-06-17T14:30:22"` (ISO 8601, UTC без суффикса)
- В имени файла S3: `20240617T143022` (без разделителей, безопасен для путей)
- Преобразование: `snapshot_ts.replace("-", "").replace(":", "")` → убрать `:` и `-`

> **Примечание:** в продакшене `context["logical_date"]` — это `pendulum.DateTime` с tz-aware UTC.
> `strftime("%Y-%m-%dT%H:%M:%S")` намеренно убирает суффикс таймзоны (`+00:00`).
> В тестах передаётся naive `datetime` — `strftime` ведёт себя идентично, тест репрезентативен.

**Путь в S3 (новый DAG):**
```
{S3_PREFIX}/{account_id}
  /_year=YYYY/_month=MM/_day=DD
  /_date=YYYYMMDD
  /calls_{snapshot_ts_compact}.json
```

**Сигнатура `_write` не меняется** — обогащение записей происходит в `execute()` до вызова `_write()`:
```python
# _write() остаётся чистым I/O-методом:
def _write(self, records: list[dict], path: str) -> None: ...

# обогащение — в execute(), перед вызовом _write():
if snapshot_ts:
    records = [{**row, "snapshot_ts": snapshot_ts} for row in records]
self._write(records, path)
```

**Типизированный контракт результата** (`CallRecord` TypedDict в `calls.py`):
```python
from typing import TypedDict

class CallRecord(TypedDict):
    date: str
    path: str
    snapshot_ts: str | None
```
`execute()` возвращает `list[CallRecord]`. Экспортировать из модуля чтобы DAG-и и тесты могли использовать тип.

**Элемент результата:**
```python
CallRecord(date="2024-06-17", path="/tmp/avito/.../2024-06-17.json", snapshot_ts="2024-06-17T14:30:22")
# или snapshot_ts=None если add_snapshot_ts=False
```

**Примеры запросов (для docstring нового DAG):**

ClickHouse — последняя версия:
```sql
SELECT * FROM s3('s3://bucket/prefix/**/*.json', 'JSONEachRow')
WHERE toDateTime(snapshot_ts) = (
    SELECT MAX(toDateTime(snapshot_ts)) FROM s3('s3://bucket/prefix/**/*.json', 'JSONEachRow')
)
```

ClickHouse — история изменений статусов:
```sql
SELECT id, status, toDateTime(snapshot_ts) AS snapshot_dt
FROM s3('s3://bucket/prefix/**/*.json', 'JSONEachRow')
ORDER BY id, snapshot_dt
```

Spark — последняя версия:
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.read.json("s3://bucket/prefix/")
w = Window.partitionBy("id").orderBy(F.desc("snapshot_ts"))
latest = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```

## What Goes Where

**Implementation Steps** — всё в этом репозитории (`airflow-provider-avito`).
**Post-Completion** — новый DAG создаётся в отдельном репозитории `airflow_examples`.

## Implementation Steps

### Task 1: Добавить `add_snapshot_ts` в AvitoCallsOperator

**Files:**
- Modify: `airflow_provider_avito/operators/calls.py`

- [x] добавить `CallRecord` TypedDict (поля: `date: str`, `path: str`, `snapshot_ts: str | None`) — до класса оператора
- [x] добавить параметр `add_snapshot_ts: bool = False` в `__init__` и сохранить как `self.add_snapshot_ts`
- [x] `_write()` НЕ меняется — сигнатура остаётся `_write(self, records: list[dict], path: str) -> None`
- [x] в `execute()`: вычислить `snapshot_ts = context["logical_date"].strftime("%Y-%m-%dT%H:%M:%S") if self.add_snapshot_ts else None`
- [x] в `execute()`, перед вызовом `_write()`: если `snapshot_ts` и формат `json` — обогатить записи: `records = [{**row, "snapshot_ts": snapshot_ts} for row in records]`
- [x] вызов остаётся `self._write(records, path)` — без изменений
- [x] добавить `"snapshot_ts"` в каждый элемент `result`: `result.append(CallRecord(date=date, path=path, snapshot_ts=snapshot_ts))`
- [x] тип возврата `execute()` изменить на `list[CallRecord]`
- [x] убедиться что `add_snapshot_ts` НЕ добавляется в `template_fields` (это bool, не шаблонизируется)

### Task 2: Тесты для нового параметра

**Files:**
- Modify: `tests/test_operator.py`

- [x] обновить `_make_context()`: добавить `logical_date` — `datetime(2026, 6, 1, 12, 0, 0)` (не строка, объект с `.strftime`)
- [x] обновить существующие тесты которые вызывают `execute()` и проверяют result — добавить `assert result[0]["snapshot_ts"] is None` (backward compat)
- [x] `test_snapshot_ts_added_to_json_records`: `add_snapshot_ts=True` → каждая JSON-строка содержит поле `snapshot_ts` в формате `YYYY-MM-DDTHH:MM:SS`
- [x] `test_snapshot_ts_in_result`: `add_snapshot_ts=True` → каждый элемент result содержит `snapshot_ts == "2026-06-01T12:00:00"` (литерал, не recomputed `strftime` — иначе тест тавтологичен)
- [x] `test_snapshot_ts_not_added_by_default`: `add_snapshot_ts=False` (default) → поле `snapshot_ts` отсутствует в JSON-файле
- [x] `test_snapshot_ts_not_added_to_csv`: `add_snapshot_ts=True, output_format="csv"` → CSV-файл не содержит колонку `snapshot_ts`
- [x] `test_snapshot_ts_empty_result`: `add_snapshot_ts=True`, `get_calls` возвращает `[]` → `execute()` не падает, возвращает `[]` (проверить что `logical_date` читается без ошибки даже когда `_write` не вызывается)
- [x] запустить все тесты: `cd /Users/mkozhin/PycharmProjects/airflow-provider-avito && .venv/bin/pytest tests/ -v` — должны пройти все

### Task 3: Убрать `dateTimeTo` из запроса к API

**Контекст:** API Авито игнорирует `dateTimeTo` в теле `callsByTime` — поле принимается, но не влияет на результат.
Фильтрация по дате-до уже реализована на клиенте (строки 295, 300 в `hooks/avito.py`).
Наличие `dateTimeTo` в теле вводит в заблуждение: читатель думает, что API фильтрует по этому параметру.

**Files:**
- Modify: `airflow_provider_avito/hooks/avito.py`
- Modify: `tests/test_hook.py`

**hooks/avito.py:**
- [x] убрать параметр `datetime_to` из сигнатуры `_make_request`
- [x] убрать `"dateTimeTo": datetime_to` из тела запроса в `_make_request`
- [x] убрать `datetime_to = date_to + "T23:59:59+03:00"` из `get_calls`
- [x] убрать передачу `datetime_to` в вызове `self._make_request(token, offset, datetime_from, datetime_to)` → `self._make_request(token, offset, datetime_from)`

**tests/test_hook.py:**
- [x] все прямые вызовы `hook._make_request(...)` — убрать 4-й аргумент `datetime_to` (6 мест в `TestMakeRequestRetry`)
- [x] в `test_request_body_and_headers`: убрать `"dateTimeTo": datetime_to` из ожидаемого тела; убрать `datetime_to` из параметров вызова
- [x] запустить тесты: `.venv/bin/pytest tests/test_hook.py -v` — должны пройти все

### Task 4: Проверка acceptance criteria

**Files:** (только чтение, без изменений)

- [x] `add_snapshot_ts=False` — поведение идентично текущему (поле не добавляется, `snapshot_ts=None` в result)
- [x] `add_snapshot_ts=True, output_format="json"` — поле есть в каждой JSON-строке
- [x] `add_snapshot_ts=True, output_format="csv"` — поле НЕ добавлено в CSV
- [x] значение `snapshot_ts` берётся из `context["logical_date"]`, формат `YYYY-MM-DDTHH:MM:SS`
- [x] тело запроса к API не содержит `dateTimeTo`
- [x] все существующие тесты проходят без изменения их смысла
- [x] запустить полный suite: `.venv/bin/pytest tests/ -v` (59 passed)
- [x] переместить план в `docs/plans/completed/`

## Post-Completion

**Новый DAG в `airflow_examples`** (отдельный репозиторий — вне scope этого плана):
- Создать `dags/avito_provider/avito_to_s3_versioned.py`
- `AvitoCallsOperator(..., add_snapshot_ts=True)`
- В `make_s3_params`: имя файла `calls_{snapshot_ts_compact}.json` где `snapshot_ts_compact = r["snapshot_ts"].replace("-", "").replace(":", "")`
- `LocalFilesystemToS3Operator.partial(..., replace=False)` — не перезаписывать
- Убрать BQ-загрузку (S3-only DAG)
- Добавить в docstring примеры ClickHouse и Spark запросов из раздела Technical Details

**Ручная проверка:**
- Запустить DAG вручную дважды подряд — убедиться что в S3 появились два файла `calls_*.json` в одной папке
- Проверить что JSON-записи содержат поле `snapshot_ts` в правильном формате
- Выполнить ClickHouse-запрос «последняя версия» — убедиться что возвращает только один snapshot
