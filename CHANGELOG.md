# Changelog

All notable changes to this project will be documented in this file.

## [0.2.1] - 2026-06-26

### Fixed

- `AvitoCallsOperator`: `snapshot_ts` теперь берётся из `dag_run.start_date` (реальное wall-clock UTC время старта прогона) вместо `logical_date` (начало data interval). Для `@daily`-DAG с `catchup=False` `logical_date` всегда равнялся полуночи предыдущих суток, что нарушало семантику поля и дедупликацию при backfill.
- `AvitoHook.parse_connection`: некорректный тип поля `accounts` (не список) больше не вызывает `TypeError` — значение логируется и игнорируется. Аналогично для записей с нестроковым `id`.
- `AvitoHook._request_calls_page`: `_token` сбрасывается в `None` перед повторным исключением при двойном 401, чтобы следующий вызов не использовал устаревший токен.
- Warning-сообщения в `parse_connection` больше не логируют `client_id` / `client_secret` в открытом виде.

### Added

- `CALL_FIELDS: tuple[str, ...]` — публичная константа в `airflow_provider_avito.hooks.avito` с каноническим упорядоченным списком 17 полей записи. `_CSV_FIELDS` оператора теперь выводится из неё, гарантируя совпадение порядка колонок.

### Changed

- Внутренний рефакторинг без изменения внешнего поведения: логика разбора `connection.extra` вынесена в `parse_connection`; auth-lifecycle (кэш учёток, обновление токена) сосредоточен в `AvitoHook._request_calls_page`, `get_calls` упрощён до чистого цикла пагинации.

## [0.2.0] - 2026-06-17

### Added

- `AvitoCallsOperator`: новый параметр `add_snapshot_ts: bool = False`. Когда включён, в каждую JSON-запись (только для `output_format="json"`) добавляется поле `snapshot_ts` — `logical_date` запуска DAG в формате `YYYY-MM-DDTHH:MM:SS` — и в каждый элемент результата оператора. Позволяет строить версионированные (не перезаписывающие) пути в S3 и выбирать последний снапшот / историю изменений в ClickHouse/Spark. По умолчанию выключен — поведение без изменений.

### Removed

- `AvitoHook._make_request` / `get_calls`: убран параметр и поле `dateTimeTo` из тела запроса к `POST /cpa/v2/callsByTime`. `dateTimeTo` был добавлен в 0.1.1 как предполагаемая причина пустых `calls`, но настоящей причиной был баг разбора ответа (обёртка `{"result": {"calls": [...]}}`), исправленный в 0.1.2. Фильтрация по `date_to` на стороне клиента не менялась и продолжает работать как раньше.

## [0.1.2] - 2026-06-11

### Fixed

- `AvitoHook.get_calls`: реальный ответ API имеет структуру `{"result": {"calls": [...]}}`, а не `{"calls": [...]}` — из-за этого оператор всегда возвращал пустой список. Код теперь поддерживает оба формата.

### Changed

- Добавлено итоговое INFO-сообщение в конце `get_calls`: `Collected N calls for DATE_FROM — DATE_TO`.

## [0.1.1] - 2026-06-11

### Fixed

- `AvitoHook._make_request`: добавлен параметр `dateTimeTo` в тело запроса к `POST /cpa/v2/callsByTime`. Без него API возвращал пустой `{"calls": []}`, что приводило к тому, что `AvitoCallsOperator` возвращал `[]` и downstream-таски пропускались несмотря на наличие данных.

## [0.1.0] - 2026-06-11

### Added

- `AvitoHook` — OAuth2 client_credentials auth, token caching, pagination, retry on 429/5xx, token refresh on 401
- `AvitoCallsOperator` — collects calls for a date range, groups by day, writes JSONL or CSV files
- `get_accounts()` helper for multi-account connections
- Example DAGs: single account (`bq_and_s3_dag.py`) and multi-account (`bq_and_s3_multi_account_dag.py`) with BigQuery + S3 upload
- GitHub Actions CI/CD workflow for automated PyPI publishing on tag push
