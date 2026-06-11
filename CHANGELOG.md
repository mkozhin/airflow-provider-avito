# Changelog

All notable changes to this project will be documented in this file.

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
