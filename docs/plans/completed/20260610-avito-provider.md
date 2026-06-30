# Airflow Provider для Авито CPA (звонки)

## Overview

Создать Python-пакет `airflow-provider-avito` — Airflow-провайдер для сбора статистики звонков с платформы Авито через CPA API.

Провайдер зеркалирует структуру `airflow-provider-cian` и предоставляет:
- `AvitoHook` — авторизация OAuth2 (client_credentials), пагинация, rate limiting
- `AvitoCallsOperator` — сбор звонков за диапазон дат, группировка по дням, запись файлов
- Примеры DAG для выгрузки в BQ + S3

Проблема которую решает: ручной сбор данных по звонкам с Авито невозможен в рамках Airflow без провайдера. Аналог уже есть для Циан — нужно то же самое для Авито.

## Context (from discovery)

- Эталон: `/Users/mkozhin/PycharmProjects/airflow-provider-cian/` — структура, паттерны, стиль кода
- Ключевые файлы эталона: `hooks/cian.py`, `operators/builder_reports.py`, `__init__.py`, `pyproject.toml`
- API Авито: `POST /cpa/v2/callsByTime` — rate limit 1 req/min, пагинация limit/offset
- Авторизация: OAuth2 client_credentials (`POST https://api.avito.ru/token/`), токен живёт 24ч
- Схема ответа: `CallV2` — 14 полей включая id, buyerPhone, sellerPhone, price (копейки), statusId, startTime

## Development Approach

- **testing approach**: Regular (code first, then tests)
- Полностью следовать стилю кода Cian-провайдера
- Каждый таск завершать тестами перед переходом к следующему
- Не добавлять ничего сверх согласованного дизайна (YAGNI)

## Testing Strategy

- **unit tests**: pytest, мокировать HTTP-запросы через `unittest.mock.patch`
- Тестировать хук изолированно от оператора
- Тестировать оператор изолированно от хука (мок хука)
- Базовый `test_provider_info.py` аналогично Cian

## Progress Tracking

- Отмечать `[x]` сразу при завершении пункта
- Новые задачи с префиксом ➕
- Блокеры с префиксом ⚠️

## Solution Overview

Структура проекта (зеркало Cian):

```
airflow_provider_avito/
  hooks/
    __init__.py
    avito.py          # AvitoHook + get_accounts() + Account
  operators/
    __init__.py
    calls.py          # AvitoCallsOperator
  __init__.py         # get_provider_info()
  _version.py         # версия через setuptools-scm
examples/
  bq_and_s3_dag.py
  bq_and_s3_multi_account_dag.py
tests/
  __init__.py
  test_provider_info.py
  test_hook.py
  test_operator.py
pyproject.toml
```

## Technical Details

### Connection structure (только extra)

| Сценарий | extra |
|---|---|
| 1 кабинет | `{"client_id": "...", "client_secret": "..."}` |
| N кабинетов | `{"accounts": [{"id": "label", "client_id": "...", "client_secret": "..."}, ...]}` |

`login` и `password` не используются. Имя кабинета задаётся через `account_id` у оператора.

### AvitoHook

- `conn_name_attr = "avito_conn_id"`, `default_conn_name = "avito_default"`, `conn_type = "http"`
- `__init__(avito_conn_id, account_id=None)` — один экземпляр = один аккаунт
- `_token: str | None = None` — кэш токена в памяти экземпляра (всегда для одного аккаунта)
- `_get_credentials() -> tuple[str, str]` — читает client_id/client_secret из extra по `self.account_id`
- `_fetch_token(client_id, client_secret) -> str` — POST https://api.avito.ru/token/
- `get_calls(date_from, date_to) -> list[dict]` — пагинация + фильтрация

### Пагинация и rate limit

```
dateTimeFrom = date_from + "T00:00:00+03:00"  # начало первого дня МСК (для запроса к API)
limit = 1000
offset = 0

while True:
    fetch page (retry на 429/5xx, refresh токена на 401)
    if response["error"] is non-empty → raise AirflowException  # HTTP-200 с CpaError
    calls = response["calls"]
    if not calls → break                      # пустая страница
    if all calls past date_to → break         # ранняя остановка (без sleep)
    filter calls by date (as-is из startTime[:10]) in [date_from, date_to]
    offset += limit
    sleep(62)                                 # rate limit 1 req/min, ТОЛЬКО если продолжаем
```

Retry [1, 2, 4] сек на 429/5xx. На 401 — сбросить `self._token`, перезапросить токен один раз, повторить.

### Поля выходных записей

```python
# Значения из Swagger statusId enum descriptions:
_STATUS_MAP = {0: "Целевой", 1: "На модерации", 2: "Целевой после модерации", 3: "Нецелевой после модерации"}

# 17 полей итого (14 из CallV2 + 3 производных: date, price_rub, status)
{
    "id": int,
    "buyer_phone": str,
    "seller_phone": str,
    "virtual_phone": str,
    "create_time": str,   # RFC3339 as-is
    "start_time": str,    # RFC3339 as-is
    "date": str,          # YYYY-MM-DD — start_time[:10] as-is, без конвертации
    "duration": int,
    "waiting_duration": float,
    "price": int,         # копейки
    "price_rub": float,   # price / 100
    "status_id": int,
    "status": str,        # из _STATUS_MAP, fallback "unknown_{n}"
    "item_id": int,
    "group_title": str,
    "is_arbitrage_available": bool,
    "record_url": str,
}
```

### AvitoCallsOperator

- Параметры: `avito_conn_id`, `date_from`, `date_to`, `base_dir="/tmp/avito"`, `output_format="json"`, `account_id: str | None`
- Путь файла: `{base_dir}/{account_id}/{safe_run_id}/{date}.json` или `{base_dir}/{safe_run_id}/{date}.json`
- Группирует по полю `date`, пропускает дни с 0 звонками
- Возвращает `list[dict]`: `[{"date": "2026-06-09", "path": "..."}, ...]`

## Implementation Steps

### Task 1: Инициализация проекта

**Files:**
- Create: `pyproject.toml`
- Create: `airflow_provider_avito/__init__.py`
- Create: `airflow_provider_avito/_version.py`
- Create: `airflow_provider_avito/hooks/__init__.py`
- Create: `airflow_provider_avito/operators/__init__.py`
- Create: `tests/__init__.py`
- Create: `.gitignore` (обновить)

- [x] создать `pyproject.toml`: `name = "airflow-provider-avito"`, `dynamic = ["version"]`, `description = "Apache Airflow provider for Avito CPA — collect call statistics"`, `readme = "README.md"`, `license = "MIT"`, `requires-python = ">=3.10"`, `authors = [{name = "Michael Kozhin", email = "michael@kozhin.cc"}]`, `keywords = ["airflow", "avito", "provider", "cpa", "calls"]`; `classifiers` включая `Framework :: Apache Airflow`, `Framework :: Apache Airflow :: Provider`, `Development Status :: 4 - Beta`, `License :: OSI Approved :: MIT License`; `[project.urls]` Homepage/Repository/Changelog = `https://github.com/mkozhin/airflow-provider-avito`; `[project.optional-dependencies] dev = ["pytest>=7.0"]`; зависимости `apache-airflow>=2.9.1,<3.0`, `requests>=2.28`; entry-point в группе `[project.entry-points."apache_airflow_provider"]`: `provider_info = "airflow_provider_avito:get_provider_info"`; блоки `[tool.setuptools_scm]` (version_file = `airflow_provider_avito/_version.py`), `[tool.setuptools.packages.find]`, `[tool.pytest.ini_options]` (testpaths, pythonpath)
- [x] создать `LICENSE` с текстом MIT-лицензии (год 2026, Copyright Michael Kozhin)
- [x] создать `airflow_provider_avito/__init__.py` с `get_provider_info()` — аналог Cian, но с Avito-специфичными данными (package-name, name, doc-url)
- [x] создать `airflow_provider_avito/_version.py` с заглушкой `__version__ = "0.0.0"` (setuptools-scm перезапишет при сборке)
- [x] создать пустые `__init__.py` для hooks/ и operators/
- [x] создать `tests/__init__.py`
- [x] проверить что `pip install -e .` отрабатывает без ошибок

### Task 2: AvitoHook — авторизация и базовые методы

**Files:**
- Create: `airflow_provider_avito/hooks/avito.py`

- [x] описать `Account` dataclass с sanitized `id` (аналог Cian)
- [x] реализовать `get_accounts(conn_id)` — читает `extra.accounts`, возвращает `list[Account]`, дедупликация с WARNING (аналог Cian)
- [x] реализовать `AvitoHook(BaseHook)` с атрибутами класса `conn_name_attr`, `default_conn_name`, `conn_type`, `hook_name`; `__init__(avito_conn_id, account_id=None)` сохраняет оба параметра
- [x] реализовать `_get_credentials() -> tuple[str, str]`: если `self.account_id` → ищет в extra.accounts по id (ошибка если не найден или нет client_id/client_secret); иначе → читает extra.client_id + extra.client_secret (ошибка если отсутствуют)
- [x] реализовать `_fetch_token(client_id, client_secret) -> str`: POST `https://api.avito.ru/token/`, form-encoded `grant_type=client_credentials`, возвращает `access_token`; ошибка при не-200
- [x] реализовать `_get_token(client_id, client_secret) -> str`: кэширует в `self._token`, при первом вызове вызывает `_fetch_token`
- [x] реализовать `_make_request(token, offset, datetime_from) -> dict`: POST `https://api.avito.ru/cpa/v2/callsByTime`, headers `Authorization: Bearer {token}` + `X-Source: airflow-provider-avito`, body JSON `{dateTimeFrom, limit:1000, offset}`; retry backoff [1,2,4] сек на 429/5xx; на 401 — raise приватный `_AvitoAuthError` чтобы вызывающий мог сбросить токен и повторить; проверять `response["error"]` при HTTP-200 (если непустой — raise AirflowException)

### Task 3: AvitoHook — get_calls с пагинацией

**Files:**
- Modify: `airflow_provider_avito/hooks/avito.py`

- [x] реализовать `_parse_date(start_time_str) -> str`: возвращает `start_time_str[:10]` — дата as-is без конвертации часового пояса
- [x] реализовать `_map_record(raw) -> dict`: маппинг CallV2 → выходной dict с 17 полями включая `date`, `price_rub`, `status`; обрабатывать отсутствующие поля через `.get()` (не падать при неизвестных полях)
- [x] реализовать `get_calls(date_from, date_to) -> list[dict]`:
  - получить credentials → токен → `datetime_from = date_from + "T00:00:00+03:00"`
  - пагинировать: offset=0, limit=1000
  - в цикле: fetch → проверить error в 200-ответе → проверить пустую страницу (break) → проверить ранняя остановка (break) → фильтровать → `time.sleep(62)` ТОЛЬКО если продолжаем следующую страницу
  - на `_AvitoAuthError` (401): сбросить `self._token`, перезапросить токен один раз, повторить запрос; на повторной ошибке — raise
- [x] добавить `test_connection() -> tuple[bool, str]` — получает токен через `_fetch_token`, без запроса к callsByTime (не тратит rate-limit квоту)

### Task 4: Тесты для AvitoHook

**Files:**
- Create: `tests/test_hook.py`

- [x] написать тест `test_get_credentials_single_account`: мок connection с `extra={"client_id": "X", "client_secret": "Y"}`, проверить возврат `("X", "Y")`
- [x] написать тест `test_get_credentials_multi_account`: мок connection с `extra.accounts`, проверить корректный поиск по id
- [x] написать тест `test_get_credentials_account_not_found`: ожидать AirflowException
- [x] написать тест `test_get_credentials_missing_fields`: extra без client_id — ожидать AirflowException
- [x] написать тест `test_fetch_token_success`: мок `requests.post` → `{"access_token": "tok"}`, проверить возврат токена
- [x] написать тест `test_fetch_token_error`: мок ответ 400 — ожидать AirflowException
- [x] написать тест `test_get_token_cached`: `_fetch_token` вызывается один раз при двух вызовах `_get_token`
- [x] написать тест `test_make_request_retry`: мок последовательных ответов [503, 503, 200], проверить что retry сработал
- [x] написать тест `test_parse_date`: разные RFC3339 строки → корректный срез `[:10]`
- [x] написать тест `test_map_record_full`: CallV2 dict → проверить все 17 полей
- [x] написать тест `test_map_record_missing_fields`: частичный dict → нет исключений, пустые поля = None
- [x] написать тест `test_get_calls_pagination`: мок двух страниц → правильная фильтрация и объединение
- [x] написать тест `test_get_calls_early_stop`: страница с записями за date_to+1 → остановка без sleep
- [x] написать тест `test_make_request_embedded_error`: HTTP-200 с `{"calls": [], "error": {"code": 1002, "message": "..."}}` → AirflowException
- [x] написать тест `test_make_request_nonretryable_4xx`: HTTP-400 → AirflowException немедленно (без retry)
- [x] написать тест `test_get_calls_token_refresh_on_401`: первый запрос → 401 (`_AvitoAuthError`), сбрасывает `_token`, перезапрашивает, второй запрос успешен
- [x] запустить `pytest tests/test_hook.py` — все тесты проходят

### Task 5: AvitoCallsOperator

**Files:**
- Create: `airflow_provider_avito/operators/calls.py`

- [x] определить `_CSV_FIELDS` список из 17 полей (в порядке выходного dict)
- [x] определить `_OUTPUT_FORMATS = ("json", "csv")`
- [x] реализовать `AvitoCallsOperator(BaseOperator)` с `template_fields = ("date_from", "date_to", "avito_conn_id")`
- [x] `__init__`: принять `avito_conn_id`, `date_from`, `date_to`, `base_dir`, `output_format`, `account_id`; валидировать `output_format`
- [x] `_build_path(run_id, date, account_id)`: формировать путь `{base_dir}/{account_id}/{safe_run_id}/{date}.{ext}` или `{base_dir}/{safe_run_id}/{date}.{ext}`; `safe_run_id = re.sub(r"[^\w-]", "_", run_id)`
- [x] `_write(records, path)`: создать директорию, удалить если существует, записать JSON (newline-delimited) или CSV
- [x] `execute(context)`: инстанциировать `AvitoHook(self.avito_conn_id, self.account_id)`, вызвать `hook.get_calls(date_from, date_to)`, сгруппировать по `date`, для каждой группы с `len > 0` вызвать `_write`, вернуть `list[dict]` с path и date

### Task 6: Тесты для AvitoCallsOperator

**Files:**
- Create: `tests/test_operator.py`

- [x] написать тест `test_operator_execute_single_day`: мок хука с 2 записями за один день → 1 файл, возврат списка с 1 элементом
- [x] написать тест `test_operator_execute_multi_day`: мок хука с записями за 3 дня → 3 файла
- [x] написать тест `test_operator_execute_empty`: хук возвращает [] → пустой список, файлы не создаются
- [x] написать тест `test_operator_skip_empty_days`: хук возвращает записи только за 2 из 5 дней → 2 файла
- [x] написать тест `test_operator_json_output`: проверить формат NDJSON в файле
- [x] написать тест `test_operator_csv_output`: `output_format="csv"`, проверить заголовок и строки CSV
- [x] написать тест `test_operator_invalid_format`: `output_format="xlsx"` → ValueError в `__init__`
- [x] написать тест `test_operator_path_with_account_id`: путь содержит account_id
- [x] написать тест `test_operator_path_without_account_id`: путь не содержит account_id
- [x] написать тест `test_csv_fields_match_mapped_record`: вызвать `_map_record` с полным CallV2-словарём, проверить что `set(_CSV_FIELDS) == set(record.keys())` — страховка от рассинхрона схемы между хуком и оператором
- [x] запустить `pytest tests/` — все тесты проходят

### Task 7: Базовый тест провайдера

**Files:**
- Create: `tests/test_provider_info.py`

- [x] написать `test_provider_info_keys`: проверить наличие всех ожидаемых ключей в словаре
- [x] написать `test_provider_info_values`: проверить `package-name == "airflow-provider-avito"`, `name == "Avito"`, типы списков
- [x] запустить `pytest tests/test_provider_info.py` — проходит

### Task 8: Пример DAG для одного кабинета

**Files:**
- Create: `examples/bq_and_s3_dag.py`

- [x] написать DAG `avito_to_bq_and_s3` по аналогии с Cian `bq_and_s3_dag.py`:
  - параметры `date_from`, `date_to`
  - `ensure_gcs_bucket` @task — создаёт бакет если не существует, lifecycle rule 1 день
  - один `AvitoCallsOperator` с `account_id` — возвращает `list[dict]` с `date` и `path`
  - `make_gcs_params` @task → `LocalFilesystemToGCSOperator.expand_kwargs`
  - `make_bq_params` @task → `GCSToBigQueryOperator.expand_kwargs` для каждого дня
  - `make_s3_params` @task → `LocalFilesystemToS3Operator.expand_kwargs`
  - `cleanup` @task с `trigger_rule="all_done"`
- [x] добавить `BQ_SCHEMA` со всеми 17 полями; типы: `id`/`item_id` → STRING, `duration`/`price`/`status_id` → INTEGER, `price_rub`/`waiting_duration` → FLOAT, `is_arbitrage_available` → BOOLEAN, `date` → DATE, остальные → STRING; все NULLABLE
- [x] `GCSToBigQueryOperator` с `time_partitioning={"type": "DAY", "field": "date"}`
- [x] добавить docstring с описанием структуры DAG и формата extra

### Task 9: Пример DAG для нескольких кабинетов

**Files:**
- Create: `examples/bq_and_s3_multi_account_dag.py`

- [x] написать DAG `avito_to_bq_and_s3_multi_account` по аналогии с Cian multi-account DAG:
  - `get_accounts(AVITO_CONN_ID)` для получения списка кабинетов
  - `TaskGroup` per account через `make_cabinet_group(account, dates, bucket_ready)`
  - внутри каждого TaskGroup: `AvitoCallsOperator` → upload_gcs → load_bq + upload_s3 → cleanup
  - пустой список аккаунтов не вызывает ошибку при импорте DAG
- [x] добавить docstring с форматом `extra` для multi-account

### Task 10: Тесты импорта example DAG-ов

**Files:**
- Create: `tests/test_example_dag.py`
- Create: `tests/test_example_dag_multi_account.py`

- [x] написать `test_example_dag.py`: убедиться что `examples/bq_and_s3_dag.py` импортируется без ошибок и не вызывает исключений при пустой Airflow connection (аналог Cian `tests/test_example_dag.py`)
- [x] написать `test_example_dag_multi_account.py`: убедиться что `examples/bq_and_s3_multi_account_dag.py` импортируется без ошибок при пустом `get_accounts()` (пустой список — без TaskGroup, без исключений)
- [x] запустить `pytest tests/test_example_dag*.py` — проходит

### Task N-1.5: CI/CD и публикация на PyPI

**Files:**
- Create: `.github/workflows/publish.yml`

- [x] создать `.github/workflows/publish.yml`: trigger `push: tags: ["v*"]`; job `test` с matrix python `["3.10", "3.11", "3.12"]`, `fetch-depth: 0`, `pip install -e ".[dev]"`, `pytest tests/ -v`; job `publish` с `needs: test`, `environment: pypi`, `permissions: id-token: write`, `fetch-depth: 0`, `python -m build`, `pypa/gh-action-pypi-publish@release/v1` — **`fetch-depth: 0` нужен в обоих jobs** иначе setuptools-scm не видит тег и версия будет `0.1.dev0`
- [x] создать environment `pypi` в GitHub Settings → Environments репозитория `mkozhin/airflow-provider-avito` (manual step - skipped - not automatable)
- [x] настроить OIDC Trusted Publisher на pypi.org **до первого push тега**: pypi.org → Account → Publishing → Add publisher: GitHub Actions, Owner=`mkozhin`, Repository=`airflow-provider-avito`, Workflow=`publish.yml`, Environment=`pypi` (manual step - skipped - not automatable)
- [x] проверить: `python -m build` создаёт `.whl` и `.tar.gz` в `dist/`

### Task N-1: Финальная проверка

- [x] запустить `pytest tests/` — все тесты проходят
- [x] проверить `pip install -e .` в чистом venv
- [x] убедиться что нет лишних print/debug statements

### Task N: Финальное оформление

**Files:**
- Create: `README.md`
- Create: `CHANGELOG.md`
- Modify: `.gitignore`

- [x] написать `README.md`: описание, установка, структура connection (только extra), пример DAG
- [x] написать `CHANGELOG.md` с версией 0.1.0
- [x] обновить `.gitignore`: добавить `dist/`, `*.egg-info/`, `.venv/`, `__pycache__/`, оставить в репо `Swagger_avito.json` и `avito.csv` как справочные материалы (или добавить в gitignore если они не нужны в репо) — `.gitignore` уже содержит все необходимые записи, изменения не требуются
- [x] переместить план в `docs/plans/completed/`

## Post-Completion

**Ручная проверка интеграции:**
- Создать Airflow connection `avito_default` с реальными `client_id`/`client_secret`
- Запустить DAG на реальных данных, проверить файлы в `/tmp/avito/`
- Проверить загрузку в BQ/S3 с реальными коннекторами

**Публикация (если нужно):**
- Настроить `setuptools-scm` тег версии: `git tag v0.1.0`
- Собрать: `python -m build`
- Опубликовать на PyPI или внутренний registry
