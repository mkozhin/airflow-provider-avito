# Замена источника snapshot_ts: logical_date → dag_run.start_date

## Overview
`AvitoCallsOperator` при `add_snapshot_ts=True` записывает в поле `snapshot_ts`
значение `context["logical_date"]`. Это неверно для DAG, запускаемых по расписанию.

**Проблема:** `logical_date` — это начало data interval прогона, а не реальное время
запуска. Для `@daily`, `catchup=False`, окно «последние 30 дней»:
- реальный старт прогона, например, `2026-06-25 06:00`;
- `logical_date` = `2026-06-24 00:00:00` (начало интервала);
- в JSON пишется `2026-06-24T00:00:00` — отставание на сутки и всегда `00:00:00`.

Это ломает семантику `snapshot_ts` («когда сделан срез данных») и дедуп при
backfill/catchup (исторические `logical_date` из прошлого).

**Решение:** брать `context["dag_run"].start_date` — реальное wall-clock UTC время
старта прогона. Единое для всех тасков прогона, монотонно растёт, корректно при
backfill. Аналогичное решение уже принято в соседнем проекте `airflow-provider-cian`.

## Context (from discovery)
- Файлы:
  - `airflow_provider_avito/operators/calls.py:94-96` — вычисление `snapshot_ts`.
  - `tests/test_operator.py` — фейковый контекст (`_make_context`, строка 32-33) и
    тесты на `snapshot_ts`.
  - `README.md:75`, `README.md:115` — описание источника `snapshot_ts`.
- Паттерн: `snapshot_ts` инъецируется только при `output_format="json"`; при `csv`
  поле не добавляется, но в `result` возвращается.
- Зависимости: `apache-airflow>=2.9.1,<3.0`. `dag_run` всегда есть в контексте при
  исполнении планировщиком; `DagRun.start_date` заполнено к моменту запуска таска.

## Development Approach
- **Testing approach**: Regular (правка кода + правка тестов в одной задаче).
- Изменение точечное, обратная совместимость сохраняется: `add_snapshot_ts=False` —
  поведение идентично текущему, поле не появляется.
- Прогнать тесты после изменений; все тесты должны проходить.

## Testing Strategy
- **Unit tests**: единственный уровень в проекте (`pytest`, `tests/test_operator.py`).
  Обновить фейковый контекст и тесты, завязанные на источник `snapshot_ts`.
- E2E-тестов в проекте нет.

## Progress Tracking
- Отмечать выполненное `[x]` сразу.
- Новые задачи — с префиксом ➕, блокеры — с ⚠️.

## Solution Overview
Одна строка в `execute()`: источник значения меняется с `context["logical_date"]`
на `context["dag_run"].start_date`. Формат (`%Y-%m-%dT%H:%M:%S`) и вся остальная
логика инъекции не меняются. Без fallback — симметрично текущему коду (отсутствие
`dag_run` в контексте → `KeyError`, как раньше было с `logical_date`).

**Краевой случай:** если `dag_run` присутствует, но `start_date is None` (возможно
при `airflow tasks test` / ручных путях вне планировщика), будет `AttributeError` на
`.strftime`. Принимаем как допустимое — оператор рассчитан на исполнение планировщиком,
где `start_date` заполнен к моменту запуска таска. Отдельный тест не добавляем.

## Technical Details
- Было: `context["logical_date"].strftime("%Y-%m-%dT%H:%M:%S")`
- Стало: `context["dag_run"].start_date.strftime("%Y-%m-%dT%H:%M:%S")`
- `DagRun.start_date` — `datetime` (tz-aware UTC), `strftime` даёт ISO 8601 без TZ —
  тот же формат, что и раньше.

## What Goes Where
- **Implementation Steps**: правка оператора, тестов, README.
- **Post-Completion**: применить аналогичную правку в `airflow-provider-cian` (уже
  запланировано там отдельно); проверить реальный scheduled-прогон в Airflow.

## Implementation Steps

### Task 1: Заменить источник snapshot_ts в операторе

**Files:**
- Modify: `airflow_provider_avito/operators/calls.py`

- [x] в `execute()` (строки 94-96) заменить `context["logical_date"]` на
      `context["dag_run"].start_date`, формат и условие `if self.add_snapshot_ts`
      оставить без изменений
- [x] убедиться, что прочая логика инъекции (`json`-only, возврат в `result`) не тронута

### Task 2: Обновить тесты под dag_run.start_date

**Files:**
- Modify: `tests/test_operator.py`

- [x] в `_make_context` (строка 32-33) заменить ключ `logical_date` на `dag_run` с
      атрибутом `start_date=datetime(2026, 6, 1, 12, 0, 0)` (например, через
      `MagicMock(start_date=...)` или `SimpleNamespace`)
- [x] проверить, что `test_snapshot_ts_in_result`, `test_snapshot_ts_added_to_json_records`,
      `test_snapshot_ts_not_added_to_csv` (строка 298, значение проверяется на строке 313)
      ожидают прежнее значение `2026-06-01T12:00:00` (меняется только источник в контексте)
- [x] перенацелить `test_snapshot_ts_requires_logical_date_in_context` (строка 326):
      переименовать в `test_snapshot_ts_requires_dag_run_in_context`, контекст без
      `dag_run` должен падать с `KeyError`
- [x] поправить устаревшие упоминания `logical_date` в текстах тестов: docstring на
      строке 271 («the literal logical_date-derived value»), docstring переименованного
      теста (строка 327) и inline-комментарий `# no logical_date` (строка 329) → заменить
      на `dag_run`/`start_date`
- [x] запустить `pytest` — все тесты должны проходить

### Task 3: Обновить README

**Files:**
- Modify: `README.md`

- [x] строка 75: заменить «the DAG run's `logical_date`» на «the DAG run's
      `start_date` (реальное время старта прогона)»
- [x] строка 115: в таблице полей заменить «DAG run's `logical_date`» на
      «DAG run's `start_date`»
- [x] проверить, нет ли других упоминаний `logical_date` как источника `snapshot_ts`

### Task 4: Verify acceptance criteria
- [x] `snapshot_ts` берётся из `dag_run.start_date`, формат `YYYY-MM-DDTHH:MM:SS`
- [x] `add_snapshot_ts=False` — поведение не изменилось (поле отсутствует, `None` в result)
- [x] CSV с `add_snapshot_ts=True` — поле не добавляется в файл, но есть в result
- [x] запустить полный набор тестов: `pytest`

### Task 5: [Final] Завершение
- [x] обновить память проекта, если ключевое решение по источнику `snapshot_ts`
      зафиксировано там
- [x] переместить этот план в `docs/plans/completed/`

## Post-Completion
*Информационно, без чекбоксов*

**Внешние системы:**
- В `airflow-provider-cian` аналогичная правка планируется отдельно (источник
  изначально брался из этого проекта).

**Ручная проверка:**
- Проверить реальный scheduled-прогон (`@daily`, `catchup=False`): в JSON-файлах
  `snapshot_ts` должен равняться фактическому времени старта прогона, а не полуночи
  предыдущих суток.
