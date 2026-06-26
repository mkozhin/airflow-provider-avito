# Архитектурное углубление: connection-парсер, auth-seam, единый список полей

## Overview
Три точечных рефактора-углубления (deepening) в `airflow_provider_avito`, выявленные
через архитектурный обзор. Цель — concentrate complexity в одном месте (locality) и
улучшить тестовую поверхность, без изменения внешнего поведения провайдера.

1. **Парсер connection.extra** — знание схемы коннекта размазано по `get_accounts` и
   `_get_credentials`. Вынести в единый `parse_connection`.
2. **Auth-seam для пагинации** — логика 401→refresh→повтор протекает в цикл `get_calls`.
   Спрятать весь auth-lifecycle за один приватный метод.
3. **Единый список полей звонка** — канон полей дублируется в `_map_record` (hook) и
   `_CSV_FIELDS` (operator). Один источник правды.

Независим от плана `20260625-snapshot-ts-dag-run-start-date.md`. Кандидаты 1 и 2 оба
правят `avito.py`, но разные функции; порядок 1 → 2 → 3.

## Context (from discovery)
- Кодовая база маленькая и хорошо факторизована: `hooks/avito.py` (319),
  `operators/calls.py` (116), приличное тестовое покрытие в `tests/`.
- `CONTEXT.md` и ADR отсутствуют. Понятия фиксируем в новом `CONTEXT.md`.
- Доменный словарь: Call (звонок), Account/Cabinet (кабинет), connection.extra
  (одиночная форма `{client_id, client_secret}` / мульти `{accounts: [...]}`).

## Development Approach
- **Testing approach**: Regular (правка кода + тесты в той же задаче).
- Обратная совместимость обязательна: внешнее поведение (возвраты, тексты ошибок,
  формат файлов) не меняется. Это рефактор, не фича.
- Каждая задача завершается прогоном `pytest`; все тесты зелёные перед следующей.

## Testing Strategy
- **Unit tests** (`pytest`, `tests/`) — единственный уровень. Для каждой задачи:
  новые тесты на выделенный seam + сохранение существующих контрактов.
- E2E нет.

## Progress Tracking
- `[x]` сразу по выполнении; ➕ — новые задачи; ⚠️ — блокеры.

## Solution Overview
Каждый кандидат — отдельный seam с малым интерфейсом и сконцентрированной за ним
сложностью. Политики (ошибки, выбор кабинета) остаются у вызывающих; за seam уходит
механика (разбор формата, auth-lifecycle, список полей).

## Technical Details

### Кандидат 1 — parse_connection
- Новое в `hooks/avito.py`:
  - `AccountCredentials(id: str | None, client_id: str | None, client_secret: str | None)`
    — внутренний, с секретами; `id=None` для одиночной формы.
  - `AvitoConnectionConfig(accounts: list[AccountCredentials], single: AccountCredentials | None)`.
  - `parse_connection(extra: dict) -> AvitoConnectionConfig` — best-effort, **не бросает**:
    пропускает записи без `id` (WARNING), **санитизирует `id` (`re.sub(r"[^\w-]", "_", ...)`)
    до дедупа и сохраняет санитизированным в `AccountCredentials.id`** (семантика `Account`),
    дедуплицирует по санитизированному id (WARNING, keep first), захватывает
    `client_id`/`client_secret` как есть (могут быть None), верхнеуровневые ключи → `single`.
  - **Матчинг в `_get_credentials`**: `self.account_id` уже санитизирован (приходит из
    `get_accounts` → `Account.id`), сравнение `ac.id == self.account_id` идёт санитизированное
    с санитизированным — поведение текущего `Account(id=entry["id"]).id == self.account_id`
    сохраняется.
  - **Малформ-записи**: пер-entry пропуск записи без `id` повторяет текущее поведение обеих
    функций (обе делают `continue`); `parse_connection` не должна падать на не-dict записи —
    `get_accounts` всё равно обёрнут в `try → []`, но семантику зафиксировать тестом.
  - `Account(id)` остаётся публичным, **без секретов**; docstring уточнить.
- `get_accounts` → `parse_connection` + маппинг в `Account` (id only); политика «глотать → []».
- `_get_credentials` → `parse_connection`; политика «бросать `AirflowException`»;
  тексты ошибок и матчи (`'missing'` по `account_id`, `'client_id'` для одиночной) сохранить.

### Кандидат 2 — _request_calls_page
- Новый приватный метод `AvitoHook._request_calls_page(offset, datetime_from) -> dict`:
  владеет всем auth-lifecycle — ленивый кэш учёток (чтобы не дёргать `get_connection`
  на каждой странице), кэш токена, на `_AvitoAuthError`: сброс `self._token`, refetch,
  один повтор; на второй 401 — `AirflowException("...after token refresh")`.
- `get_calls` теряет блоки добычи учёток/токена и inline-retry; цикл становится чистой
  пагинацией (запрос страницы → парсинг payload → early-stop → фильтр → offset → sleep).

### Кандидат 3 — CALL_FIELDS
- В `hooks/avito.py` рядом с `_map_record` объявить `CALL_FIELDS: tuple[str, ...]` —
  канон из 17 имён. **Порядок обязан точно воспроизводить текущий `_CSV_FIELDS`** (порядок
  колонок CSV — внешнее поведение, `DictWriter(fieldnames=...)`).
- В `operators/calls.py`: `_CSV_FIELDS = list(CALL_FIELDS)` (импорт из hook). Кросс-модульное
  дублирование списка имён убирается; внутри hook имена всё ещё перечислены и в литерале
  `_map_record`, и в `CALL_FIELDS` — это **охраняется тестом-стражем** (не «единый источник
  правды», а «один список с проверкой совпадения»). Строить dict из `CALL_FIELDS` динамически
  не будем — это было бы over-engineering.

## What Goes Where
- **Implementation Steps**: код + тесты + `CONTEXT.md`.
- **Post-Completion**: проверить, что аналогичные seam'ы уместны в соседнем
  `airflow-provider-cian` (необязательно).

## Implementation Steps

### Task 1: Парсер connection.extra (Кандидат 1)

**Files:**
- Modify: `airflow_provider_avito/hooks/avito.py`
- Modify: `tests/test_hook.py`
- Create: `CONTEXT.md`

- [x] добавить dataclass'ы `AccountCredentials`, `AvitoConnectionConfig` и функцию
      `parse_connection(extra)` (best-effort, без raise)
- [x] переписать `get_accounts` через `parse_connection` (маппинг в `Account` без секретов)
- [x] переписать `_get_credentials` через `parse_connection`; сохранить тексты ошибок
- [x] уточнить docstring `Account` («public identity, no secrets»)
- [x] создать `CONTEXT.md` с доменными/архитектурными понятиями
- [x] добавить тесты `parse_connection`: одиночная форма, мульти, запись без id,
      дедуп, аккаунт без секретов (попадает в список, но не дропается), пустой extra
- [x] тест санитизации: мульти-аккаунт с id, требующим санитизации (напр. `a.b`),
      `_get_credentials(account_id="a_b")` находит учётку (защита от регресса матчинга)
- [x] тест: `get_accounts` для одиночной формы (`{client_id, client_secret}`, без `accounts`)
      возвращает `[]` (не подмешивает `config.single`)
- [x] тест: малформ-запись (напр. не-dict в `accounts`) — `get_accounts` возвращает `[]`,
      зафиксировать выбранную семантику
- [x] проверить, что существующие `TestGetCredentials` и `TestGetAccounts` проходят без правок
- [x] запустить `pytest` — все тесты зелёные

### Task 2: Auth-seam для пагинации (Кандидат 2)

**Files:**
- Modify: `airflow_provider_avito/hooks/avito.py`
- Modify: `tests/test_hook.py`

- [x] добавить `_request_calls_page(offset, datetime_from)` с полным auth-lifecycle
      (кэш учёток/токена, refresh-on-401 + один повтор, raise на второй 401)
- [x] упростить `get_calls`: убрать добычу учёток/токена и inline-retry, оставить чистую пагинацию
- [x] переписать `test_token_refresh_on_401` и `test_token_refresh_second_error_raises`
      на прямой вызов `_request_calls_page` (или сохранить через `get_calls`, если так нагляднее)
- [x] тест кэша учёток: на N страницах `get_connection` (или `_get_credentials`)
      вызывается ровно один раз (защита от per-page lookup)
- [x] проверить, что `test_pagination_*`, `test_early_stop_*`, `test_filters_*` проходят
- [x] запустить `pytest` — все тесты зелёные

### Task 3: Единый список полей звонка (Кандидат 3)

**Files:**
- Modify: `airflow_provider_avito/hooks/avito.py`
- Modify: `airflow_provider_avito/operators/calls.py`
- Modify: `tests/test_hook.py` (при необходимости)

- [x] объявить `CALL_FIELDS: tuple[str, ...]` (17 имён) в `hooks/avito.py` рядом с
      `_map_record`, **в точном текущем порядке `_CSV_FIELDS`**
- [x] в `operators/calls.py` заменить литерал `_CSV_FIELDS` на `list(CALL_FIELDS)` (импорт из hook)
- [x] ужесточить тест-страж до **упорядоченного** сравнения: `tuple(record.keys()) == CALL_FIELDS`
      (текущий `test_csv_fields_match_mapped_record` сравнивает `set` — порядок не ловит)
- [x] проверить `test_full_record_17_fields` и тесты CSV в `tests/test_operator.py`
- [x] запустить `pytest` — все тесты зелёные

### Task 4: Verify acceptance criteria
- [ ] внешнее поведение не изменилось: возвраты, тексты ошибок, формат JSON/CSV
- [ ] секреты не присутствуют в результате `get_accounts`
- [ ] запустить полный `pytest`

### Task 5: [Final] Завершение
- [ ] обновить `CONTEXT.md`, если по ходу всплыли новые понятия
- [ ] переместить план в `docs/plans/completed/`

## Post-Completion
*Информационно, без чекбоксов*
- Рассмотреть те же три seam'а в соседнем `airflow-provider-cian` (общая логика).
