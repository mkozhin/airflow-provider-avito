# airflow-provider-avito

Apache Airflow провайдер для [Avito CPA](https://developers.avito.ru/api-catalog/cpa/documentation) — сбор статистики звонков с рекламной платформы Авито.

---

*Powered by [Claude Code](https://claude.ai/code)*

---

## Установка

```bash
pip install airflow-provider-avito
```

Требуется Python 3.10+ и `apache-airflow>=2.9.1`.

## Настройка подключения

Создайте Airflow connection типа **HTTP** с `conn_id = avito_default` (или любым именем, которое вы передаёте в оператор).

Используется только поле **Extra**. Поля `login` и `password` игнорируются.

### Один кабинет

```json
{
  "client_id": "ваш_client_id",
  "client_secret": "ваш_client_secret"
}
```

### Несколько кабинетов

```json
{
  "accounts": [
    {"id": "main",    "client_id": "id1", "client_secret": "secret1"},
    {"id": "agency",  "client_id": "id2", "client_secret": "secret2"}
  ]
}
```

Параметр `account_id` оператора указывает, какой кабинет использовать.

## Быстрый старт

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
        output_format="json",   # или "csv"
        add_snapshot_ts=True,   # опционально, см. «Версионирование снапшотов» ниже
    )

avito_calls_example()
```

Оператор записывает один JSONL (или CSV) файл на каждую дату в `{base_dir}/{safe_run_id}/{date}.json` и возвращает `list[dict]` с записями вида `{"date": ..., "path": ..., "snapshot_ts": ...}` (`snapshot_ts` равен `None`, если `add_snapshot_ts=False`).

### Версионирование снапшотов (`add_snapshot_ts`)

По умолчанию каждый запуск DAG пишет в один и тот же путь на дату, поэтому повторный запуск перезаписывает предыдущие данные и история изменений статуса звонка теряется.

Параметр `add_snapshot_ts=True` добавляет поле `snapshot_ts` — `start_date` запуска DAG (реальное wall-clock UTC время старта прогона) в формате `YYYY-MM-DDTHH:MM:SS` — в каждую JSON-запись и в возвращаемый оператором ключ `snapshot_ts`. Это позволяет даунстрим-таску строить уникальный, не перезаписывающий путь на каждый запуск (например, ключ в S3 с суффиксом снапшота) и делать запросы в ClickHouse/Spark, выбирающие последнюю версию или историю изменений статуса:

```sql
-- ClickHouse: только последний снапшот
SELECT * FROM s3('s3://bucket/prefix/**/*.json', 'JSONEachRow')
WHERE toDateTime(snapshot_ts) = (
    SELECT MAX(toDateTime(snapshot_ts)) FROM s3('s3://bucket/prefix/**/*.json', 'JSONEachRow')
)
```

`add_snapshot_ts` применяется только к `output_format="json"`; при `output_format="csv"` параметр игнорируется (схема колонок CSV фиксирована).

## Схема записи

Каждая запись содержит 17 полей:

| Поле | Тип | Описание |
|---|---|---|
| `id` | int | Идентификатор звонка |
| `buyer_phone` | str | Телефон покупателя |
| `seller_phone` | str | Телефон продавца |
| `virtual_phone` | str | Виртуальный (подменный) номер |
| `create_time` | str | Время создания (RFC3339) |
| `start_time` | str | Время начала звонка (RFC3339) |
| `date` | str | Дата (YYYY-MM-DD), извлечённая из `start_time` |
| `duration` | int | Длительность звонка, секунды |
| `waiting_duration` | float | Время ожидания ответа, секунды |
| `price` | int | Цена в копейках |
| `price_rub` | float | Цена в рублях (`price / 100`) |
| `status_id` | int | Код статуса |
| `status` | str | Текстовый статус (например, "Целевой") |
| `item_id` | int | Идентификатор объявления |
| `group_title` | str | Название кампании |
| `is_arbitrage_available` | bool | Доступен ли арбитраж |
| `record_url` | str | Ссылка на запись разговора |

При `add_snapshot_ts=True` и `output_format="json"` в каждую запись добавляется 18-е поле:

| Поле | Тип | Описание |
|---|---|---|
| `snapshot_ts` | str | `start_date` запуска DAG, ISO 8601 (`YYYY-MM-DDTHH:MM:SS`). Присутствует только при `add_snapshot_ts=True` и `output_format="json"`. |

### Статусы звонков

| `status_id` | `status` |
|---|---|
| 0 | Целевой |
| 1 | На модерации |
| 2 | Целевой после модерации |
| 3 | Нецелевой после модерации |

## Примеры

Полные production-примеры с загрузкой в BigQuery + S3 находятся в [`examples/`](examples/):

- [`bq_and_s3_multi_account_dag.py`](examples/bq_and_s3_multi_account_dag.py) — несколько кабинетов параллельно

## Лицензия

MIT
