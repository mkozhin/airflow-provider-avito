# Domain & Architecture Context — airflow-provider-avito

## Domain concepts

| Term | Definition |
|---|---|
| **Call** (звонок) | A single CPA phone call record returned by the Avito `callsByTime` API. Canonical representation: 17-field dict produced by `AvitoHook._map_record`. |
| **Account / Cabinet** (кабинет) | An Avito advertiser account identified by a sanitised string id. Public identity only — no secrets. Represented by `Account(id)`. |
| **connection.extra** | JSON stored in the Airflow connection's *Extra* field. Two valid shapes: *single-form* (top-level `client_id` / `client_secret`) and *multi-form* (`accounts: [{id, client_id, client_secret}, ...]`). |
| **single-form** | `connection.extra` with top-level `client_id` / `client_secret` and no `accounts` key. Used when a single Avito account is sufficient. |
| **multi-form** | `connection.extra` with an `accounts` array. Each entry must have `id`; `client_id` / `client_secret` are required for actual API calls. |

## Key seams (architectural boundaries)

### `parse_connection(extra: dict) -> AvitoConnectionConfig`
Module-level function in `hooks/avito.py`. Single point of knowledge about
`connection.extra` format. Best-effort (never raises): skips malformed entries,
sanitises account ids, deduplicates. Produces:
- `AvitoConnectionConfig.accounts` — list of `AccountCredentials` (multi-form entries)
- `AvitoConnectionConfig.single` — `AccountCredentials | None` (top-level single-form)

Callers own policy (raising vs. returning empty).

### `AccountCredentials` (internal)
Dataclass holding `id | None`, `client_id | None`, `client_secret | None`.
`id` is sanitised on creation. `id=None` exclusively for `single`.
**Never returned to external callers** — secrets must not leak.

### `Account` (public)
Dataclass holding sanitised `id: str` only. Returned by `get_accounts`, used
as keys in DAG/operator logic. No secrets.

## Id sanitisation rule
`re.sub(r"[^\w-]", "_", raw_id)` — any character outside `[A-Za-z0-9_-]` is
replaced with `_`. Applied in both `Account.__post_init__` and
`AccountCredentials.__post_init__`. Matching in `_get_credentials` is always
sanitised-to-sanitised.

### `AvitoHook._request_calls_page(offset, datetime_from) -> dict`
Private method that owns the full **auth-lifecycle** for one callsByTime page:
- Lazily caches `_credentials` — `_get_credentials` is called at most once per
  hook instance regardless of page count.
- Caches the OAuth token via `_get_token`.
- On HTTP 401 (`_AvitoAuthError`): resets `self._token`, fetches a fresh token,
  retries exactly once. Second 401 → `AirflowException`.

`get_calls` delegates all auth concerns here; its loop is pure pagination.

### `CALL_FIELDS: tuple[str, ...]`
Module-level constant in `hooks/avito.py`. Canonical ordered tuple of the 17
call-record field names. Single source of truth shared across:
- `AvitoHook._map_record` — keys of the returned dict follow this order.
- `AvitoCallsOperator._CSV_FIELDS` — derived as `list(CALL_FIELDS)` so CSV
  column order matches exactly.

## Error policies
| Call-site | Policy |
|---|---|
| `get_accounts` | catch-all → return `[]` (callers must tolerate empty list) |
| `_get_credentials` | raise `AirflowException` with a message containing `'client_id'` (missing creds) or the `account_id` value (not found / missing) |
| `parse_connection` | never raises; logs WARNINGs for skipped entries |
| `_request_calls_page` | raises `AirflowException` on second consecutive 401; other HTTP errors propagate from `_make_request` |
