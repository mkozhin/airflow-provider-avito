from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

_RETRY_STATUSES = (429, 500, 502, 503, 504)
_BACKOFF_DELAYS = [1, 2, 4]
_PAGE_LIMIT = 1000


class _AvitoAuthError(Exception):
    """Raised by _make_request on HTTP 401 so the caller can refresh the token and retry."""


@dataclass
class Account:
    """Public identity of an Avito account (cabinet). No secrets.

    The `id` is sanitized on creation: any character outside ``[\\w-]`` is
    replaced with ``_``.
    """

    id: str

    def __post_init__(self) -> None:
        self.id = re.sub(r"[^\w-]", "_", self.id)


@dataclass
class AccountCredentials:
    """Internal credential holder for an Avito account. Contains secrets.

    ``id`` is sanitized on creation (same rule as :class:`Account`).
    ``id=None`` is used exclusively for the top-level single-form entry.
    """

    id: str | None
    client_id: str | None
    client_secret: str | None

    def __post_init__(self) -> None:
        if self.id is not None:
            self.id = re.sub(r"[^\w-]", "_", self.id)


@dataclass
class AvitoConnectionConfig:
    """Parsed representation of an Avito connection's ``extra`` field.

    ``accounts`` — list of multi-form entries (from the ``accounts`` array).
    ``single``   — top-level ``client_id``/``client_secret`` (single-account
                   form); ``None`` when neither key is present.
    """

    accounts: list[AccountCredentials]
    single: AccountCredentials | None


def parse_connection(extra: dict) -> AvitoConnectionConfig:
    """Parse ``connection.extra`` into :class:`AvitoConnectionConfig`.

    Best-effort: never raises.  Skips non-dict entries and entries missing
    ``id`` (emits WARNING).  Deduplicates by sanitized id, keeping the first
    occurrence (WARNING).  Top-level ``client_id``/``client_secret`` become
    ``single``.
    """
    raw_accounts = extra.get("accounts") or []
    accounts: list[AccountCredentials] = []
    seen: set[str] = set()

    for entry in raw_accounts:
        if not isinstance(entry, dict):
            log.warning("Skipping non-dict account entry: %r", entry)
            continue
        if "id" not in entry or entry["id"] is None:
            log.warning("Skipping account entry missing required 'id' key: %r", entry)
            continue
        original_id = entry["id"]
        ac = AccountCredentials(
            id=original_id,
            client_id=entry.get("client_id"),
            client_secret=entry.get("client_secret"),
        )
        if ac.id in seen:
            log.warning(
                "Duplicate account id after sanitization: %r becomes %r. "
                "Keeping the first, skipping the second.",
                original_id,
                ac.id,
            )
        else:
            seen.add(ac.id)  # type: ignore[arg-type]
            accounts.append(ac)

    client_id = extra.get("client_id")
    client_secret = extra.get("client_secret")
    single: AccountCredentials | None = (
        AccountCredentials(id=None, client_id=client_id, client_secret=client_secret)
        if client_id is not None or client_secret is not None
        else None
    )

    return AvitoConnectionConfig(accounts=accounts, single=single)


def get_accounts(conn_id: str) -> list[Account]:
    """Read accounts from the Airflow connection extra field.

    Returns a list of :class:`Account` objects (sanitized ids). Returns ``[]``
    on any error (missing connection, missing key, etc.) — callers must not
    raise.

    Duplicate sanitized ids are deduplicated: only the first account is kept
    and a WARNING is logged.  The top-level single-form entry (``client_id`` /
    ``client_secret`` without an ``accounts`` array) is **not** included.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        config = parse_connection(conn.extra_dejson)
        return [Account(id=ac.id) for ac in config.accounts if ac.id is not None]
    except Exception:
        log.warning(
            "Could not load accounts from connection %r. Returning empty list.",
            conn_id,
            exc_info=True,
        )
        return []


#: Canonical ordered tuple of call record field names.
#: This is the single source of truth for field names and their order.
#: ``AvitoHook._map_record`` returns a dict whose keys follow this order;
#: ``AvitoCallsOperator._CSV_FIELDS`` is derived from this tuple so that
#: CSV column order matches exactly.
CALL_FIELDS: tuple[str, ...] = (
    "id",
    "buyer_phone",
    "seller_phone",
    "virtual_phone",
    "create_time",
    "start_time",
    "date",
    "duration",
    "waiting_duration",
    "price",
    "price_rub",
    "status_id",
    "status",
    "item_id",
    "group_title",
    "is_arbitrage_available",
    "record_url",
)


class AvitoHook(BaseHook):
    conn_name_attr = "avito_conn_id"
    default_conn_name = "avito_default"
    conn_type = "http"
    hook_name = "Avito CPA API"

    def __init__(self, avito_conn_id: str = default_conn_name, account_id: str | None = None) -> None:
        super().__init__()
        self.avito_conn_id = avito_conn_id
        self.account_id = account_id
        self._token: str | None = None
        self._credentials: tuple[str, str] | None = None

    def _get_credentials(self) -> tuple[str, str]:
        """Return (client_id, client_secret) for the configured account."""
        conn = self.get_connection(self.avito_conn_id)
        config = parse_connection(conn.extra_dejson)

        if self.account_id is not None:
            for ac in config.accounts:
                if ac.id == self.account_id:
                    if not ac.client_id or not ac.client_secret:
                        raise AirflowException(
                            f"Account id={self.account_id!r} found in connection "
                            f"{self.avito_conn_id!r} but is missing required "
                            f"'client_id' or 'client_secret' fields"
                        )
                    return ac.client_id, ac.client_secret
            raise AirflowException(
                f"Account id={self.account_id!r} not found in connection "
                f"{self.avito_conn_id!r} extra.accounts"
            )

        if config.single is None or not config.single.client_id or not config.single.client_secret:
            raise AirflowException(
                f"Connection {self.avito_conn_id!r} extra is missing "
                f"'client_id' or 'client_secret' and no account_id was provided."
            )
        return config.single.client_id, config.single.client_secret

    def _fetch_token(self, client_id: str, client_secret: str) -> str:
        """Fetch a new OAuth2 access token using client_credentials grant."""
        url = "https://api.avito.ru/token/"
        try:
            resp = requests.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise AirflowException(f"Token request to {url} failed: {e}") from e

        if resp.status_code != 200:
            raise AirflowException(
                f"Avito token endpoint returned {resp.status_code}: {resp.text[:200]}"
            )

        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise AirflowException(
                f"Avito token response missing 'access_token': {resp.text[:200]}"
            )
        return token

    def _get_token(self, client_id: str, client_secret: str) -> str:
        """Return the cached token, fetching a new one if necessary."""
        if self._token is None:
            self._token = self._fetch_token(client_id, client_secret)
        return self._token

    def _make_request(self, token: str, offset: int, datetime_from: str) -> dict:
        """POST to callsByTime with retry on 429/5xx and _AvitoAuthError on 401.

        On HTTP-200 responses with a non-empty ``response["error"]`` field,
        raises AirflowException.
        """
        url = "https://api.avito.ru/cpa/v2/callsByTime"
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Source": "airflow-provider-avito",
        }
        body = {
            "dateTimeFrom": datetime_from,
            "limit": _PAGE_LIMIT,
            "offset": offset,
        }

        for attempt in range(len(_BACKOFF_DELAYS) + 1):
            try:
                resp = requests.post(url, json=body, headers=headers, timeout=30)
            except requests.RequestException as e:
                raise AirflowException(f"Request to {url} failed: {e}") from e

            if resp.status_code == 200:
                data = resp.json()
                error = data.get("error")
                if error:
                    raise AirflowException(
                        f"Avito API returned HTTP-200 with error: {error}"
                    )
                return data

            if resp.status_code == 401:
                raise _AvitoAuthError("Avito API returned 401 Unauthorized")

            if resp.status_code in _RETRY_STATUSES:
                if attempt < len(_BACKOFF_DELAYS):
                    time.sleep(_BACKOFF_DELAYS[attempt])
                    continue
                raise AirflowException(
                    f"Avito API returned {resp.status_code} for {url} (attempt {attempt + 1})"
                )

            raise AirflowException(
                f"Avito API error {resp.status_code} for {url}: {resp.text[:200]}"
            )

    # ------------------------------------------------------------------
    # Status mapping
    # ------------------------------------------------------------------

    _STATUS_MAP: dict[int, str] = {
        0: "Целевой",
        1: "На модерации",
        2: "Целевой после модерации",
        3: "Нецелевой после модерации",
    }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _map_record(self, raw: dict) -> dict:
        """Map a raw CallV2 dict to the canonical 17-field output dict."""
        price: int | None = raw.get("price")
        price_rub: float | None = (price / 100) if price is not None else None
        start_time: str | None = raw.get("startTime")
        date: str | None = start_time[:10] if start_time else None
        status_id: int | None = raw.get("statusId")
        if status_id is not None:
            status: str = self._STATUS_MAP.get(status_id, f"unknown_{status_id}")
        else:
            status = None  # type: ignore[assignment]
        raw_id = raw.get("id")
        raw_item_id = raw.get("itemId")
        return {
            "id": str(raw_id) if raw_id is not None else None,
            "buyer_phone": raw.get("buyerPhone"),
            "seller_phone": raw.get("sellerPhone"),
            "virtual_phone": raw.get("virtualPhone"),
            "create_time": raw.get("createTime"),
            "start_time": start_time,
            "date": date,
            "duration": raw.get("duration"),
            "waiting_duration": raw.get("waitingDuration"),
            "price": price,
            "price_rub": price_rub,
            "status_id": status_id,
            "status": status,
            "item_id": str(raw_item_id) if raw_item_id is not None else None,
            "group_title": raw.get("groupTitle"),
            "is_arbitrage_available": raw.get("isArbitrageAvailable"),
            "record_url": raw.get("recordUrl"),
        }

    def _request_calls_page(self, offset: int, datetime_from: str) -> dict:
        """Fetch one page from callsByTime, owning the full auth-lifecycle.

        Lazily caches credentials (``_get_credentials`` is called at most once
        per hook instance regardless of how many pages are fetched).  Caches the
        OAuth token via ``_get_token``.

        On HTTP 401 (``_AvitoAuthError``): resets ``self._token``, fetches a
        fresh token, and retries the request exactly once.  If the retry also
        returns 401 an ``AirflowException`` is raised.
        """
        if self._credentials is None:
            self._credentials = self._get_credentials()
        client_id, client_secret = self._credentials

        token = self._get_token(client_id, client_secret)
        try:
            return self._make_request(token, offset, datetime_from)
        except _AvitoAuthError:
            self._token = None
            new_token = self._fetch_token(client_id, client_secret)
            self._token = new_token
            try:
                return self._make_request(new_token, offset, datetime_from)
            except _AvitoAuthError as e:
                raise AirflowException("Avito API returned 401 after token refresh") from e

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_calls(self, date_from: str, date_to: str) -> list[dict]:
        """Fetch all calls in [date_from, date_to] with pagination.

        date_from / date_to are ``YYYY-MM-DD`` strings. The method compares
        the ``date`` field (``startTime[:10]``) of each record against this
        range.

        A ``time.sleep(62)`` is inserted **only** between pages (not after the
        last page) to respect the 1 req/min rate limit on this endpoint.

        Auth-lifecycle (credential caching, token refresh on 401) is fully
        delegated to :meth:`_request_calls_page`.
        """
        datetime_from = date_from + "T00:00:00+03:00"
        limit = _PAGE_LIMIT
        offset = 0
        result: list[dict] = []

        while True:
            data = self._request_calls_page(offset, datetime_from)

            payload = data.get("result", data)
            calls = payload.get("calls") or []

            if not calls:
                break

            # Early-stop: if every record with a startTime is past date_to,
            # there is nothing more to collect (API returns in asc order).
            # Guard against pages where no record has startTime (avoids vacuous all([]) = True).
            timed = [c for c in calls if c.get("startTime")]
            if timed and all(c["startTime"][:10] > date_to for c in timed):
                break

            for raw in calls:
                record = self._map_record(raw)
                if record["date"] is not None and date_from <= record["date"] <= date_to:
                    result.append(record)

            offset += limit
            # Sleep ONLY if we are going to fetch another page (full page received).
            if len(calls) >= limit:
                time.sleep(62)

        log.info("Collected %d calls for %s — %s", len(result), date_from, date_to)
        return result

    def test_connection(self) -> tuple[bool, str]:
        """Verify the connection by fetching an OAuth2 token.

        Does NOT call callsByTime so it doesn't consume the rate-limit quota.
        """
        try:
            client_id, client_secret = self._get_credentials()
            self._fetch_token(client_id, client_secret)
            return True, "Successfully obtained Avito access token"
        except Exception as exc:
            return False, str(exc)
