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
    """Represents an Avito account (cabinet). The `id` is sanitized on creation."""

    id: str

    def __post_init__(self) -> None:
        self.id = re.sub(r"[^\w-]", "_", self.id)


def get_accounts(conn_id: str) -> list[Account]:
    """Read accounts from the Airflow connection extra field.

    Returns a list of Account objects (sanitized ids). Returns [] on any error
    (missing connection, missing key, etc.) — callers must not raise.

    Duplicate sanitized ids are deduplicated: only the first account is kept
    and a WARNING is logged.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        raw_accounts = conn.extra_dejson.get("accounts", [])
        accounts: list[Account] = []
        seen: set[str] = set()
        for entry in raw_accounts:
            if "id" not in entry:
                log.warning("Skipping account entry missing required 'id' key: %r", entry)
                continue
            original_id = entry["id"]
            acc = Account(id=original_id)
            if acc.id in seen:
                log.warning(
                    "Duplicate account id after sanitization: %r becomes %r. "
                    "Keeping the first, skipping the second.",
                    original_id,
                    acc.id,
                )
            else:
                seen.add(acc.id)
                accounts.append(acc)
        return accounts
    except Exception:
        log.warning(
            "Could not load accounts from connection %r. Returning empty list.",
            conn_id,
            exc_info=True,
        )
        return []


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

    def _get_credentials(self) -> tuple[str, str]:
        """Return (client_id, client_secret) for the configured account."""
        conn = self.get_connection(self.avito_conn_id)
        extra = conn.extra_dejson

        if self.account_id is not None:
            raw_accounts = extra.get("accounts", [])
            for entry in raw_accounts:
                if "id" not in entry:
                    continue
                if Account(id=entry["id"]).id == self.account_id:
                    client_id = entry.get("client_id")
                    client_secret = entry.get("client_secret")
                    if not client_id or not client_secret:
                        raise AirflowException(
                            f"Account id={self.account_id!r} found in connection "
                            f"{self.avito_conn_id!r} but is missing required "
                            f"'client_id' or 'client_secret' fields"
                        )
                    return client_id, client_secret
            raise AirflowException(
                f"Account id={self.account_id!r} not found in connection "
                f"{self.avito_conn_id!r} extra.accounts"
            )

        client_id = extra.get("client_id")
        client_secret = extra.get("client_secret")
        if not client_id or not client_secret:
            raise AirflowException(
                f"Connection {self.avito_conn_id!r} extra is missing "
                f"'client_id' or 'client_secret' and no account_id was provided."
            )
        return client_id, client_secret

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

    def _make_request(self, token: str, offset: int, datetime_from: str, datetime_to: str) -> dict:
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
            "dateTimeTo": datetime_to,
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
        """
        client_id, client_secret = self._get_credentials()
        token = self._get_token(client_id, client_secret)
        datetime_from = date_from + "T00:00:00+03:00"
        datetime_to = date_to + "T23:59:59+03:00"
        limit = _PAGE_LIMIT
        offset = 0
        result: list[dict] = []

        while True:
            try:
                data = self._make_request(token, offset, datetime_from, datetime_to)
            except _AvitoAuthError:
                # Reset cached token and retry exactly once.
                self._token = None
                token = self._fetch_token(client_id, client_secret)
                self._token = token
                try:
                    data = self._make_request(token, offset, datetime_from, datetime_to)
                except _AvitoAuthError as e:
                    raise AirflowException("Avito API returned 401 after token refresh") from e

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
