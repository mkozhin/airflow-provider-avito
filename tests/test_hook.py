from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_avito.hooks.avito import Account, AvitoHook, get_accounts, parse_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_hook(extra: dict | None = None) -> AvitoHook:
    hook = AvitoHook(avito_conn_id="avito_test")
    conn = Connection(
        conn_id="avito_test",
        conn_type="http",
        extra=json.dumps(extra or {"client_id": "cid", "client_secret": "csec"}),
    )
    hook.get_connection = MagicMock(return_value=conn)
    return hook


def _mock_response(status_code: int, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    resp.text = str(json_data)
    return resp


def _sample_call(
    call_id: int = 1,
    start_time: str = "2026-06-09T10:00:00+03:00",
    status_id: int = 0,
    price: int = 500,
) -> dict:
    return {
        "id": call_id,
        "buyerPhone": "+79001234567",
        "sellerPhone": "+79007654321",
        "virtualPhone": "+74951234567",
        "createTime": "2026-06-09T09:50:00+03:00",
        "startTime": start_time,
        "duration": 120,
        "waitingDuration": 3.5,
        "price": price,
        "statusId": status_id,
        "itemId": 42,
        "groupTitle": "Group A",
        "isArbitrageAvailable": True,
        "recordUrl": "https://example.com/record/1",
    }


# ---------------------------------------------------------------------------
# Test _get_credentials
# ---------------------------------------------------------------------------


class TestGetCredentials:
    def test_single_account(self):
        hook = _make_hook({"client_id": "X", "client_secret": "Y"})
        assert hook._get_credentials() == ("X", "Y")

    def test_multi_account(self):
        hook = AvitoHook(avito_conn_id="avito_test", account_id="acc1")
        conn = Connection(
            conn_id="avito_test",
            conn_type="http",
            extra=json.dumps(
                {
                    "accounts": [
                        {"id": "acc1", "client_id": "c1", "client_secret": "s1"},
                        {"id": "acc2", "client_id": "c2", "client_secret": "s2"},
                    ]
                }
            ),
        )
        hook.get_connection = MagicMock(return_value=conn)
        assert hook._get_credentials() == ("c1", "s1")

    def test_account_not_found(self):
        hook = AvitoHook(avito_conn_id="avito_test", account_id="missing")
        conn = Connection(
            conn_id="avito_test",
            conn_type="http",
            extra=json.dumps(
                {"accounts": [{"id": "acc1", "client_id": "c1", "client_secret": "s1"}]}
            ),
        )
        hook.get_connection = MagicMock(return_value=conn)
        with pytest.raises(AirflowException, match="missing"):
            hook._get_credentials()

    def test_missing_fields(self):
        hook = _make_hook({"some_other_key": "value"})
        with pytest.raises(AirflowException, match="client_id"):
            hook._get_credentials()


# ---------------------------------------------------------------------------
# Test _fetch_token
# ---------------------------------------------------------------------------


class TestFetchToken:
    def test_success(self):
        hook = _make_hook()
        resp = _mock_response(200, {"access_token": "tok123"})
        with patch("requests.post", return_value=resp):
            token = hook._fetch_token("cid", "csec")
        assert token == "tok123"

    def test_non_200_raises(self):
        hook = _make_hook()
        resp = _mock_response(400, {})
        with patch("requests.post", return_value=resp):
            with pytest.raises(AirflowException, match="400"):
                hook._fetch_token("cid", "csec")


# ---------------------------------------------------------------------------
# Test _get_token caching
# ---------------------------------------------------------------------------


class TestGetTokenCached:
    def test_fetches_only_once(self):
        hook = _make_hook()
        resp = _mock_response(200, {"access_token": "tok"})
        with patch("requests.post", return_value=resp) as mock_post:
            hook._get_token("cid", "csec")
            hook._get_token("cid", "csec")
        assert mock_post.call_count == 1


# ---------------------------------------------------------------------------
# Test _make_request retry
# ---------------------------------------------------------------------------


class TestMakeRequestRetry:
    def test_retries_on_503(self):
        hook = _make_hook()
        fail = _mock_response(503)
        ok = _mock_response(200, {"calls": [], "error": None})
        with patch("requests.post", side_effect=[fail, fail, ok]):
            with patch("time.sleep"):
                result = hook._make_request("tok", 0, "2026-06-09T00:00:00+03:00")
        assert result == {"calls": [], "error": None}

    def test_embedded_error_raises(self):
        hook = _make_hook()
        resp = _mock_response(200, {"calls": [], "error": {"code": 1002, "message": "bad"}})
        with patch("requests.post", return_value=resp):
            with pytest.raises(AirflowException, match="error"):
                hook._make_request("tok", 0, "2026-06-09T00:00:00+03:00")

    def test_nonretryable_4xx_raises(self):
        hook = _make_hook()
        resp = _mock_response(400)
        with patch("requests.post", return_value=resp):
            with pytest.raises(AirflowException):
                hook._make_request("tok", 0, "2026-06-09T00:00:00+03:00")

    def test_all_retries_exhausted_raises(self):
        hook = _make_hook()
        fail = _mock_response(503)
        with patch("requests.post", side_effect=[fail, fail, fail, fail]):
            with patch("time.sleep"):
                with pytest.raises(AirflowException, match="503"):
                    hook._make_request("tok", 0, "2026-06-09T00:00:00+03:00")

    def test_retries_on_429(self):
        hook = _make_hook()
        fail = _mock_response(429)
        ok = _mock_response(200, {"calls": [], "error": None})
        with patch("requests.post", side_effect=[fail, ok]):
            with patch("time.sleep"):
                result = hook._make_request("tok", 0, "2026-06-09T00:00:00+03:00")
        assert result == {"calls": [], "error": None}

    def test_request_body_and_headers(self):
        hook = _make_hook()
        ok = _mock_response(200, {"calls": [], "error": None})
        datetime_from = "2026-06-09T00:00:00+03:00"
        with patch("requests.post", return_value=ok) as mock_post:
            hook._make_request("mytoken", 500, datetime_from)
        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["json"] == {
            "dateTimeFrom": datetime_from,
            "limit": 1000,
            "offset": 500,
        }
        headers = call_kwargs.kwargs["headers"]
        assert headers["Authorization"] == "Bearer mytoken"
        assert headers["X-Source"] == "airflow-provider-avito"

    def test_sleep_backoff_delays(self):
        from airflow_provider_avito.hooks.avito import _BACKOFF_DELAYS

        hook = _make_hook()
        fails = [_mock_response(503)] * len(_BACKOFF_DELAYS)
        ok = _mock_response(200, {"calls": [], "error": None})
        with patch("requests.post", side_effect=fails + [ok]):
            with patch("time.sleep") as mock_sleep:
                hook._make_request("tok", 0, "2026-06-09T00:00:00+03:00")
        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert sleep_calls == _BACKOFF_DELAYS


# ---------------------------------------------------------------------------
# Test _fetch_token request body
# ---------------------------------------------------------------------------


class TestFetchTokenBody:
    def test_sends_grant_type_client_credentials(self):
        hook = _make_hook()
        resp = _mock_response(200, {"access_token": "tok"})
        with patch("requests.post", return_value=resp) as mock_post:
            hook._fetch_token("my_client_id", "my_client_secret")
        data = mock_post.call_args.kwargs["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "my_client_id"
        assert data["client_secret"] == "my_client_secret"


# ---------------------------------------------------------------------------
# Test _map_record
# ---------------------------------------------------------------------------


class TestMapRecord:
    def test_full_record_17_fields(self):
        hook = _make_hook()
        raw = _sample_call()
        record = hook._map_record(raw)
        assert len(record) == 17
        assert record["id"] == "1"
        assert record["buyer_phone"] == "+79001234567"
        assert record["seller_phone"] == "+79007654321"
        assert record["virtual_phone"] == "+74951234567"
        assert record["create_time"] == "2026-06-09T09:50:00+03:00"
        assert record["start_time"] == "2026-06-09T10:00:00+03:00"
        assert record["date"] == "2026-06-09"
        assert record["duration"] == 120
        assert record["waiting_duration"] == 3.5
        assert record["price"] == 500
        assert record["price_rub"] == 5.0
        assert record["status_id"] == 0
        assert record["status"] == "Целевой"
        assert record["item_id"] == "42"
        assert record["group_title"] == "Group A"
        assert record["is_arbitrage_available"] is True
        assert record["record_url"] == "https://example.com/record/1"

    def test_status_map_all_values(self):
        hook = _make_hook()
        expected = {
            0: "Целевой",
            1: "На модерации",
            2: "Целевой после модерации",
            3: "Нецелевой после модерации",
        }
        for status_id, label in expected.items():
            raw = _sample_call(status_id=status_id)
            record = hook._map_record(raw)
            assert record["status"] == label

    def test_unknown_status_id_fallback(self):
        hook = _make_hook()
        raw = _sample_call(status_id=99)
        record = hook._map_record(raw)
        assert record["status"] == "unknown_99"

    def test_missing_fields_no_exception(self):
        hook = _make_hook()
        record = hook._map_record({})
        assert record["id"] is None
        assert record["price_rub"] is None
        assert record["date"] is None
        assert record["status"] is None


# ---------------------------------------------------------------------------
# Test get_calls pagination
# ---------------------------------------------------------------------------


class TestGetCalls:
    def _make_hook_with_token(self) -> AvitoHook:
        hook = _make_hook()
        hook._token = "tok"
        hook._get_credentials = MagicMock(return_value=("cid", "csec"))
        return hook

    def test_nested_result_format(self):
        """API returns {'result': {'calls': [...]}} — real Avito response structure."""
        hook = self._make_hook_with_token()
        page1 = {"result": {"calls": [_sample_call(1, "2026-06-09T10:00:00+03:00")]}}
        page2 = {"result": {"calls": []}}

        with patch.object(hook, "_make_request", side_effect=[page1, page2]):
            with patch("time.sleep"):
                result = hook.get_calls("2026-06-09", "2026-06-09")

        assert len(result) == 1
        assert result[0]["id"] == "1"

    def test_single_page(self):
        hook = self._make_hook_with_token()
        page1 = {"calls": [_sample_call(1, "2026-06-09T10:00:00+03:00")], "error": None}
        page2 = {"calls": [], "error": None}

        with patch.object(hook, "_make_request", side_effect=[page1, page2]):
            with patch("time.sleep"):
                result = hook.get_calls("2026-06-09", "2026-06-09")

        assert len(result) == 1
        assert result[0]["id"] == "1"

    def test_pagination_two_pages(self):
        hook = self._make_hook_with_token()
        calls_page1 = [_sample_call(i, "2026-06-09T10:00:00+03:00") for i in range(1000)]
        page1 = {"calls": calls_page1, "error": None}
        calls_page2 = [_sample_call(1001, "2026-06-09T11:00:00+03:00")]
        page2 = {"calls": calls_page2, "error": None}
        page3 = {"calls": [], "error": None}

        with patch.object(hook, "_make_request", side_effect=[page1, page2, page3]):
            with patch("time.sleep") as mock_sleep:
                result = hook.get_calls("2026-06-09", "2026-06-09")

        assert len(result) == 1001
        # Sleep only called between full pages (page1→page2), not after partial page2
        assert mock_sleep.call_count == 1

    def test_early_stop_past_date_to(self):
        hook = self._make_hook_with_token()
        # All calls are past date_to
        page1 = {
            "calls": [_sample_call(1, "2026-06-11T10:00:00+03:00")],
            "error": None,
        }

        with patch.object(hook, "_make_request", side_effect=[page1]):
            with patch("time.sleep") as mock_sleep:
                result = hook.get_calls("2026-06-09", "2026-06-09")

        assert result == []
        mock_sleep.assert_not_called()

    def test_filters_calls_outside_range(self):
        hook = self._make_hook_with_token()
        page1 = {
            "calls": [
                _sample_call(1, "2026-06-08T10:00:00+03:00"),  # before range
                _sample_call(2, "2026-06-09T10:00:00+03:00"),  # in range
                _sample_call(3, "2026-06-10T10:00:00+03:00"),  # after range (not early stop since mixed)
            ],
            "error": None,
        }
        page2 = {"calls": [], "error": None}

        with patch.object(hook, "_make_request", side_effect=[page1, page2]):
            with patch("time.sleep"):
                result = hook.get_calls("2026-06-09", "2026-06-09")

        assert len(result) == 1
        assert result[0]["id"] == "2"

    def test_token_refresh_on_401(self):
        from airflow_provider_avito.hooks.avito import _AvitoAuthError

        hook = self._make_hook_with_token()
        hook._fetch_token = MagicMock(return_value="new_tok")

        ok_page = {"calls": [_sample_call(1, "2026-06-09T10:00:00+03:00")], "error": None}
        empty_page = {"calls": [], "error": None}

        with patch.object(
            hook,
            "_make_request",
            side_effect=[_AvitoAuthError("401"), ok_page, empty_page],
        ):
            with patch("time.sleep"):
                result = hook.get_calls("2026-06-09", "2026-06-09")

        assert len(result) == 1
        assert hook._token == "new_tok"
        hook._fetch_token.assert_called_once_with("cid", "csec")

    def test_token_refresh_second_error_raises(self):
        from airflow_provider_avito.hooks.avito import _AvitoAuthError

        hook = self._make_hook_with_token()
        hook._fetch_token = MagicMock(return_value="new_tok")

        with patch.object(
            hook,
            "_make_request",
            side_effect=[_AvitoAuthError("401"), _AvitoAuthError("401 again")],
        ):
            with patch("time.sleep"):
                with pytest.raises(AirflowException, match="after token refresh"):
                    hook.get_calls("2026-06-09", "2026-06-09")

    def test_page_without_start_time_does_not_trigger_early_stop(self):
        """A page where no record has startTime must NOT cause early-break (vacuous truth guard)."""
        hook = self._make_hook_with_token()
        # Page has records but none have startTime — must not early-break
        page1 = {"calls": [{"id": 1}, {"id": 2}], "error": None}
        page2 = {
            "calls": [_sample_call(3, "2026-06-09T10:00:00+03:00")],
            "error": None,
        }
        page3 = {"calls": [], "error": None}

        with patch.object(hook, "_make_request", side_effect=[page1, page2, page3]):
            with patch("time.sleep"):
                result = hook.get_calls("2026-06-09", "2026-06-09")

        # Record 3 (from page2) is in range and must be collected
        assert any(r["id"] == "3" for r in result)


# ---------------------------------------------------------------------------
# Test test_connection
# ---------------------------------------------------------------------------


class TestTestConnection:
    def test_success(self):
        hook = _make_hook()
        with patch.object(hook, "_get_credentials", return_value=("cid", "csec")):
            with patch.object(hook, "_fetch_token", return_value="tok"):
                ok, msg = hook.test_connection()
        assert ok is True
        assert "token" in msg.lower()

    def test_failure(self):
        hook = _make_hook()
        with patch.object(hook, "_get_credentials", side_effect=AirflowException("no creds")):
            ok, msg = hook.test_connection()
        assert ok is False
        assert "no creds" in msg


# ---------------------------------------------------------------------------
# Test get_accounts
# ---------------------------------------------------------------------------


class TestGetAccounts:
    def test_returns_accounts(self):
        conn = Connection(
            conn_id="avito_test",
            conn_type="http",
            extra=json.dumps(
                {
                    "accounts": [
                        {"id": "a1", "client_id": "c1", "client_secret": "s1"},
                        {"id": "a2", "client_id": "c2", "client_secret": "s2"},
                    ]
                }
            ),
        )
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("avito_test")
        assert len(result) == 2
        assert result[0].id == "a1"

    def test_deduplicates_sanitized_ids(self):
        conn = Connection(
            conn_id="avito_test",
            conn_type="http",
            extra=json.dumps(
                {"accounts": [{"id": "a.b"}, {"id": "a/b"}]}
            ),
        )
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("avito_test")
        assert len(result) == 1
        assert result[0].id == "a_b"

    def test_returns_empty_on_error(self):
        from airflow.exceptions import AirflowNotFoundException

        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=AirflowNotFoundException("not found"),
        ):
            result = get_accounts("nonexistent")
        assert result == []

    def test_single_form_returns_empty_list(self):
        """get_accounts for single-form extra returns [] (single entry not exposed)."""
        conn = Connection(
            conn_id="avito_test",
            conn_type="http",
            extra=json.dumps({"client_id": "cid", "client_secret": "csec"}),
        )
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("avito_test")
        assert result == []

    def test_malform_non_dict_entry(self):
        """Non-dict entries in accounts are skipped; get_accounts returns []."""
        conn = Connection(
            conn_id="avito_test",
            conn_type="http",
            extra=json.dumps({"accounts": ["not-a-dict", 42]}),
        )
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("avito_test")
        assert result == []


# ---------------------------------------------------------------------------
# Test parse_connection
# ---------------------------------------------------------------------------


class TestParseConnection:
    def test_single_form(self):
        """Top-level client_id/client_secret → single; accounts list is empty."""
        config = parse_connection({"client_id": "cid", "client_secret": "csec"})
        assert config.accounts == []
        assert config.single is not None
        assert config.single.id is None
        assert config.single.client_id == "cid"
        assert config.single.client_secret == "csec"

    def test_multi_form(self):
        """Multi-account form populates accounts list."""
        extra = {
            "accounts": [
                {"id": "acc1", "client_id": "c1", "client_secret": "s1"},
                {"id": "acc2", "client_id": "c2", "client_secret": "s2"},
            ]
        }
        config = parse_connection(extra)
        assert len(config.accounts) == 2
        assert config.accounts[0].id == "acc1"
        assert config.accounts[0].client_id == "c1"
        assert config.accounts[1].id == "acc2"
        assert config.single is None

    def test_entry_without_id_skipped(self):
        """Entry missing 'id' key is skipped; valid entries remain."""
        extra = {
            "accounts": [
                {"client_id": "c1", "client_secret": "s1"},  # no id
                {"id": "acc2", "client_id": "c2", "client_secret": "s2"},
            ]
        }
        config = parse_connection(extra)
        assert len(config.accounts) == 1
        assert config.accounts[0].id == "acc2"

    def test_dedup_by_sanitized_id(self):
        """Two entries that map to the same sanitized id: first kept, second dropped."""
        extra = {
            "accounts": [
                {"id": "a.b", "client_id": "c1", "client_secret": "s1"},
                {"id": "a/b", "client_id": "c2", "client_secret": "s2"},
            ]
        }
        config = parse_connection(extra)
        assert len(config.accounts) == 1
        assert config.accounts[0].id == "a_b"
        assert config.accounts[0].client_id == "c1"

    def test_account_without_secrets_included(self):
        """Entry with id but no secrets is kept in accounts (policy belongs to caller)."""
        extra = {"accounts": [{"id": "acc1"}]}
        config = parse_connection(extra)
        assert len(config.accounts) == 1
        assert config.accounts[0].id == "acc1"
        assert config.accounts[0].client_id is None
        assert config.accounts[0].client_secret is None

    def test_empty_extra(self):
        """Empty extra dict → empty accounts and no single."""
        config = parse_connection({})
        assert config.accounts == []
        assert config.single is None

    def test_id_sanitization(self):
        """Special chars in id are replaced with underscore."""
        extra = {"accounts": [{"id": "a.b.c", "client_id": "c1", "client_secret": "s1"}]}
        config = parse_connection(extra)
        assert config.accounts[0].id == "a_b_c"

    def test_non_dict_entry_skipped(self):
        """Non-dict entries in accounts list are skipped without raising."""
        extra = {"accounts": ["bad-entry", 42, None, {"id": "ok", "client_id": "c", "client_secret": "s"}]}
        config = parse_connection(extra)
        assert len(config.accounts) == 1
        assert config.accounts[0].id == "ok"


# ---------------------------------------------------------------------------
# Test _get_credentials sanitization regression
# ---------------------------------------------------------------------------


class TestGetCredentialsSanitization:
    def test_sanitized_id_matches_sanitized_account_id(self):
        """account_id='a_b' (already sanitized) matches entry with id='a.b'."""
        hook = AvitoHook(avito_conn_id="avito_test", account_id="a_b")
        conn = Connection(
            conn_id="avito_test",
            conn_type="http",
            extra=json.dumps(
                {"accounts": [{"id": "a.b", "client_id": "c1", "client_secret": "s1"}]}
            ),
        )
        hook.get_connection = MagicMock(return_value=conn)
        assert hook._get_credentials() == ("c1", "s1")
