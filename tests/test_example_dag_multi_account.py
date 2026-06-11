"""Smoke tests: multi-account example DAG imports without errors."""

from __future__ import annotations

import importlib
import sys
import types
from unittest.mock import MagicMock, patch

from airflow_provider_avito.hooks.avito import Account

_MOD_NAME = "examples.bq_and_s3_multi_account_dag"
# The DAG module calls get_accounts at import time (module-level code inside the @dag body runs
# when the trailing `avito_to_bq_and_s3_multi_account()` call executes the decorated function).
# Patching the source function ensures the mock is bound before import.
_PATCH_TARGET = "airflow_provider_avito.hooks.avito.get_accounts"

# Provider sub-packages not installed in the test environment.
# NOTE: do NOT include "airflow.providers" — it is a real namespace package.
_MISSING_PROVIDERS = [
    "airflow.providers.amazon",
    "airflow.providers.amazon.aws",
    "airflow.providers.amazon.aws.transfers",
    "airflow.providers.amazon.aws.transfers.local_to_s3",
    "airflow.providers.google",
    "airflow.providers.google.cloud",
    "airflow.providers.google.cloud.hooks",
    "airflow.providers.google.cloud.hooks.gcs",
    "airflow.providers.google.cloud.transfers",
    "airflow.providers.google.cloud.transfers.gcs_to_bigquery",
    "airflow.providers.google.cloud.transfers.local_to_gcs",
]


def _make_provider_stubs() -> dict[str, types.ModuleType]:
    """Build a dict of stub modules to inject into sys.modules."""
    stubs: dict[str, types.ModuleType] = {}

    for name in _MISSING_PROVIDERS:
        if name in sys.modules:
            continue
        mod = types.ModuleType(name)
        stubs[name] = mod

    def _get(name: str) -> types.ModuleType:
        return stubs.get(name, sys.modules.get(name, types.ModuleType(name)))

    gcs_mod = _get("airflow.providers.google.cloud.hooks.gcs")
    gcs_mod.GCSHook = MagicMock(name="GCSHook")

    gcs_to_bq_mod = _get("airflow.providers.google.cloud.transfers.gcs_to_bigquery")
    GCSToBQ = MagicMock(name="GCSToBigQueryOperator")
    GCSToBQ.partial = MagicMock(return_value=MagicMock())
    gcs_to_bq_mod.GCSToBigQueryOperator = GCSToBQ

    local_to_gcs_mod = _get("airflow.providers.google.cloud.transfers.local_to_gcs")
    LocalToGCS = MagicMock(name="LocalFilesystemToGCSOperator")
    LocalToGCS.partial = MagicMock(return_value=MagicMock())
    local_to_gcs_mod.LocalFilesystemToGCSOperator = LocalToGCS

    s3_mod = _get("airflow.providers.amazon.aws.transfers.local_to_s3")
    LocalToS3 = MagicMock(name="LocalFilesystemToS3Operator")
    LocalToS3.partial = MagicMock(return_value=MagicMock())
    s3_mod.LocalFilesystemToS3Operator = LocalToS3

    return stubs


def _import_dag_module(mock_accounts: list[Account]):
    """Import (or re-import) the multi-account DAG module with mocked providers
    and a mocked get_accounts function.

    Removes the cached module so that module-level code re-runs each time.
    """
    sys.modules.pop(_MOD_NAME, None)

    stubs = _make_provider_stubs()
    previously_absent = [k for k in stubs if k not in sys.modules]
    sys.modules.update(stubs)

    try:
        with patch(_PATCH_TARGET, return_value=mock_accounts):
            mod = importlib.import_module(_MOD_NAME)
    finally:
        for k in previously_absent:
            sys.modules.pop(k, None)

    return mod


def _get_dag_obj(mod):
    """Return the actual DAG object from a @dag-decorated callable."""
    decorated = mod.avito_to_bq_and_s3_multi_account
    if hasattr(decorated, "dag"):
        return decorated.dag
    return decorated()


class TestMultiAccountDagImport:
    """Smoke tests: DAG imports without errors."""

    def test_dag_imports_with_empty_accounts(self):
        """DAG imports successfully when get_accounts returns []."""
        mod = _import_dag_module([])
        assert hasattr(mod, "avito_to_bq_and_s3_multi_account")

    def test_dag_imports_with_two_accounts(self):
        """DAG imports successfully when get_accounts returns two accounts."""
        accounts = [Account(id="aa"), Account(id="bb")]
        mod = _import_dag_module(accounts)
        assert hasattr(mod, "avito_to_bq_and_s3_multi_account")

    def test_dag_imports_does_not_raise_on_empty_connection(self):
        """DAG import must not raise when get_accounts returns [] (empty/missing connection)."""
        mod = _import_dag_module([])
        assert mod.avito_to_bq_and_s3_multi_account is not None

    def test_bq_schema_has_17_fields(self):
        """BQ_SCHEMA must contain exactly 17 fields."""
        mod = _import_dag_module([])
        assert len(mod.BQ_SCHEMA) == 17


class TestMultiAccountDagTaskGroups:
    """Verify that TaskGroups are created for each account."""

    def test_no_task_groups_when_accounts_empty(self):
        """When accounts is [], the DAG body has no cabinet_* TaskGroups."""
        mod = _import_dag_module([])
        dag_obj = _get_dag_obj(mod)
        task_group_ids = set(dag_obj.task_group_dict.keys())
        cabinet_groups = [tgid for tgid in task_group_ids if tgid.startswith("cabinet_")]
        assert cabinet_groups == []

    def test_two_task_groups_for_two_accounts(self):
        """When two accounts are present, two TaskGroups are created."""
        accounts = [Account(id="aa"), Account(id="bb")]
        mod = _import_dag_module(accounts)
        dag_obj = _get_dag_obj(mod)

        task_group_ids = set(dag_obj.task_group_dict.keys())
        assert "cabinet_aa" in task_group_ids
        assert "cabinet_bb" in task_group_ids

    def test_task_group_ids_match_sanitized_account_ids(self):
        """TaskGroup ids use sanitized account ids."""
        accounts = [Account(id="a.b"), Account(id="c/d")]
        mod = _import_dag_module(accounts)
        dag_obj = _get_dag_obj(mod)

        task_group_ids = set(dag_obj.task_group_dict.keys())
        assert "cabinet_a_b" in task_group_ids
        assert "cabinet_c_d" in task_group_ids
