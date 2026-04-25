import importlib.util
import sys
import types
from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch


def _load_cdc_snapshot_module():
    # Minimal dependency stubs needed to import dataflow.cdc_snapshot.
    pyspark = types.ModuleType("pyspark")
    pyspark.pipelines = types.SimpleNamespace(
        create_auto_cdc_from_snapshot_flow=lambda **_: None
    )
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.DataFrame = type("DataFrame", (), {})
    pyspark_sql.functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql.types = types.ModuleType("pyspark.sql.types")
    pyspark_sql.types.TimestampType = type("TimestampType", (), {})
    pyspark_sql.types.IntegerType = type("IntegerType", (), {})
    pyspark_sql.types.DataType = type("DataType", (), {})
    pyspark.sql = pyspark_sql

    src_root = Path(__file__).resolve().parents[1] / "src"
    dataflow_root = src_root / "dataflow"

    dataflow_pkg = types.ModuleType("dataflow")
    dataflow_pkg.__path__ = [str(dataflow_root)]

    class _SourceStub:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def read_source(self, _):
            return object()

    class _ReadConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    module_path = dataflow_root / "cdc_snapshot.py"
    fake_modules = {
        "pyspark": pyspark,
        "pyspark.sql": pyspark_sql,
        "pyspark.sql.functions": pyspark_sql.functions,
        "pyspark.sql.types": pyspark_sql.types,
        "pipeline_config": types.SimpleNamespace(
            get_logger=lambda: MagicMock(),
            get_dbutils=lambda: None,
            get_spark=lambda: None,
            get_pipeline_details=lambda: types.SimpleNamespace(pipeline_catalog="test_catalog"),
        ),
        "dataflow": dataflow_pkg,
        "dataflow.dataflow_config": types.SimpleNamespace(
            DataFlowConfig=type("DataFlowConfig", (), {})
        ),
        "dataflow.sources": types.SimpleNamespace(
            SourceDelta=_SourceStub,
            SourceBatchFiles=_SourceStub,
            ReadConfig=_ReadConfig,
        ),
    }

    with patch.dict(sys.modules, fake_modules, clear=False):
        spec = importlib.util.spec_from_file_location("dataflow.cdc_snapshot", module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules["dataflow.cdc_snapshot"] = module
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module


def test_historical_table_source_two_part_identifiers():
    # Tests to ensure two part identifiers are correctly handled
    mod = _load_cdc_snapshot_module()
    source_delta = MagicMock()
    source_delta.return_value.read_source.return_value = object()
    mod.SourceDelta = source_delta

    settings = mod.CDCSnapshotSettings(
        keys=["id"],
        scd_type="1",
        snapshotType=mod.CDCSnapshotTypes.HISTORICAL,
        sourceType=mod.CDCSnapshotSourceTypes.TABLE,
        source={
            "table": "staging.customer",
            "versionColumn": "version_col",
            "versionType": mod.CDCSnapshotVersionTypes.INTEGER,
        },
    )
    flow = mod.CDCSnapshotFlow(settings)

    class _DummyDataflowConfig:
        features = {}

    version_info = mod.VersionInfo(raw_value=1, version_type=mod.CDCSnapshotVersionTypes.INTEGER)

    flow._read_snapshot_dataframe(version_info, _DummyDataflowConfig())

    kwargs = source_delta.call_args.kwargs
    assert kwargs["database"] == "test_catalog.staging"
    assert kwargs["table"] == "customer"

def test_historical_table_source_three_part_identifiers():
    # Tests to ensure three part identifiers are correctly handled
    mod = _load_cdc_snapshot_module()
    source_delta = MagicMock()
    source_delta.return_value.read_source.return_value = object()
    mod.SourceDelta = source_delta

    settings = mod.CDCSnapshotSettings(
        keys=["id"],
        scd_type="1",
        snapshotType=mod.CDCSnapshotTypes.HISTORICAL,
        sourceType=mod.CDCSnapshotSourceTypes.TABLE,
        source={
            "table": "test_catalog.staging.customer",
            "versionColumn": "version_col",
            "versionType": mod.CDCSnapshotVersionTypes.INTEGER,
        },
    )
    flow = mod.CDCSnapshotFlow(settings)

    class _DummyDataflowConfig:
        features = {}

    version_info = mod.VersionInfo(raw_value=1, version_type=mod.CDCSnapshotVersionTypes.INTEGER)

    flow._read_snapshot_dataframe(version_info, _DummyDataflowConfig())

    kwargs = source_delta.call_args.kwargs
    assert kwargs["database"] == "test_catalog.staging"
    assert kwargs["table"] == "customer"

def test_historical_table_source_single_part_identifiers():
    # Tests to ensure 1 part identifiers correctly fail
    mod = _load_cdc_snapshot_module()
    source_delta = MagicMock()
    source_delta.return_value.read_source.return_value = object()
    mod.SourceDelta = source_delta

    settings = mod.CDCSnapshotSettings(
        keys=["id"],
        scd_type="1",
        snapshotType=mod.CDCSnapshotTypes.HISTORICAL,
        sourceType=mod.CDCSnapshotSourceTypes.TABLE,
        source={
            "table": "table",
            "versionColumn": "version_col",
            "versionType": mod.CDCSnapshotVersionTypes.INTEGER,
        },
    )
    flow = mod.CDCSnapshotFlow(settings)

    class _DummyDataflowConfig:
        features = {}

    version_info = mod.VersionInfo(raw_value=1, version_type=mod.CDCSnapshotVersionTypes.INTEGER)

    with pytest.raises(ValueError, match="Invalid table name format"):
        flow._read_snapshot_dataframe(version_info, _DummyDataflowConfig())

    source_delta.assert_not_called()

def test_historical_table_source_multi_part_identifiers():
    # Tests to ensure multi (more than 3) part identifiers correctly fail
    mod = _load_cdc_snapshot_module()
    source_delta = MagicMock()
    source_delta.return_value.read_source.return_value = object()
    mod.SourceDelta = source_delta

    settings = mod.CDCSnapshotSettings(
        keys=["id"],
        scd_type="1",
        snapshotType=mod.CDCSnapshotTypes.HISTORICAL,
        sourceType=mod.CDCSnapshotSourceTypes.TABLE,
        source={
            "table": "this.is.too.long",
            "versionColumn": "version_col",
            "versionType": mod.CDCSnapshotVersionTypes.INTEGER,
        },
    )
    flow = mod.CDCSnapshotFlow(settings)

    class _DummyDataflowConfig:
        features = {}

    version_info = mod.VersionInfo(raw_value=1, version_type=mod.CDCSnapshotVersionTypes.INTEGER)

    with pytest.raises(ValueError, match="Invalid table name format"):
        flow._read_snapshot_dataframe(version_info, _DummyDataflowConfig())

    source_delta.assert_not_called()