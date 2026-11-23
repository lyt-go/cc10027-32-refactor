"""聚合成品写入确认契约与格式输出确认的回归测试。

覆盖：同一日志源多批次（已出清单 / 正在重算）、新明细半批落盘失败、
重算失败后已确认范围可恢复、明细与确认位置一致、半批不成为可导出成品、
格式输出同契约核对并按原格式保留已确认数据、清单可核对历史可访问。
"""
from __future__ import annotations

import builtins
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from app.services import base as base_module
from app.services.aggregate_olap import AggregateOlapService
from app.services.base import BaseModuleService
from app.services.format_output import FormatOutputService
from app.services.write_contract import (
    ConfirmationError,
    RecordCountMismatch,
    WriteConfirmationContract,
)
from app.store import store

SOURCE = "LOG_-CONTRACT"
OTHER_SOURCE = "LOG_-CONTRACT-2"


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    """把 DATA_ROOT 指到临时目录，测试之间互不干扰。"""
    monkeypatch.setattr(base_module, "DATA_ROOT", tmp_path)
    return tmp_path


@pytest.fixture()
def rules():
    """临时生效聚合规则，测试结束后从 store 移除并还原其它规则状态。"""
    created = [
        {"id": 7101, "规则编号": "AGGR-T-COUNT", "关联源": SOURCE, "聚合维度": "level", "聚合函数": "count"},
        {"id": 7102, "规则编号": "AGGR-T-AVG", "关联源": SOURCE, "聚合维度": "level", "聚合函数": "avg_latency"},
    ]
    added = []
    for rule in created:
        entry = dict(rule, status="已生效", pending=False, abnormal=False)
        store.rows("aggregate_olap").append(entry)
        added.append(entry)
    previous = [(r, r.get("status")) for r in store.rows("aggregate_olap") if r not in added]
    for r, _ in previous:
        r["status"] = "已停用"
    yield created
    for entry in added:
        store.rows("aggregate_olap").remove(entry)
    for r, status in previous:
        r["status"] = status


def _records(levels, source: str = SOURCE):
    return [
        {"source_key": source, "line_no": i + 1, "fields": {"level": level, "latency_ms": 100 + i * 10}}
        for i, level in enumerate(levels)
    ]


def _product_lines(root: Path, source: str = SOURCE) -> list[dict]:
    path = root / "aggregations" / f"{source}_aggregated.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_parsed(root: Path, records: list[dict], source: str = SOURCE) -> None:
    path = root / "logs" / f"{source}_parsed.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _fail_after_n_staging_writes(monkeypatch, n: int):
    """让暂存文件的第 n+1 次写入抛错，模拟部分落盘后断开。"""
    real_open = builtins.open
    state = {"writes": 0}

    class Flaky:
        def __init__(self, inner):
            self._inner = inner

        def write(self, data):
            if state["writes"] >= n:
                raise OSError("磁盘写入中断")
            state["writes"] += 1
            return self._inner.write(data)

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return self._inner.__exit__(*exc)

    def flaky_open(path, mode="r", *args, **kwargs):
        handle = real_open(path, mode, *args, **kwargs)
        if str(path).endswith(".part") and "w" in mode:
            return Flaky(handle)
        return handle

    monkeypatch.setattr(builtins, "open", flaky_open)


class TestAggregateConfirmationContract:
    def test_batch_commit_records_product_checkpoint_manifest(self, data_root, rules):
        svc = AggregateOlapService()
        result = svc.aggregate_batch(SOURCE, _records(["INFO", "ERROR"]), rule_ids=["AGGR-T-COUNT"])

        assert result["confirmation"]["status"] == "confirmed"
        contract = svc.write_contract(SOURCE)
        verification = contract.verify()
        assert verification["ok"], verification
        assert verification["status"] == "confirmed"
        assert verification["records"] == 1
        assert verification["expected_records"] == 1

        cp = contract.confirmation()
        assert cp["contract_version"] == 1
        assert cp["records"] == 1
        assert cp["batch_id"] == result["confirmation"]["batch_id"]

        manifest = contract.manifest(verify_history=True)
        assert manifest["count"] == 1
        assert manifest["entries"][0]["batch_id"] == cp["batch_id"]
        assert manifest["entries"][0]["history_accessible"] is True
        assert manifest["entries"][0]["history_checksum_ok"] is True

    def test_two_batches_same_source_listed_and_recomputing(self, data_root, rules):
        """一个批次已经用来出清单，另一个批次正在重算失败：确认范围只属于已确认批次。"""
        svc = AggregateOlapService()
        first = svc.aggregate_batch(SOURCE, _records(["INFO", "INFO"]), rule_ids=["AGGR-T-COUNT"])
        first_batch = first["confirmation"]["batch_id"]
        product_before = _product_lines(data_root)
        contract = svc.write_contract(SOURCE)

        class Boom(RuntimeError):
            pass

        original = WriteConfirmationContract._append_manifest

        def explode(*args, **kwargs):
            raise Boom("模拟重算中断")

        WriteConfirmationContract._append_manifest = explode
        try:
            with pytest.raises(Boom):
                svc.aggregate_batch(SOURCE, _records(["ERROR"]), rule_ids=["AGGR-T-COUNT"])
        finally:
            WriteConfirmationContract._append_manifest = original

        # 正式成品仍是已确认批次，确认位置和清单都没变
        assert _product_lines(data_root) == product_before
        assert contract.confirmation()["batch_id"] == first_batch
        manifest = contract.manifest()
        assert [e["batch_id"] for e in manifest["entries"]] == [first_batch]

        # 重算失败后恢复，已确认范围仍可读出
        recovery = svc.confirm_aggregation(SOURCE)
        assert recovery["ok"], recovery
        assert _product_lines(data_root) == product_before

    def test_partial_details_failure_never_becomes_confirmed_product(self, data_root, rules, monkeypatch):
        """新明细文件部分落盘后失败：正式成品与确认位置保持上一确认批次。"""
        svc = AggregateOlapService()
        svc.aggregate_batch(SOURCE, _records(["INFO"]), rule_ids=["AGGR-T-COUNT"])
        contract = svc.write_contract(SOURCE)
        before = contract.confirmation()
        before_product = _product_lines(data_root)

        with monkeypatch.context() as patch:
            _fail_after_n_staging_writes(patch, 1)
            with pytest.raises(OSError):
                svc.aggregate_batch(
                    SOURCE, _records(["ERROR", "WARN"]),
                    rule_ids=["AGGR-T-COUNT", "AGGR-T-AVG"],
                )

        assert _product_lines(data_root) == before_product
        assert contract.confirmation()["batch_id"] == before["batch_id"]
        assert contract.verify()["ok"]
        # 暂存目录没有留下可被误认作成品的文件
        leftovers = [p for p in contract.scope.staging_dir.glob("*") if p.is_file()]
        assert all(p.name.endswith(".part") for p in leftovers)

    def test_record_count_mismatch_rejected(self, data_root, rules):
        svc = AggregateOlapService()
        svc.aggregate_batch(SOURCE, _records(["INFO"]), rule_ids=["AGGR-T-COUNT"])
        contract = svc.write_contract(SOURCE)
        before = contract.confirmation()

        batch = contract.begin()
        batch.append({"aggregation": {}, "source_key": SOURCE})
        with pytest.raises(RecordCountMismatch):
            batch.commit(expected_records=2)

        assert contract.confirmation()["batch_id"] == before["batch_id"]
        assert contract.verify()["ok"]

    def test_checkpoint_write_failure_rolls_back_product(self, data_root, rules, monkeypatch):
        """确认位置写失败时正式成品回退到上一确认版本，避免半确认。"""
        svc = AggregateOlapService()
        svc.aggregate_batch(SOURCE, _records(["INFO"]), rule_ids=["AGGR-T-COUNT"])
        contract = svc.write_contract(SOURCE)
        before_product = _product_lines(data_root)
        before_cp = contract.confirmation()

        def broken_checkpoint(*args, **kwargs):
            raise OSError("checkpoint disk full")

        with monkeypatch.context() as patch:
            patch.setattr(BaseModuleService, "_write_checkpoint", broken_checkpoint)
            with pytest.raises(OSError):
                svc.aggregate_batch(SOURCE, _records(["ERROR", "ERROR"]), rule_ids=["AGGR-T-COUNT"])

        assert _product_lines(data_root) == before_product
        assert contract.confirmation()["batch_id"] == before_cp["batch_id"]

    def test_preview_does_not_advance_confirmation(self, data_root, rules):
        svc = AggregateOlapService()
        svc.aggregate_batch(SOURCE, _records(["INFO"]), rule_ids=["AGGR-T-COUNT"])
        contract = svc.write_contract(SOURCE)
        before = contract.confirmation()
        before_product = _product_lines(data_root)

        preview = svc.aggregate_batch(
            SOURCE, _records(["ERROR", "ERROR"]), rule_ids=["AGGR-T-COUNT"], confirm=False
        )
        assert preview["confirmation"] is None
        assert contract.confirmation()["batch_id"] == before["batch_id"]
        assert _product_lines(data_root) == before_product

    def test_history_survives_new_batch_and_stays_accessible(self, data_root, rules):
        """新批写入完成后，清单里每个已确认历史快照仍可访问、校验一致。"""
        svc = AggregateOlapService()
        first = svc.aggregate_batch(SOURCE, _records(["INFO"]), rule_ids=["AGGR-T-COUNT"])
        second = svc.aggregate_batch(SOURCE, _records(["ERROR"]), rule_ids=["AGGR-T-COUNT"])
        third = svc.aggregate_batch(SOURCE, _records(["WARN"]), rule_ids=["AGGR-T-COUNT"])

        contract = svc.write_contract(SOURCE)
        manifest = contract.manifest(verify_history=True)
        assert manifest["count"] == 3
        assert [e["batch_id"] for e in manifest["entries"]] == [
            first["confirmation"]["batch_id"],
            second["confirmation"]["batch_id"],
            third["confirmation"]["batch_id"],
        ]
        for entry in manifest["entries"]:
            assert entry["history_accessible"] is True
            assert entry["history_checksum_ok"] is True
            # 每个历史快照都保留了当时的完整成品
            history = Path(entry["history_path"])
            assert history.read_text(encoding="utf-8").strip()

    def test_interrupted_new_batch_ignored_by_manifest(self, data_root, rules):
        """清单写入时中断留下半行，不应影响已确认流程，也不会吞掉历史。"""
        svc = AggregateOlapService()
        svc.aggregate_batch(SOURCE, _records(["INFO"]), rule_ids=["AGGR-T-COUNT"])
        contract = svc.write_contract(SOURCE)
        manifest_path = contract.scope.manifest_path

        with open(manifest_path, "a", encoding="utf-8") as f:
            f.write('{"partial": ')  # 半行，模拟写入断开

        manifest = contract.manifest(verify_history=True)
        assert manifest["count"] == 1
        assert manifest["entries"][0]["history_accessible"] is True
        assert contract.verify()["ok"]

        # 新批仍可确认，历史继续累积
        second = svc.aggregate_batch(SOURCE, _records(["ERROR"]), rule_ids=["AGGR-T-COUNT"])
        manifest = contract.manifest()
        assert [e["batch_id"] for e in manifest["entries"]][-1] == second["confirmation"]["batch_id"]
        assert manifest["count"] == 2

    def test_multiple_sources_isolation(self, data_root, rules):
        svc = AggregateOlapService()
        _write_parsed(data_root, _records(["INFO"]), OTHER_SOURCE)
        svc.aggregate_batch(SOURCE, _records(["INFO"]), rule_ids=["AGGR-T-COUNT"])
        svc.aggregate_batch(OTHER_SOURCE, _records(["ERROR"]), rule_ids=["AGGR-T-COUNT"])

        rule_cp_a = svc.write_contract(SOURCE).confirmation()
        rule_cp_b = svc.write_contract(OTHER_SOURCE).confirmation()
        assert rule_cp_a["batch_id"] != rule_cp_b["batch_id"]
        assert _product_lines(data_root, OTHER_SOURCE)[0]["aggregation"]["rule_id"] == "AGGR-T-COUNT"
        assert len(svc.aggregation_manifest(SOURCE)["entries"]) == 1
        assert len(svc.aggregation_manifest(OTHER_SOURCE)["entries"]) == 1


class TestFormatOutputConfirmationContract:
    def _write_mapped(self, root: Path, records: list[dict], source: str = SOURCE) -> None:
        path = root / "logs" / f"{source}_mapped.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def _mapped_records(self):
        return [
            {"source_key": SOURCE, "line_no": 1, "standardized": {"level": "INFO", "latency_ms": "100"}},
            {"source_key": SOURCE, "line_no": 2, "standardized": {"level": "ERROR", "latency_ms": "200"}},
        ]

    def test_export_uses_contract_and_keeps_legacy_manifest_shape(self, data_root):
        self._write_mapped(data_root, self._mapped_records())
        svc = FormatOutputService()

        result = svc.export_source(SOURCE, "jsonl")
        assert result["records"] == 2

        contract = svc.write_contract(SOURCE, "jsonl")
        verification = contract.verify()
        assert verification["ok"], verification
        assert verification["status"] == "confirmed"

        manifest = svc.get_export_manifest(SOURCE)
        # 旧字段结构保持不变
        assert set(["source_key", "collect", "parse", "mapping", "export"]).issubset(manifest)
        assert manifest["export"]["records"] == 2
        # 契约视角额外给出核对结果和清单
        assert manifest["confirmation"]["ok"] is True
        assert manifest["manifest"]["count"] == 1

    @pytest.mark.parametrize("format_type,suffix", [
        ("jsonl", ".jsonl"), ("json", ".json"), ("gz", ".jsonl.gz"), ("csv", ".csv"),
    ])
    def test_all_formats_confirmed_and_preserved(self, data_root, format_type, suffix):
        self._write_mapped(data_root, self._mapped_records())
        svc = FormatOutputService()
        result = svc.export_source(SOURCE, format_type)
        assert result["records"] == 2

        product = data_root / "exports" / f"{SOURCE}_export{suffix}"
        assert product.exists()

        contract = svc.write_contract(SOURCE, format_type)
        assert contract.verify()["ok"]
        manifest = contract.manifest(verify_history=True)
        assert manifest["count"] == 1
        assert manifest["entries"][0]["format"] == format_type
        assert manifest["entries"][0]["history_accessible"] is True

        # 已确认数据可按原格式读回
        if format_type == "jsonl":
            assert len(product.read_text(encoding="utf-8").strip().splitlines()) == 2
        elif format_type == "json":
            assert len(json.loads(product.read_text(encoding="utf-8"))) == 2
        elif format_type == "gz":
            assert len(gzip.open(product, "rt", encoding="utf-8").read().strip().splitlines()) == 2
        elif format_type == "csv":
            lines = product.read_text(encoding="utf-8").strip().splitlines()
            assert lines[0] == "level,latency_ms"
            assert len(lines) == 3

    def test_partial_download_export_failure_keeps_confirmed_export(self, data_root, monkeypatch):
        """格式输出仍在处理下载时新批写入失败：下载方仍能拿到上一确认成品。"""
        self._write_mapped(data_root, self._mapped_records())
        svc = FormatOutputService()
        svc.export_source(SOURCE, "jsonl")
        contract = svc.write_contract(SOURCE, "jsonl")
        confirmed_path = contract.scope.product_path
        before_bytes = confirmed_path.read_bytes()

        # 新增映射记录后重导出，中途失败
        self._write_mapped(data_root, self._mapped_records() + [
            {"source_key": SOURCE, "line_no": 3, "standardized": {"level": "WARN", "latency_ms": "300"}},
        ])

        with monkeypatch.context() as patch:
            _fail_after_n_staging_writes(patch, 1)
            with pytest.raises(OSError):
                svc.export_source(SOURCE, "jsonl")

        # 正在下载的进程读正式路径，内容仍是上一确认的完整成品
        assert confirmed_path.read_bytes() == before_bytes
        assert contract.verify()["ok"]
        manifest = svc.get_export_manifest(SOURCE)
        assert manifest["export"]["records"] == 2
        assert manifest["manifest"]["count"] == 1

    def test_export_checkpoint_mismatch_detected_and_recovered(self, data_root):
        self._write_mapped(data_root, self._mapped_records())
        svc = FormatOutputService()
        svc.export_source(SOURCE, "jsonl")
        contract = svc.write_contract(SOURCE, "jsonl")
        confirmed = contract.scope.product_path.read_bytes()

        # 模拟成品被外部截断/改写
        contract.scope.product_path.write_bytes(confirmed[: len(confirmed) // 2])
        verification = contract.verify()
        assert not verification["ok"]
        assert verification["status"] in ("mismatch", "missing_product")

        recovery = svc.confirm_export(SOURCE, "jsonl")
        assert recovery["recovery"]["ok"], recovery
        assert contract.scope.product_path.read_bytes() == confirmed
        assert contract.verify()["ok"]

    def test_export_generation_isolated_per_format(self, data_root):
        """同一日志源多格式导出共用位点名，但各自成品/清单历史互不覆盖。"""
        self._write_mapped(data_root, self._mapped_records())
        svc = FormatOutputService()
        svc.export_source(SOURCE, "jsonl")
        svc.export_source(SOURCE, "csv")

        jsonl_manifest = svc.export_manifest(SOURCE, "jsonl")
        csv_manifest = svc.export_manifest(SOURCE, "csv")
        assert jsonl_manifest["count"] == 1
        assert csv_manifest["count"] == 1
        assert jsonl_manifest["entries"][0]["format"] == "jsonl"
        assert csv_manifest["entries"][0]["format"] == "csv"
        assert svc.write_contract(SOURCE, "jsonl").verify()["ok"]
        assert svc.write_contract(SOURCE, "csv").verify()["ok"]


class TestSharedContractSurface:
    def test_services_share_one_contract_implementation(self):
        agg_contract = AggregateOlapService().write_contract(SOURCE)
        export_contract = FormatOutputService().write_contract(SOURCE, "jsonl")
        assert type(agg_contract) is type(export_contract) is WriteConfirmationContract
        assert agg_contract.scope.kind == "aggregate"
        assert export_contract.scope.kind == "export"
        # 两者使用同一套提交/核对/恢复/清单方法
        for name in ("begin", "verify", "recover", "manifest", "confirmation"):
            assert hasattr(agg_contract, name)
            assert hasattr(export_contract, name)

    def test_confirmation_error_is_shared_type(self):
        assert issubclass(RecordCountMismatch, ConfirmationError)
