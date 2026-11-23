"""重构前后等价性：用相同输入比对聚合分组和成品结果。

fixture 由重构前的 AggregateOlapService 生成（见 tests/fixtures/aggregate_baseline.json），
这里在契约化后的实现上重放同一批输入，要求分组口径、结果字段和成品内容完全一致。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from app.services import base as base_module
from app.services.aggregate_olap import AggregateOlapService
from app.store import store

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "aggregate_baseline.json"


def _strip_timestamps(obj):
    if isinstance(obj, dict):
        return {k: _strip_timestamps(v) for k, v in obj.items() if k != "aggregated_at"}
    if isinstance(obj, list):
        return [_strip_timestamps(v) for v in obj]
    return obj


@pytest.fixture()
def baseline(tmp_path, monkeypatch):
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    monkeypatch.setattr(base_module, "DATA_ROOT", tmp_path)

    previous = [(r, r.get("status")) for r in store.rows("aggregate_olap")]
    for r, _ in previous:
        r["status"] = "已停用"
    added = []
    for rule in fixture["rules"]:
        entry = dict(rule)
        store.rows("aggregate_olap").append(entry)
        added.append(entry)

    parsed = tmp_path / "logs" / f"{fixture['source_key']}_parsed.jsonl"
    parsed.parent.mkdir(parents=True, exist_ok=True)
    with open(parsed, "w", encoding="utf-8") as f:
        for rec in fixture["records"]:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    yield fixture

    for entry in added:
        store.rows("aggregate_olap").remove(entry)
    for r, status in previous:
        r["status"] = status


def test_aggregate_grouping_and_product_match_pre_refactor(baseline, tmp_path):
    result = AggregateOlapService().aggregate_source(baseline["source_key"])

    assert sorted(result.keys()) == baseline["return_keys"]
    assert result["total_records"] == baseline["total_records"]
    assert result["rules_applied"] == baseline["rules_applied"]
    assert _strip_timestamps(result["results"]) == baseline["results"]

    product = _strip_timestamps([
        {"aggregation": v, "source_key": baseline["source_key"]}
        for v in result["results"].values()
    ])
    assert product == baseline["product"]

    product_file = tmp_path / "aggregations" / f"{baseline['source_key']}_aggregated.jsonl"
    on_disk = [
        json.loads(line) for line in product_file.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    assert _strip_timestamps(on_disk) == baseline["product"]


def test_aggregate_product_is_confirmed_against_checkpoint(baseline):
    svc = AggregateOlapService()
    svc.aggregate_source(baseline["source_key"])
    contract = svc.write_contract(baseline["source_key"])
    verification = contract.verify()
    assert verification["ok"], verification
    assert verification["records"] == baseline["rules_applied"]
    assert contract.manifest()["count"] == 1
