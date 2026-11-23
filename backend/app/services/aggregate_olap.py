"""聚合计算：按维度对日志数据进行聚合计算并生成分析结果。

成品写入统一走 :mod:`app.services.write_contract`：只有完整聚合批次才会
被确认为成品，失败批次不会污染已确认范围。
"""
from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from app.services.base import BaseModuleService
from app.services.write_contract import (
    ConfirmationError,
    ProductScope,
    WriteConfirmationContract,
)
from app.store import store


class AggregateOlapService(BaseModuleService):
    MODULE = "aggregate_olap"
    ENTITY = "聚合规则"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["规则编号", "关联源", "聚合维度", "聚合函数", "时间窗口", "写入目标", "延迟容忍", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联源", "聚合维度"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    PRODUCT_KIND = "aggregate"
    CONFIRMATION_KEY = "aggregate_{source_key}"
    MANIFEST_NAME = "aggregate_manifest.jsonl"

    # -- 成品位置 --

    def product_scope(self, source_key: str) -> ProductScope:
        aggregations_dir = self._data_dir("aggregations")
        return ProductScope(
            kind=self.PRODUCT_KIND,
            source_key=source_key,
            product_path=aggregations_dir / f"{source_key}_aggregated.jsonl",
            checkpoint_name=self.CONFIRMATION_KEY.format(source_key=source_key),
            staging_dir=self._data_dir("aggregations") / ".staging",
            history_dir=aggregations_dir / "history" / source_key,
            manifest_path=self._data_dir("aggregations") / self.MANIFEST_NAME,
            suffix=".jsonl",
            fmt="jsonl",
        )

    def write_contract(self, source_key: str) -> WriteConfirmationContract:
        return WriteConfirmationContract(self, self.product_scope(source_key))

    # -- 聚合计算 --

    def aggregate_source(self, source_key: str) -> dict[str, Any]:
        """对指定日志源的已解析数据进行聚合计算。"""
        parsed_file = self._data_dir("logs") / f"{source_key}_parsed.jsonl"
        if not parsed_file.exists():
            return {"error": "无已解析数据", "aggregated": 0}

        records = self._read_jsonl(parsed_file)
        rules = [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]

        results = {}
        for rule in rules:
            dim_field = rule.get("聚合维度", "")
            agg_func = rule.get("聚合函数", "count")

            grouped = self._group_by_dimension(records, dim_field)
            agg_result = self._apply_aggregation(grouped, agg_func)

            agg_key = rule["规则编号"]
            agg_output = {
                "rule_id": agg_key,
                "dimension": dim_field,
                "aggregation": agg_func,
                "groups": len(agg_result),
                "results": agg_result,
                "aggregated_at": time.time(),
            }
            results[agg_key] = agg_output

        contract = self.write_contract(source_key)
        batch = contract.begin()
        try:
            batch.write_jsonl(
                {"aggregation": v, "source_key": source_key} for v in results.values()
            )
            batch.commit(
                expected_records=len(results),
                meta={
                    "total_records": len(records),
                    "rules_applied": len(rules),
                    "groups": sum(r.get("groups", 0) for r in results.values()),
                },
                checkpoint_extra={
                    "total_records": len(records),
                    "rules_applied": len(rules),
                    "groups": sum(r.get("groups", 0) for r in results.values()),
                    "completed_at": time.time(),
                },
            )
        except ConfirmationError:
            # 半批数据已经丢弃，正式成品保持上一确认范围，异常继续上抛供调用方重试。
            raise
        except BaseException:
            batch.abort()
            raise

        return {
            "source_key": source_key, "total_records": len(records),
            "rules_applied": len(rules), "results": results,
        }

    def aggregate_batch(
        self,
        source_key: str,
        records: list[dict[str, Any]],
        rule_ids: list[str] | None = None,
        *,
        confirm: bool = True,
    ) -> dict[str, Any]:
        """用显式批次明细执行聚合，便于同一日志源的多个批次分批确认。

        ``records`` 是这一批次的完整明细；只有整批写入成功才会推进确认位置。
        未被确认的批次不会改变成品和确认位置，已确认的历史范围仍可恢复。
        """
        rules = [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]
        if rule_ids is not None:
            wanted = set(rule_ids)
            rules = [r for r in rules if r["规则编号"] in wanted]

        results = {}
        for rule in rules:
            dim_field = rule.get("聚合维度", "")
            agg_func = rule.get("聚合函数", "count")
            grouped = self._group_by_dimension(records, dim_field)
            agg_result = self._apply_aggregation(grouped, agg_func)
            results[rule["规则编号"]] = {
                "rule_id": rule["规则编号"],
                "dimension": dim_field,
                "aggregation": agg_func,
                "groups": len(agg_result),
                "results": agg_result,
                "aggregated_at": time.time(),
            }

        contract = self.write_contract(source_key)
        batch = contract.begin()
        try:
            batch.write_jsonl(
                {"aggregation": v, "source_key": source_key} for v in results.values()
            )
            if not confirm:
                # 重算预览：只写暂存文件，不推进确认位置，已确认范围保持不变。
                batch.abort()
                return {
                    "source_key": source_key,
                    "total_records": len(records),
                    "rules_applied": len(rules),
                    "results": results,
                    "confirmation": None,
                }
            entry = batch.commit(
                expected_records=len(results),
                meta={
                    "total_records": len(records),
                    "rules_applied": len(rules),
                    "groups": sum(r.get("groups", 0) for r in results.values()),
                },
                checkpoint_extra={
                    "total_records": len(records),
                    "rules_applied": len(rules),
                    "groups": sum(r.get("groups", 0) for r in results.values()),
                    "completed_at": time.time(),
                },
            )
        except BaseException:
            batch.abort()
            raise

        return {
            "source_key": source_key,
            "total_records": len(records),
            "rules_applied": len(rules),
            "results": results,
            "confirmation": entry,
        }

    def confirm_aggregation(self, source_key: str) -> dict[str, Any]:
        """核对聚合成品和确认位置；不一致时恢复最近一次确认范围。"""
        contract = self.write_contract(source_key)
        verification = contract.verify()
        if verification["ok"]:
            return verification
        recovery = contract.recover()
        return {"verification": verification, "recovery": recovery}

    def aggregation_manifest(self, source_key: str, *, verify_history: bool = True) -> dict[str, Any]:
        """可核对的聚合成品清单；默认核对历史快照是否仍可访问。"""
        return self.write_contract(source_key).manifest(verify_history=verify_history)

    def aggregation_history(self, source_key: str) -> list[dict[str, Any]]:
        return self.write_contract(source_key).history_entries()

    def _group_by_dimension(self, records: list[dict[str, Any]], dim_field: str) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for rec in records:
            fields = rec.get("fields", rec)
            dim_val = str(fields.get(dim_field, fields.get("level", rec.get("source_key", "unknown"))))
            groups[dim_val].append(rec)
        return dict(groups)

    def _apply_aggregation(self, groups: dict[str, list[dict[str, Any]]], agg_func: str) -> list[dict[str, Any]]:
        results = []
        for dim_val, recs in groups.items():
            result = {"dimension": dim_val, "count": len(recs)}
            if "avg" in agg_func.lower():
                vals = [float(r.get("fields", {}).get("latency_ms", 0)) for r in recs if r.get("fields", {}).get("latency_ms")]
                result["avg_latency"] = round(sum(vals) / len(vals), 2) if vals else 0
            if "sum" in agg_func.lower():
                vals = [float(r.get("fields", {}).get("latency_ms", 0)) for r in recs]
                result["total_latency"] = round(sum(vals), 2)
            if "max" in agg_func.lower():
                vals = [float(r.get("fields", {}).get("latency_ms", 0)) for r in recs]
                result["max_latency"] = round(max(vals), 2) if vals else 0
            if "min" in agg_func.lower():
                vals = [float(r.get("fields", {}).get("latency_ms", 0)) for r in recs]
                result["min_latency"] = round(min(vals), 2) if vals else 0
            results.append(result)
        return results

    def compute_olap_cube(self, source_key: str) -> dict[str, Any]:
        """对富化后的数据进行多维度OLAP立方体聚合。"""
        enriched_file = self._data_dir("logs") / f"{source_key}_enriched.jsonl"
        if not enriched_file.exists():
            return self.aggregate_source(source_key)

        records = self._read_jsonl(enriched_file)
        # 二维交叉聚合：source_level × date
        by_source_level: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        by_date: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for rec in records:
            fields = rec.get("fields", rec)
            level = str(fields.get("level", "unknown"))
            source = rec.get("source_key", "unknown")
            collected = rec.get("collected_at", 0)
            date_key = time.strftime("%Y%m%d", time.localtime(collected)) if isinstance(collected, (int, float)) else "unknown"

            by_source_level[source][level] += 1
            by_date[date_key][level] += 1

        return {
            "source_key": source_key,
            "total_records": len(records),
            "by_source_level": {k: dict(v) for k, v in by_source_level.items()},
            "by_date": dict(by_date),
        }
