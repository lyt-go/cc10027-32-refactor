"""格式输出：将标准化数据按输出格式配置写入指定路径。

导出的成品和确认位置同样走 :mod:`app.services.write_contract`：
只有整批导出写完才确认，半批数据不会变成可下载的确认成品，
已确认的历史导出仍可按原格式访问。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.write_contract import (
    ConfirmationError,
    ProductScope,
    WriteConfirmationContract,
)

SUPPORTED_FORMATS = {
    "jsonl": ".jsonl",
    "json": ".json",
    "gz": ".jsonl.gz",
    "csv": ".csv",
}


class FormatOutputService(BaseModuleService):
    MODULE = "format_output"
    ENTITY = "输出配置"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["配置编号", "关联源", "输出格式", "输出路径", "压缩方式", "分片大小", "命名规则", "配置版本"]
    REQUIRED_FIELDS = ["配置编号", "关联源", "输出格式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    PRODUCT_KIND = "export"
    # 重构前 export_source 所有格式共用一个位点名。契约按格式拆开位点和历史，
    # 但 jsonl 仍沿用旧位点名，保证既有清单接口的 export 字段语义不变。
    CONFIRMATION_KEY = "{source_key}_export"
    LEGACY_CONFIRMATION_KEY = "{source_key}_export"
    MANIFEST_NAME = "export_manifest.jsonl"

    # -- 成品位置 --

    def _checkpoint_name(self, source_key: str, format_type: str) -> str:
        base = self.CONFIRMATION_KEY.format(source_key=source_key)
        return base if format_type == "jsonl" else f"{base}_{format_type}"

    def product_scope(self, source_key: str, format_type: str = "jsonl") -> ProductScope:
        exports_dir = self._data_dir("exports")
        suffix = SUPPORTED_FORMATS.get(format_type, ".jsonl")
        return ProductScope(
            kind=self.PRODUCT_KIND,
            source_key=source_key,
            product_path=exports_dir / f"{source_key}_export{suffix}",
            checkpoint_name=self._checkpoint_name(source_key, format_type),
            staging_dir=self._data_dir("exports") / ".staging",
            history_dir=exports_dir / "history" / source_key / format_type,
            manifest_path=self._data_dir("exports") / self.MANIFEST_NAME,
            suffix=suffix,
            fmt=format_type,
            format_checkpoint_name=self.LEGACY_CONFIRMATION_KEY.format(source_key=source_key),
        )

    def write_contract(self, source_key: str, format_type: str = "jsonl") -> WriteConfirmationContract:
        return WriteConfirmationContract(self, self.product_scope(source_key, format_type))

    # -- 导出 --

    def export_source(self, source_key: str, format_type: str = "jsonl") -> dict[str, Any]:
        """将指定日志源的标准化数据导出为指定格式。"""
        mapped_file = self._data_dir("logs") / f"{source_key}_mapped.jsonl"
        if not mapped_file.exists():
            return {"error": f"日志源 {source_key} 无映射数据，请先执行字段映射", "exported": 0}

        if format_type not in SUPPORTED_FORMATS:
            return {"error": f"不支持的输出格式: {format_type}"}

        records = self._read_jsonl(mapped_file)
        contract = self.write_contract(source_key, format_type)
        batch = contract.begin()
        try:
            self._write_export(batch, records, format_type)
            batch.commit(
                expected_records=len(records),
                meta={"format": format_type, "source_key": source_key},
                checkpoint_extra={
                    "format": format_type,
                    "records": len(records),
                    "output_file": str(contract.scope.product_path),
                    "exported_at": time.time(),
                },
            )
        except ConfirmationError:
            # 半批导出已丢弃；正在下载的读取方仍拿到上一确认成品。
            raise
        except BaseException:
            batch.abort()
            raise

        return {
            "source_key": source_key,
            "format": format_type,
            "records": len(records),
            "output_file": str(contract.scope.product_path),
        }

    def _write_export(self, batch, records: list[dict[str, Any]], format_type: str) -> None:
        if format_type == "jsonl":
            batch.write_jsonl(records)
            return

        if format_type == "json":
            f = batch.open_text()
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)
            batch.records = len(records)
            return

        if format_type == "gz":
            f = batch.open_gzip_text(compresslevel=6)
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            batch.records = len(records)
            return

        if format_type == "csv":
            # 保持重构前的 CSV 拼接方式，输出字节不变（含无引号转义的行为）。
            f = batch.open_text()
            if records:
                keys = list(records[0].get("standardized", records[0]).keys())
                f.write(",".join(keys) + "\n")
                for rec in records:
                    std = rec.get("standardized", rec)
                    f.write(",".join(str(std.get(k, "")) for k in keys) + "\n")
            batch.records = len(records)
            return

    def export_batch(self, source_keys: list[str], format_type: str = "jsonl") -> dict[str, Any]:
        """批量导出多个日志源的数据。"""
        results = []
        total = 0
        for key in source_keys:
            r = self.export_source(key, format_type)
            results.append(r)
            total += r.get("records", 0)
        return {"total": total, "sources": len(results), "details": results}

    def get_export_manifest(self, source_key: str) -> dict[str, Any]:
        """获取指定日志源的导出清单。

        保留重构前的字段结构；``export`` 仍是确认位置，同时附上契约视角的
        成品核对结果和可核对清单，旧调用方不受影响。
        """
        cp = self._read_checkpoint(f"{source_key}_export")
        mapped_cp = self._read_checkpoint(f"{source_key}_map")
        parsed_cp = self._read_checkpoint(f"{source_key}_parse")
        collect_cp = self._read_checkpoint(source_key)

        format_type = str((cp or {}).get("format") or "jsonl")
        contract = self.write_contract(source_key, format_type)

        return {
            "source_key": source_key,
            "collect": collect_cp,
            "parse": parsed_cp,
            "mapping": mapped_cp,
            "export": cp,
            "confirmation": contract.verify(),
            "manifest": contract.manifest(verify_history=True),
        }

    def confirm_export(self, source_key: str, format_type: str = "jsonl") -> dict[str, Any]:
        """核对导出成品和确认位置；不一致时恢复最近一次确认范围。"""
        contract = self.write_contract(source_key, format_type)
        verification = contract.verify()
        if verification["ok"]:
            return verification
        return {"verification": verification, "recovery": contract.recover()}

    def export_manifest(
        self, source_key: str, format_type: str = "jsonl", *, verify_history: bool = True
    ) -> dict[str, Any]:
        return self.write_contract(source_key, format_type).manifest(verify_history=verify_history)
