"""成品写入确认契约：聚合明细和格式输出共用的「成品 + 确认位置」记录方式。

聚合计算和格式输出原先各写了一套“先落成品、再记确认位点”的逻辑，
而且都是直接向正式文件追加，重算中途失败会把半批数据留在正式文件里。
这里把两处共用的部分抽出来：

* 新批次只写临时暂存文件（staging），完整写完后才原子替换正式成品；
* 正式成品、确认位置（checkpoint）和可核对的清单（manifest）同步提交；
* 每次提交都在 history 目录留下不可变的成品快照，供失败后恢复和审计；
* 提交前核对记录数与校验和，明细与确认位置不一致时拒绝确认；
* 失败时暂存文件被丢弃，正式成品和确认位置保持上一确认批次不变；
* 正在下载/读取的进程要么看到旧完整成品，要么看到新完整成品。

两个服务只负责各业务数据的序列化格式，写入确认依据完全共用。
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型提示，避免循环导入
    from app.services.base import BaseModuleService

CONTRACT_VERSION = 1
STAGING_DIRNAME = ".staging"
HISTORY_DIRNAME = "history"


class ConfirmationError(RuntimeError):
    """成品写入确认失败：调用方应保留上一确认版本。"""


class RecordCountMismatch(ConfirmationError):
    """暂存明细的记录数与调用方声明的不一致。"""


_LOCK_REGISTRY: dict[str, threading.RLock] = {}
_REGISTRY_LOCK = threading.Lock()


def _lock_for(path: Path) -> threading.RLock:
    key = str(path.resolve(strict=False))
    with _REGISTRY_LOCK:
        lock = _LOCK_REGISTRY.get(key)
        if lock is None:
            lock = threading.RLock()
            _LOCK_REGISTRY[key] = lock
        return lock


def file_checksum(path: Path) -> str:
    """按字节计算成品校验和，长度与 BaseModuleService._compute_checksum 一致。"""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()[:16]


def _count_jsonl_records(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with open(path, "rb") as f:
        for raw in f:
            if raw.strip():
                count += 1
    return count


def _read_jsonl_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines: list[str] = []
    with open(path, "rb") as f:
        for raw in f:
            if raw.strip():
                lines.append(raw.decode("utf-8", errors="replace"))
    return lines


def _atomic_replace(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src, dst)


def _fsync_path(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@dataclass(frozen=True)
class ProductScope:
    """一份成品的全部位置信息；聚合明细和导出成品各有一套。

    聚合明细沿用重构前的位点名 ``aggregate_<source>``；导出按格式拆开位点名，
    但 jsonl 仍沿用旧名，既有清单接口的 ``export`` 字段语义不变。
    ``suffix`` / ``fmt`` 用于区分同一日志源的不同成品，
    并让历史快照与清单条目各自独立。
    """

    kind: str
    source_key: str
    product_path: Path
    checkpoint_name: str
    staging_dir: Path
    history_dir: Path
    manifest_path: Path
    suffix: str = ""
    fmt: str = "jsonl"
    # 旧版本多种导出格式共用一个位点名；按格式拆分后用它兼容读取旧确认记录。
    format_checkpoint_name: str = ""


class StagedProduct:
    """一次尚未确认的成品写入；只在 commit 之后才变成正式成品。"""

    def __init__(self, contract: "WriteConfirmationContract", batch_id: str, staging_path: Path):
        self.contract = contract
        self.batch_id = batch_id
        self.staging_path = staging_path
        self.records = 0
        self._fh = None
        self._closed = False
        self._committed = False
        self._aborted = False

    # -- 写入接口：调用方按业务格式写，记录数由这里累加 --

    def open_text(self):
        self.staging_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.staging_path, "w", encoding="utf-8")
        return self._fh

    def open_gzip_text(self, compresslevel: int = 6):
        self.staging_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = gzip.open(self.staging_path, "wt", encoding="utf-8", compresslevel=compresslevel)
        return self._fh

    def append(self, record: dict[str, Any]) -> None:
        """逐条落盘；中途异常只会留下暂存文件，不影响正式成品。"""
        if self._fh is None:
            self.open_text()
        self._fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        self.records += 1

    def write_jsonl(self, records: Iterable[dict[str, Any]]) -> int:
        if self._fh is None:
            self.open_text()
        for record in records:
            self._fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            self.records += 1
        return self.records

    def _close(self) -> None:
        if self._fh is not None and not self._closed:
            self._fh.flush()
            try:
                os.fsync(self._fh.fileno())
            except (OSError, ValueError):
                pass
            self._fh.close()
            self._closed = True
        self._fh = None

    def commit(
        self,
        *,
        expected_records: int | None = None,
        meta: dict[str, Any] | None = None,
        checkpoint_extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._committed:
            raise ConfirmationError("该批次已经提交，不能重复确认")
        if self._aborted:
            raise ConfirmationError("该批次已经中止，不能提交")
        return self.contract._commit(
            self,
            expected_records=expected_records,
            meta=meta or {},
            checkpoint_extra=checkpoint_extra or {},
        )

    def abort(self) -> None:
        if self._committed:
            return
        self._aborted = True
        try:
            self._close()
        finally:
            try:
                self.staging_path.unlink(missing_ok=True)
            except OSError:
                pass

    def __enter__(self) -> "StagedProduct":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None and not self._committed:
            self.abort()
        return False


class WriteConfirmationContract:
    """共享的写入确认依据：暂存 -> 校验 -> 原子替换 -> 确认位置 -> 清单。"""

    def __init__(self, service: "BaseModuleService", scope: ProductScope):
        self.service = service
        self.scope = scope
        # 同一日志源的不同格式共用一个确认位置，因此按位点名加锁。
        self._lock = _lock_for(Path(f"checkpoint::{scope.checkpoint_name}"))

    # -- 读取与核对 --

    def _checkpoint_path(self) -> Path:
        return self.service._data_dir("checkpoints") / f"{self.scope.checkpoint_name}.json"

    def confirmation(self) -> dict[str, Any] | None:
        """读取确认位置；兼容重构前没有契约字段的旧位点。"""
        cp = self.service._read_checkpoint(self.scope.checkpoint_name)
        if cp is None and self.scope.format_checkpoint_name:
            # 历史版本所有格式共用一个位点名；按格式拆开后仍要能读到旧记录。
            legacy_cp = self.service._read_checkpoint(self.scope.format_checkpoint_name)
            if legacy_cp is not None and str(legacy_cp.get("format") or self.scope.fmt) == self.scope.fmt:
                cp = legacy_cp
        if cp is None:
            return None
        normalised = dict(cp)
        normalised.setdefault("kind", self.scope.kind)
        normalised.setdefault("source_key", self.scope.source_key)
        # 旧导出位点用 output_file 记录成品位置，优先沿用，避免误判成品缺失。
        if not normalised.get("product_path") and normalised.get("output_file"):
            normalised["product_path"] = normalised["output_file"]
        normalised.setdefault("product_path", str(self.scope.product_path))
        if "contract_version" in normalised:
            normalised["legacy"] = False
        else:
            normalised["contract_version"] = 0
            normalised["legacy"] = True
            # 旧位点只记录了各自的业务计数，成品行数无法从位点还原。
            normalised.setdefault("records", None)
        normalised.setdefault("format", self.scope.fmt)
        return normalised

    def _same_product(self, confirmation: dict[str, Any]) -> bool:
        product = Path(confirmation.get("product_path") or self.scope.product_path)
        return product == self.scope.product_path

    def _latest_manifest_entry(self, *, fmt: str | None = None) -> dict[str, Any] | None:
        for entry in reversed(self.manifest_entries()):
            if fmt is not None and entry.get("format") != fmt:
                continue
            if not self._same_product(entry):
                continue
            return entry
        return None

    def scoped_confirmation(self) -> dict[str, Any] | None:
        """当前成品对应的确认记录。"""
        return self.confirmation()

    def _product_matches(self, confirmation: dict[str, Any]) -> bool:
        product = Path(confirmation.get("product_path") or self.scope.product_path)
        if not product.exists():
            return False
        if confirmation.get("legacy"):
            return True
        expected_checksum = confirmation.get("checksum")
        if expected_checksum:
            try:
                if file_checksum(product) != expected_checksum:
                    return False
            except OSError:
                return False
        expected_records = confirmation.get("records")
        if expected_records is not None and self._format_for(confirmation) == "jsonl":
            if _count_jsonl_records(product) != int(expected_records):
                return False
        return True

    def _format_for(self, confirmation: dict[str, Any]) -> str:
        return str(confirmation.get("format") or self.scope.fmt)

    def verify(self) -> dict[str, Any]:
        """核对「自己的成品」和「确认位置」是否一致。"""
        with self._lock:
            confirmation = self.scoped_confirmation()
            if confirmation is None:
                return {
                    "ok": False,
                    "status": "missing",
                    "kind": self.scope.kind,
                    "source_key": self.scope.source_key,
                    "format": self.scope.fmt,
                    "product_path": str(self.scope.product_path),
                    "confirmation": None,
                }

            status = "confirmed"
            if confirmation.get("legacy"):
                status = "legacy"
            if not self._product_matches(confirmation):
                status = "missing_product" if not self.scope.product_path.exists() else "mismatch"

            actual_checksum = None
            actual_records = None
            if self.scope.product_path.exists():
                try:
                    actual_checksum = file_checksum(self.scope.product_path)
                except OSError:
                    actual_checksum = None
                if self._format_for(confirmation) == "jsonl":
                    actual_records = _count_jsonl_records(self.scope.product_path)

            return {
                "ok": status in ("confirmed", "legacy"),
                "status": status,
                "kind": self.scope.kind,
                "source_key": self.scope.source_key,
                "format": self._format_for(confirmation),
                "product_path": str(self.scope.product_path),
                "records": actual_records if actual_records is not None else confirmation.get("records"),
                "expected_records": confirmation.get("records"),
                "checksum": actual_checksum,
                "expected_checksum": confirmation.get("checksum"),
                "generation": confirmation.get("generation"),
                "batch_id": confirmation.get("batch_id"),
                "confirmation": confirmation,
            }

    # -- 清单 --

    def manifest_entries(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for line in _read_jsonl_lines(self.scope.manifest_path):
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                # 清单只接受完整行；半行既不能当成已确认成品，也不能吞掉历史。
                continue
            if isinstance(entry, dict) and self._same_product(entry):
                entries.append(entry)
        return entries

    def manifest(self, *, verify_history: bool = False) -> dict[str, Any]:
        """返回可核对的清单；可选逐条核对历史快照是否仍可访问。"""
        entries = self.manifest_entries()
        if verify_history:
            for entry in entries:
                history_path = Path(entry.get("history_path", ""))
                entry["history_accessible"] = bool(history_path and history_path.exists())
                entry["history_checksum"] = None
                entry["history_checksum_ok"] = False
                if entry["history_accessible"]:
                    try:
                        entry["history_checksum"] = file_checksum(history_path)
                        entry["history_checksum_ok"] = (
                            not entry.get("checksum")
                            or entry["history_checksum"] == entry.get("checksum")
                        )
                    except OSError:
                        entry["history_accessible"] = False
        return {
            "source_key": self.scope.source_key,
            "kind": self.scope.kind,
            "format": self.scope.fmt,
            "manifest_path": str(self.scope.manifest_path),
            "count": len(entries),
            "entries": entries,
        }

    def history_entries(self) -> list[dict[str, Any]]:
        """列出历史成品快照，按代次从旧到新。"""
        if not self.scope.history_dir.exists():
            return []
        items = []
        for path in sorted(self.scope.history_dir.iterdir()):
            if not path.is_file() or not path.name.endswith(self.scope.suffix or ".jsonl"):
                continue
            try:
                stat = path.stat()
                checksum = file_checksum(path)
            except OSError:
                continue
            items.append({
                "path": str(path),
                "name": path.name,
                "size": stat.st_size,
                "checksum": checksum,
                "records": _count_jsonl_records(path) if path.name.endswith(".jsonl") else None,
                "modified_at": stat.st_mtime,
            })
        return items

    def _next_generation(self) -> int:
        generations = [int(e.get("generation", 0) or 0) for e in self.manifest_entries()]
        for entry in (self.confirmation() or {},):
            if entry.get("generation"):
                generations.append(int(entry["generation"]))
        return max(generations, default=0) + 1

    # -- 写入生命周期 --

    def begin(self) -> StagedProduct:
        self.scope.staging_dir.mkdir(parents=True, exist_ok=True)
        self.scope.history_dir.mkdir(parents=True, exist_ok=True)
        batch_id = f"{int(time.time() * 1000):013d}-{uuid.uuid4().hex[:10]}"
        staging_path = self.scope.staging_dir / f"{self.scope.source_key}.{batch_id}.part"
        return StagedProduct(self, batch_id, staging_path)

    def _previous_snapshot(self, confirmation: dict[str, Any] | None) -> Path | None:
        """替换正式成品前，确保上一确认版本在 history 中有完整快照。"""
        if not self.scope.product_path.exists():
            return None
        if confirmation is not None and confirmation.get("history_path"):
            candidate = Path(confirmation["history_path"])
            if candidate.exists():
                return candidate
        generation = int((confirmation or {}).get("generation", 0) or 0)
        try:
            checksum = file_checksum(self.scope.product_path)
        except OSError:
            checksum = "unknown"
        history_path = self.scope.history_dir / (
            f"{generation:06d}-legacy-{checksum}{self.scope.suffix}"
        )
        if not history_path.exists():
            shutil.copyfile(self.scope.product_path, history_path)
            _fsync_path(history_path)
        return history_path

    def _restore_from(self, snapshot: Path) -> None:
        tmp = self.scope.product_path.with_name(
            f".{self.scope.product_path.name}.restore-{uuid.uuid4().hex[:8]}.tmp"
        )
        shutil.copyfile(snapshot, tmp)
        _fsync_path(tmp)
        _atomic_replace(tmp, self.scope.product_path)

    def _commit(
        self,
        batch: StagedProduct,
        *,
        expected_records: int | None,
        meta: dict[str, Any],
        checkpoint_extra: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock:
            batch._close()

            # 未写过任何记录时也要确认一个空成品（空导出 / 无生效规则）。
            if not batch.staging_path.exists():
                batch.staging_path.parent.mkdir(parents=True, exist_ok=True)
                batch.staging_path.write_bytes(b"")

            if expected_records is not None and batch.records != int(expected_records):
                batch.abort()
                raise RecordCountMismatch(
                    f"暂存明细记录数 {batch.records} 与预期 {expected_records} 不一致，拒绝确认"
                )
            if self.scope.fmt == "jsonl":
                staged_records = _count_jsonl_records(batch.staging_path)
                if staged_records != batch.records:
                    batch.abort()
                    raise RecordCountMismatch(
                        f"暂存明细实际落盘 {staged_records} 条，与已写计数 {batch.records} 不一致"
                    )

            checksum = file_checksum(batch.staging_path)
            generation = self._next_generation()
            previous = self.scoped_confirmation()
            previous_checkpoint = self.service._read_checkpoint(self.scope.checkpoint_name)
            previous_snapshot = self._previous_snapshot(previous)

            history_path = self.scope.history_dir / (
                f"{generation:06d}-{batch.batch_id}{self.scope.suffix}"
            )
            shutil.copyfile(batch.staging_path, history_path)
            _fsync_path(history_path)

            # 正式成品原子替换：读取方要么读到旧完整文件，要么读到新完整文件。
            _atomic_replace(batch.staging_path, self.scope.product_path)
            _fsync_path(self.scope.product_path)

            confirmed_at = time.time()
            state: dict[str, Any] = dict(checkpoint_extra)
            state.update({
                "source_key": self.scope.source_key,
                "kind": self.scope.kind,
                "format": self.scope.fmt,
                "product_path": str(self.scope.product_path),
                "history_path": str(history_path),
                "records": batch.records,
                "checksum": checksum,
                "batch_id": batch.batch_id,
                "generation": generation,
                "contract_version": CONTRACT_VERSION,
                "confirmed_at": confirmed_at,
            })

            entry = {
                "contract_version": CONTRACT_VERSION,
                "kind": self.scope.kind,
                "source_key": self.scope.source_key,
                "format": self.scope.fmt,
                "batch_id": batch.batch_id,
                "generation": generation,
                "status": "confirmed",
                "records": batch.records,
                "checksum": checksum,
                "product_path": str(self.scope.product_path),
                "history_path": str(history_path),
                "confirmed_at": confirmed_at,
                "meta": dict(meta),
            }

            try:
                self.service._write_checkpoint(self.scope.checkpoint_name, state)
                self._append_manifest(entry)
            except BaseException:
                # 确认位置或清单没有同步成功时回退到上一确认版本，避免半确认状态。
                self._rollback_commit(
                    previous_snapshot, previous_checkpoint, history_path
                )
                batch._aborted = True
                raise

            batch._committed = True
            return entry

    def _rollback_commit(
        self,
        previous_snapshot: Path | None,
        previous_checkpoint: dict[str, Any] | None,
        history_path: Path,
    ) -> None:
        try:
            if previous_snapshot is not None and previous_snapshot.exists():
                self._restore_from(previous_snapshot)
            elif self.scope.product_path.exists():
                self.scope.product_path.unlink()
        except OSError:
            pass
        try:
            if previous_checkpoint is None:
                self._checkpoint_path().unlink(missing_ok=True)
            else:
                self.service._write_checkpoint(
                    self.scope.checkpoint_name, dict(previous_checkpoint)
                )
        except OSError:
            pass
        try:
            history_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _append_manifest(self, entry: dict[str, Any]) -> None:
        self.scope.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(entry, ensure_ascii=False, default=str) + "\n"
        with open(self.scope.manifest_path, "a+b") as f:
            # 上一次写入可能断在半行；先补换行，避免新记录粘到残行上一起丢掉。
            f.seek(0, os.SEEK_END)
            if f.tell() > 0:
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b"\n":
                    f.write(b"\n")
            f.write(line.encode("utf-8"))
            f.flush()
            try:
                os.fsync(f.fileno())
            except (OSError, ValueError):
                pass

    # -- 失败恢复 --

    def recover(self) -> dict[str, Any]:
        """把正式成品恢复到最近一次确认的完整快照。

        重算失败时正式成品和确认位置本来不会被改动；这里额外覆盖
        “提交过程中断导致成品与确认位置不一致”的情况。
        """
        with self._lock:
            confirmation = self.scoped_confirmation()
            if confirmation is not None and self._product_matches(confirmation):
                return {
                    "ok": True,
                    "status": "confirmed",
                    "kind": self.scope.kind,
                    "source_key": self.scope.source_key,
                    "format": self.scope.fmt,
                    "confirmation": confirmation,
                }

            target: dict[str, Any] | None = None
            if confirmation is not None and confirmation.get("history_path"):
                candidate = Path(confirmation["history_path"])
                if candidate.exists():
                    target = confirmation
            if target is None:
                target = self._latest_manifest_entry(fmt=self.scope.fmt)
                if target is not None and not Path(target.get("history_path", "")).exists():
                    target = None

            if target is None:
                histories = [
                    item for item in self.history_entries()
                    if item["name"].endswith(self.scope.suffix or ".jsonl")
                ]
                if not histories:
                    return {
                        "ok": False,
                        "status": "no_history",
                        "kind": self.scope.kind,
                        "source_key": self.scope.source_key,
                        "format": self.scope.fmt,
                        "confirmation": confirmation,
                    }
                snapshot = Path(histories[-1]["path"])
                self._restore_from(snapshot)
                return {
                    "ok": True,
                    "status": "recovered_from_history",
                    "kind": self.scope.kind,
                    "source_key": self.scope.source_key,
                    "format": self.scope.fmt,
                    "history_path": str(snapshot),
                    "confirmation": self.confirmation(),
                }

            snapshot = Path(target["history_path"])
            self._restore_from(snapshot)

            # 确认位置缺失或落后于清单时，用被恢复批次的元数据补齐。
            if confirmation is None or confirmation.get("batch_id") != target.get("batch_id"):
                state = {
                    "source_key": self.scope.source_key,
                    "kind": self.scope.kind,
                    "format": self.scope.fmt,
                    "product_path": str(self.scope.product_path),
                    "history_path": str(snapshot),
                    "records": target.get("records"),
                    "checksum": target.get("checksum"),
                    "batch_id": target.get("batch_id"),
                    "generation": target.get("generation"),
                    "contract_version": CONTRACT_VERSION,
                    "confirmed_at": target.get("confirmed_at", time.time()),
                }
                self.service._write_checkpoint(self.scope.checkpoint_name, state)

            return {
                "ok": True,
                "status": "recovered",
                "kind": self.scope.kind,
                "source_key": self.scope.source_key,
                "format": self.scope.fmt,
                "history_path": str(snapshot),
                "confirmation": self.confirmation(),
            }

    def cleanup_staging(self, max_age_seconds: float = 24 * 3600) -> int:
        """清理中断留下的暂存文件；正式成品与清单不受影响。"""
        if not self.scope.staging_dir.exists():
            return 0
        cutoff = time.time() - max_age_seconds
        removed = 0
        for path in self.scope.staging_dir.glob("*"):
            if not path.is_file() or not path.name.endswith(".part"):
                continue
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
                    removed += 1
            except OSError:
                continue
        return removed
