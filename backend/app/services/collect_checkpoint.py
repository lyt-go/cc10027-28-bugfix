"""采集位点管理：采集进度记录、位点提交、回滚和过期管理。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CollectCheckpointService(BaseModuleService):
    MODULE = "collect_checkpoint"
    ENTITY = "位点记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录编号", "关联源", "文件偏移", "处理行号", "位点时间", "校验摘要", "提交状态", "回滚位点"]
    REQUIRED_FIELDS = ["记录编号", "关联源", "文件偏移"]
    STATUS_ORDER = ["采集中", "已提交", "已回滚", "已过期"]
    ACTION_RULES = {"确认提交": "已提交", "发起回滚": "已回滚", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def save_checkpoint(self, source_key: str, offset: int, line_number: int) -> dict[str, Any]:
        """保存采集位点状态。"""
        state = {
            "source_key": source_key,
            "offset": offset,
            "line_number": line_number,
            "status": "active",
            "updated_at": time.time(),
        }
        self._write_checkpoint(source_key, state)

        ckpt_dir = self._data_dir("checkpoints")
        self._write_jsonl(ckpt_dir / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": offset,
            "line_number": line_number, "at": time.time(), "action": "save",
        }])
        return state

    def rollback_to(self, source_key: str, to_offset: int) -> dict[str, Any]:
        """将指定位点回滚到指定偏移位置。

        回滚（断点重放）请求必须记录真实旧位点，并核对目标位置是否落在
        原始日志的行末，避免重放时多出一段或丢失原来的行。
        """
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"error": f"位点 {source_key} 不存在"}

        # 先取真实旧位点再覆盖，避免把新位置当成历史位置记入重放请求
        previous_offset = cp.get("offset", 0)
        cp["previous_offset"] = previous_offset
        cp["offset"] = to_offset
        cp["status"] = "rolled_back"
        cp["rolled_back_at"] = time.time()

        # 核对关联原文：目标位置是否为某行的行末位点
        line_ends = [entry["end_offset"] for entry in self._line_end_offsets(source_key)]
        if line_ends:
            cp["rollback_verified"] = to_offset == 0 or to_offset in line_ends
            if not cp["rollback_verified"]:
                cp["nearest_line_end"] = min(line_ends, key=lambda end: abs(end - to_offset))
        self._write_checkpoint(source_key, cp)

        self._write_jsonl(self._data_dir("checkpoints") / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": to_offset,
            "previous_offset": previous_offset, "at": time.time(), "action": "rollback",
        }])
        return cp

    def get_checkpoint_state(self, source_key: str) -> dict[str, Any]:
        """获取指定源的位点信息，位点详情与采集原文核对后返回。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"source_key": source_key, "status": "no_checkpoint", "offset": 0}

        confirm = self.confirm_resume_offset(source_key)
        cp["offset"] = confirm["offset"]
        cp["line_number"] = confirm["line_number"]
        cp["resume_verified"] = confirm["verified"]
        if confirm.get("corrected"):
            cp["resume_corrected"] = True
            cp["stored_offset_before"] = confirm.get("stored_offset")
        return cp

    def list_all_checkpoints(self) -> list[dict[str, Any]]:
        """列出所有位点状态。"""
        all_cps = []
        for src in self._collect_sources():
            key = src["源编号"]
            cp = self._read_checkpoint(key)
            if cp:
                all_cps.append(cp)
        return all_cps

    def commit_checkpoint(self, source_key: str) -> dict[str, Any]:
        """提交采集位点并自动触发下游格式化输出。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"error": f"位点 {source_key} 不存在"}
        cp["status"] = "committed"
        cp["committed_at"] = time.time()
        self._write_checkpoint(source_key, cp)

        # 写入位点历史
        self._write_jsonl(self._data_dir("checkpoints") / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": cp.get("offset", 0),
            "at": time.time(), "action": "commit",
        }])

        result = {
            "source_key": source_key,
            "committed_at": time.time(),
            "offset": cp.get("offset", 0),
            "line_number": cp.get("line_number", 0),
        }

        # 提交后自动触发格式输出
        export_r = self._trigger_export_for_source(source_key)
        if "error" not in export_r:
            result["auto_export"] = export_r

        # 同时触发质量诊断
        quality_r = self._trigger_quality_diagnosis(source_key)
        if "error" not in quality_r:
            result["auto_quality"] = quality_r

        return result

    def verify_checkpoint(self, source_key: str, expected_checksum: str) -> dict[str, Any]:
        """验证采集位点的校验摘要。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"source_key": source_key, "status": "no_checkpoint", "match": False}

        actual = cp.get("checksum", "")
        match = actual == expected_checksum
        cp["last_verified_at"] = time.time()
        cp["last_verified_match"] = match
        self._write_checkpoint(source_key, cp)

        return {"source_key": source_key, "match": match, "expected": expected_checksum, "actual": actual}

    # -- 逐行累计字节位点 --

    def _line_end_offsets(self, source_key: str) -> list[dict[str, Any]]:
        """按原始日志的 UTF-8 字节及换行，逐行累计每行的行末位点。"""
        log_file = self._data_dir("logs") / f"{source_key}.jsonl"
        records = self._read_jsonl(log_file)
        ends: list[dict[str, Any]] = []
        offset = 0
        for index, rec in enumerate(records, start=1):
            raw = rec.get("raw", "")
            offset += len(str(raw).encode("utf-8")) + 1
            ends.append({
                "line_no": rec.get("line_no") or index,
                "end_offset": offset,
                "recorded_offset": rec.get("offset"),
            })
        return ends

    def confirm_resume_offset(self, source_key: str) -> dict[str, Any]:
        """读取逐行累计字节位点，确认复采的实际起点。

        存量位置与原文对不上时，按记录位点/行号核对关联原文，把位点修正为
        对应原始行的行末位置，避免断点重放多出一段或丢失原来的行。
        """
        cp = self._read_checkpoint(source_key)
        lines = self._line_end_offsets(source_key)

        if cp is None:
            # 无存量位点：以原文逐行累计的末尾为起点
            offset = lines[-1]["end_offset"] if lines else 0
            return {
                "source_key": source_key, "offset": offset, "line_number": len(lines),
                "verified": True, "corrected": False,
                "status": "derived_from_source" if lines else "no_checkpoint",
            }

        try:
            stored_offset = int(cp.get("offset", 0) or 0)
        except (TypeError, ValueError):
            stored_offset = 0
        try:
            stored_line = int(cp.get("line_number", 0) or 0)
        except (TypeError, ValueError):
            stored_line = 0

        result: dict[str, Any] = {
            "source_key": source_key,
            "stored_offset": stored_offset,
            "stored_line_number": stored_line,
            "offset": stored_offset,
            "line_number": stored_line,
            "verified": False,
            "corrected": False,
        }

        if not lines:
            # 无采集原文可核对，信任存量位点
            result["status"] = "no_source_data"
            return result

        # 存量位置落在初始位置或某行行末 → 确认
        if stored_offset == 0 and stored_line == 0:
            result["verified"] = True
            result["status"] = "confirmed"
            return result

        matched = next(
            (i for i, entry in enumerate(lines) if entry["end_offset"] == stored_offset),
            None,
        )
        if matched is not None:
            result["verified"] = True
            result["status"] = "confirmed"
            result["offset"] = stored_offset
            result["line_number"] = matched + 1
            if stored_line != matched + 1:
                # 位点一致但行号与原文不符，随位点一并修正
                self._repair_checkpoint(source_key, cp, stored_offset, stored_offset, matched + 1)
                result["corrected"] = True
            return result

        # 存量位置对不上 → 核对关联原文，定位对应原始行
        by_record = next(
            (i for i, entry in enumerate(lines) if entry["recorded_offset"] == stored_offset),
            None,
        )
        if by_record is not None:
            # 历史逐行偏移写错时，按记录位点找到对应原始行
            new_line = by_record + 1
        elif 0 < stored_line <= len(lines):
            # 按行号取原始行末
            new_line = stored_line
        else:
            # 行号超出原文：以原文末尾为界，避免重放丢失原来的行
            new_line = len(lines)
        new_offset = lines[new_line - 1]["end_offset"]

        self._repair_checkpoint(source_key, cp, stored_offset, new_offset, new_line)
        result.update({
            "offset": new_offset,
            "line_number": new_line,
            "corrected": True,
            "status": "corrected",
        })
        return result

    def _repair_checkpoint(
        self,
        source_key: str,
        cp: dict[str, Any],
        old_offset: int,
        new_offset: int,
        new_line: int,
    ) -> None:
        """把核对后的行末位点写回存量位点，并记入位点历史。"""
        cp["offset"] = new_offset
        cp["line_number"] = new_line
        cp["resume_corrected_from"] = old_offset
        cp["corrected_at"] = time.time()
        self._write_checkpoint(source_key, cp)
        self._write_jsonl(self._data_dir("checkpoints") / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": new_offset, "line_number": new_line,
            "previous_offset": old_offset, "at": time.time(), "action": "correct",
        }])
