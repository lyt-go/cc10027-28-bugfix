"""日志源管理：接入日志源、模拟日志采集、管理源状态与生命周期。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LogSourceService(BaseModuleService):
    MODULE = "log_source"
    ENTITY = "日志源"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源编号", "源名称", "日志类型", "接入方式", "路径模版", "字符编码", "轮转规则", "源状态"]
    REQUIRED_FIELDS = ["源编号", "源名称", "日志类型"]
    STATUS_ORDER = ["草稿", "待校验", "已接入", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认接入": "已接入", "停用源": "已停用"}
    NEGATIVE_ACTIONS = ["停用源"]

    def onboard_source(self, values: dict[str, Any]) -> dict[str, Any]:
        """接入新日志源并初始化采集位点。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"缺少必填字段: {missing}")
        # 初始化采集位点
        source_key = entry["源编号"]
        self._write_checkpoint(source_key, {
            "source_key": source_key,
            "offset": 0,
            "line_number": 0,
            "status": "active",
            "created_at": time.time(),
        })
        # 写入接入日志
        self._write_jsonl(
            self._data_dir("logs") / "onboard.jsonl",
            [{"action": "onboard", "source_key": source_key, "name": entry["源名称"], "at": time.time()}]
        )
        return entry

    def activate_source(self, source_id: int) -> dict[str, Any]:
        """确认接入日志源，并自动创建配套的解析规则、缓冲配置和代理注册。"""
        entry, msg = self.run_action(source_id, "确认接入")
        if entry is None:
            return {"error": msg, "source_id": source_id}
        source_key = entry["源编号"]

        triggers = {}
        # 自动创建配套log_parser规则
        try:
            parser_svc = self._safe_import("app.services.log_parser", "LogParserService")()
            parser_entry = self._auto_create_seed("log_parser", {
                "规则编号": f"PARSER-{source_key}",
                "关联源": source_key,
                "正则表达式": "parser_auto",
                "字段提取": "auto_extract",
                "时间格式": "auto",
                "日志级别": "auto",
                "多行合并": "否",
                "规则版本": "v1.0",
            })
            if parser_entry:
                triggers["log_parser"] = {"created": parser_entry["规则编号"], "status": "已生效"}
        except Exception as e:
            triggers["log_parser"] = {"error": str(e)}

        # 自动创建配套stream_buffer配置
        try:
            buffer_svc = self._safe_import("app.services.stream_buffer", "StreamBufferService")()
            buffer_entry = self._auto_create_seed("stream_buffer", {
                "配置编号": f"BUFFER-{source_key}",
                "关联源": source_key,
                "缓冲区大小": "1024",
                "批量大小": "100",
                "刷写间隔": "30",
                "内存上限": "512",
                "溢出策略": "block",
                "配置版本": "v1.0",
            })
            if buffer_entry:
                triggers["stream_buffer"] = {"created": buffer_entry["配置编号"], "status": "已生效"}
        except Exception as e:
            triggers["stream_buffer"] = {"error": str(e)}

        # 自动注册采集代理
        try:
            agent_svc = self._safe_import("app.services.agent_heartbeat", "AgentHeartbeatService")()
            agent_entry = self._auto_create_seed("agent_heartbeat", {
                "代理编号": f"AGENT-{source_key}",
                "部署主机": f"host-{source_key}",
                "代理版本": "v2.1",
                "上次心跳": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
                "采集量": "0",
                "错误率": "0",
                "内存占用": "0",
                "代理状态": "运行中",
            })
            if agent_entry:
                triggers["agent_heartbeat"] = {"created": agent_entry["代理编号"], "status": "运行中"}
        except Exception as e:
            triggers["agent_heartbeat"] = {"error": str(e)}

        return {
            "source_entry": entry,
            "source_key": source_key,
            "activated_at": time.time(),
            "auto_triggers": triggers,
        }

    def collect_from_source(self, source_id: int, batch_size: int = 100, lines: list[str] | None = None) -> dict[str, Any]:
        """从指定日志源采集一批日志行到本地日志文件。"""
        source = store.find(self.MODULE, source_id)
        if source is None:
            return {"error": f"日志源 {source_id} 不存在"}
        if source.get("status") != "已接入":
            return {"error": f"日志源 {source_id} 未接入，当前状态: {source.get('status')}"}

        cp = self._read_checkpoint(source["源编号"]) or {"offset": 0, "line_number": 0}
        # 续采入口：读取逐行累计字节位点，核对存量位置后确认本批实际起点
        cp = self._confirm_resume_start(source["源编号"], cp)

        if lines is None:
            lines = self._simulate_log_lines(source, count=batch_size)
        else:
            lines = [str(line) for line in lines]
        start_line = cp["line_number"]

        records = []
        offset = cp["offset"]
        for i, line in enumerate(lines):
            # 行末位置按原始日志的 UTF-8 字节及换行逐行累计
            offset += len(line.encode("utf-8")) + 1
            records.append({
                "source_key": source["源编号"],
                "source_name": source["源名称"],
                "line_no": start_line + i + 1,
                "offset": offset,
                "raw": line,
                "collected_at": time.time(),
            })

        output_file = self._data_dir("logs") / f"{source['源编号']}.jsonl"
        written = self._write_jsonl(output_file, records)

        new_line = start_line + written
        # 批末确认位置与最后一行的累计行末位点一致
        new_offset = offset
        self._write_checkpoint(source["源编号"], {
            "source_key": source["源编号"],
            "offset": new_offset,
            "line_number": new_line,
            "status": "active",
            "last_collected_at": time.time(),
        })

        result = {
            "source_key": source["源编号"],
            "batch_size": batch_size,
            "written": written,
            "start_line": start_line,
            "end_line": new_line,
            "resume_offset": cp["offset"],
            "checkpoint_offset": new_offset,
        }

        # 采集后自动触发解析
        parse_r = self._trigger_parse_for_source(source["源编号"])
        if "error" not in parse_r:
            result["auto_parse"] = parse_r
            # 解析成功后自动触发映射
            map_r = self._trigger_mapping_for_source(source["源编号"])
            if "error" not in map_r:
                result["auto_mapping"] = map_r

        return result

    def _confirm_resume_start(self, source_key: str, cp: dict[str, Any]) -> dict[str, Any]:
        """续采前核对存量位点与采集原文，返回确认后的位点作为本批实际起点。"""
        try:
            cls = self._safe_import("app.services.collect_checkpoint", "CollectCheckpointService")
            confirmed = cls().confirm_resume_offset(source_key)
            if "offset" in confirmed:
                cp = dict(cp)
                cp["offset"] = confirmed["offset"]
                cp["line_number"] = confirmed.get("line_number", cp.get("line_number", 0))
        except Exception:
            pass
        return cp

    def get_active_sources(self) -> list[dict[str, Any]]:
        """获取所有已接入的活跃日志源。"""
        return self._collect_sources()