"""逐行累计字节位点回归测试。

场景：同一批两行不同长度的文本（其中一行含多字节字符），
核对每行结束的 UTF-8 字节位点、批末确认位置的一致性，
以及从旧存量位置恢复（断点重放）的结果。
"""
import json
import sys
from pathlib import Path

# 确保 backend 在 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.services.base import DATA_ROOT
from app.services.log_source import LogSourceService
from app.services.collect_checkpoint import CollectCheckpointService
from app.services.log_parser import LogParserService
from app.store import store

TEST_SOURCE_KEY = "LOG_-9001"
TEST_SOURCE_ID = 9001

# 同一批两行：不同长度，第二行含多字节字符
LINE1 = "INFO boot ok"
LINE2 = "用户登录成功 订单号=ORD-9876543210 支付完成"


def _line_end(*lines: str) -> list[int]:
    """按原始日志的 UTF-8 字节及换行逐行累计行末位点。"""
    ends = []
    offset = 0
    for line in lines:
        offset += len(line.encode("utf-8")) + 1
        ends.append(offset)
    return ends


def _fresh_source() -> dict:
    """准备一个已接入的测试日志源，并清理其历史产物。"""
    rows = store.rows("log_source")
    source = None
    for row in rows:
        if row.get("源编号") == TEST_SOURCE_KEY:
            source = row
            break
    if source is None:
        source = {
            "id": TEST_SOURCE_ID,
            "源编号": TEST_SOURCE_KEY,
            "源名称": "逐行位点测试源",
            "日志类型": "test",
            "接入方式": "api",
            "路径模版": "/var/log/test",
            "字符编码": "UTF-8",
            "轮转规则": "daily",
            "源状态": "已接入",
            "status": "已接入",
            "pending": False,
            "abnormal": False,
        }
        rows.append(source)
    source["status"] = "已接入"

    for path in [
        DATA_ROOT / "logs" / f"{TEST_SOURCE_KEY}.jsonl",
        DATA_ROOT / "logs" / f"{TEST_SOURCE_KEY}_parsed.jsonl",
        DATA_ROOT / "logs" / f"{TEST_SOURCE_KEY}_parse_errors.jsonl",
        DATA_ROOT / "checkpoints" / f"{TEST_SOURCE_KEY}.json",
    ]:
        if path.exists():
            path.unlink()
    return source


def _read_log_records() -> list[dict]:
    log_file = DATA_ROOT / "logs" / f"{TEST_SOURCE_KEY}.jsonl"
    if not log_file.exists():
        return []
    with open(log_file, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_batch_line_offsets_cumulative_bytes():
    """同一批两行（含多字节字符）：逐行位点按 UTF-8 字节及换行累计。"""
    source = _fresh_source()
    svc = LogSourceService()

    # 多字节行：按字符和按存储字节计数必须落在不同位置
    assert len(LINE2.encode("utf-8")) != len(LINE2), "第二行应含多字节字符"

    result = svc.collect_from_source(source["id"], lines=[LINE1, LINE2])
    assert result.get("written") == 2, f"应写入两行: {result}"

    e1, e2 = _line_end(LINE1, LINE2)
    records = _read_log_records()
    assert len(records) == 2, f"日志文件应有两行: {records}"
    assert records[0]["line_no"] == 1 and records[1]["line_no"] == 2
    # 第一行行末 = 自身字节 + 换行
    assert records[0]["offset"] == e1, f"第一行行末位点应为 {e1}: {records[0]['offset']}"
    # 第二行行末 = 第一行累计 + 自身字节 + 换行（不是批次起点 + 自身长度）
    assert records[1]["offset"] == e2, f"第二行行末位点应为累计 {e2}: {records[1]['offset']}"
    # 按字符计数会落在不同位置，这里必须是字节口径
    assert records[1]["offset"] != e1 + len(LINE2) + 1

    # 批末确认位置与最后一行的累计行末位点一致
    assert result["checkpoint_offset"] == e2
    cp = CollectCheckpointService().get_checkpoint_state(TEST_SOURCE_KEY)
    assert cp["offset"] == e2 and cp["line_number"] == 2


def test_resume_continues_from_line_end():
    """续采入口：从查得的原始行末继续读取，位点继续逐行累计。"""
    source = _fresh_source()
    svc = LogSourceService()
    svc.collect_from_source(source["id"], lines=[LINE1, LINE2])
    _, e2 = _line_end(LINE1, LINE2)

    line3 = "WARN 磁盘空间不足 剩余=5%"
    result = svc.collect_from_source(source["id"], lines=[line3])
    records = _read_log_records()
    assert len(records) == 3, "续采应只追加新行"
    assert records[2]["line_no"] == 3
    assert records[2]["offset"] == e2 + len(line3.encode("utf-8")) + 1
    assert result["resume_offset"] == e2, "续采实际起点应为上一批末行末"
    assert result["checkpoint_offset"] == records[2]["offset"]


def test_confirm_resume_repairs_legacy_offsets():
    """旧存量位置（历史错误逐行偏移）恢复：核对原文修正到真实行末。"""
    source = _fresh_source()
    svc = LogSourceService()
    cp_svc = CollectCheckpointService()

    e1, e2 = _line_end(LINE1, LINE2)
    # 模拟历史错误：逐行偏移只算本行长度（未累计），第二行位点串位
    legacy_offset = len(LINE2.encode("utf-8")) + 1
    assert legacy_offset != e2 and legacy_offset != e1
    legacy_records = [
        {"source_key": TEST_SOURCE_KEY, "line_no": 1, "offset": e1, "raw": LINE1},
        {"source_key": TEST_SOURCE_KEY, "line_no": 2, "offset": legacy_offset, "raw": LINE2},
    ]
    log_file = DATA_ROOT / "logs" / f"{TEST_SOURCE_KEY}.jsonl"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "w", encoding="utf-8") as f:
        for rec in legacy_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # 历史上这类错误位置已被记入位点（另一份重放请求）
    cp_svc.save_checkpoint(TEST_SOURCE_KEY, legacy_offset, 2)

    # 续采入口核对：存量位置对不上 → 核对关联原文，修正到第二行真实行末
    confirm = cp_svc.confirm_resume_offset(TEST_SOURCE_KEY)
    assert confirm["corrected"] is True, f"旧存量位置应被修正: {confirm}"
    assert confirm["offset"] == e2, f"应修正到第二行行末 {e2}: {confirm['offset']}"
    assert confirm["line_number"] == 2

    # 位点详情与原文一致
    state = cp_svc.get_checkpoint_state(TEST_SOURCE_KEY)
    assert state["offset"] == e2

    # 从修正后的位置复采：不多出一段，也不丢失原来的行
    line3 = "ERROR 支付回调失败 订单号=ORD-9876543210"
    svc.collect_from_source(source["id"], lines=[line3])
    records = _read_log_records()
    assert len(records) == 3, "复采应只追加新行，不应重放已有行"
    assert records[1]["raw"] == LINE2, "原来的行不应丢失"
    assert records[2]["line_no"] == 3
    assert records[2]["offset"] == e2 + len(line3.encode("utf-8")) + 1


def test_rollback_records_true_previous_offset():
    """回滚（断点重放）请求记录真实旧位点，并核对目标位置是否行末。"""
    source = _fresh_source()
    svc = LogSourceService()
    cp_svc = CollectCheckpointService()
    svc.collect_from_source(source["id"], lines=[LINE1, LINE2])
    e1, e2 = _line_end(LINE1, LINE2)

    rolled = cp_svc.rollback_to(TEST_SOURCE_KEY, e1)
    assert rolled["offset"] == e1
    assert rolled["previous_offset"] == e2, "回滚请求应记录真实旧位点而非目标位点"
    assert rolled.get("rollback_verified") is True, "行末位置应通过原文核对"

    # 回滚到非行末位置：核对原文应标记未确认，复采前修正到真实行末
    bad = cp_svc.rollback_to(TEST_SOURCE_KEY, e1 + 1)
    assert bad.get("rollback_verified") is False, "非行末位置应标记未确认"
    confirm = cp_svc.confirm_resume_offset(TEST_SOURCE_KEY)
    assert confirm["corrected"] is True
    assert confirm["offset"] == e2, f"应按行号核对到第二行行末 {e2}: {confirm['offset']}"


def test_collected_raw_still_parses_by_line():
    """采集端写出的原文仍能按对应行解析。"""
    source = _fresh_source()
    svc = LogSourceService()
    svc.collect_from_source(source["id"], lines=[LINE1, LINE2])

    result = LogParserService().parse_log_file(TEST_SOURCE_KEY)
    assert result.get("total") == 2, f"应解析到两行原文: {result}"
    assert result.get("parsed", 0) + result.get("errors", 0) == 2

    parsed_file = DATA_ROOT / "logs" / f"{TEST_SOURCE_KEY}_parsed.jsonl"
    if parsed_file.exists():
        with open(parsed_file, "r", encoding="utf-8") as f:
            parsed = [json.loads(line) for line in f if line.strip()]
        for rec in parsed:
            assert rec.get("raw") in (LINE1, LINE2), "解析记录应保留原始行"
            assert rec.get("line_no") in (1, 2), "解析记录应保留对应行号"


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
        except AssertionError as exc:
            failed += 1
            print(f"FAIL {test.__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} 通过")
    sys.exit(1 if failed else 0)
