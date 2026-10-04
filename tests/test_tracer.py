"""
游戏内分布式追踪与链路监控系统 - 验收测试
运行方式：python -m pytest tests/test_tracer.py -q
共 15 个测试用例
"""
import json
import time

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from monitoring.tracer import DistributedTracer


def make_slow_span(tracer, name, duration_ms):
    """构造一个耗时可控的跨度。"""
    span = tracer.start_span(name)
    span.start_time = time.time() - duration_ms / 1000.0
    tracer.end_span(span)
    return span


class TestDistributedTracer:
    def setup_method(self):
        self.system = DistributedTracer()

    # ---- 跨度开始：trace_id / span_id / 父节点 / 标签 ----
    def test_case_01(self):
        """start_span 必须分配 trace_id 和 span_id，且各跨度唯一。"""
        span = self.system.start_span("login")
        assert span.trace_id and span.span_id
        other = self.system.start_span("pay")
        assert other.span_id != span.span_id

    def test_case_02(self):
        """子跨度继承 trace_id 并记录父节点，可构建完整调用链。"""
        root = self.system.start_span("login")
        child = self.system.start_span("query_user")
        grandchild = self.system.start_span("db_select")
        assert child.trace_id == root.trace_id
        assert grandchild.trace_id == root.trace_id
        assert child.parent_span_id == root.span_id
        assert grandchild.parent_span_id == child.span_id
        assert root.parent_span_id is None

    def test_case_03(self):
        """跨度带标签：服务名、接口名、业务参数。"""
        span = self.system.start_span(
            "charge", service="pay-service",
            tags={"player_id": 393, "amount": 100},
        )
        assert span.service == "pay-service"
        assert span.tags["interface"] == "charge"
        assert span.tags["player_id"] == 393
        assert span.tags["amount"] == 100

    # ---- 跨度结束：状态 / 错误标记 / 上报 ----
    def test_case_04(self):
        """end_span 记录结束时间、耗时与 ok 状态。"""
        span = self.system.start_span("match")
        time.sleep(0.01)
        self.system.end_span(span)
        assert span.end_time is not None
        assert span.status == "ok"
        assert span.duration_ms >= 10

    def test_case_05(self):
        """end_span 携带异常时标记 error 状态与错误信息。"""
        span = self.system.start_span("save")
        self.system.end_span(span, error=ValueError("db down"))
        assert span.status == "error"
        assert "db down" in span.error
        assert span.tags["error"] is True

    def test_case_06(self):
        """结束的跨度必须上报：进入已上报列表并触发导出器。"""
        exported = []
        self.system.add_exporter(exported.append)
        span = self.system.start_span("login")
        self.system.end_span(span)
        assert span in self.system.reported
        assert exported == [span]

    # ---- 追踪上下文：传播与序列化 ----
    def test_case_07(self):
        """trace_context 返回当前跨度的 trace_id/span_id，无活跃跨度时为 None。"""
        assert self.system.trace_context() is None
        span = self.system.start_span("gateway")
        ctx = self.system.trace_context()
        assert ctx == {"trace_id": span.trace_id, "span_id": span.span_id}

    def test_case_08(self):
        """HTTP 传播：注入请求头 -> 对端提取 -> 下游跨度延续同一 trace_id。"""
        self.system.start_span("gateway")
        headers = self.system.inject_context()
        assert "x-trace-id" in headers and "traceparent" in headers

        downstream = DistributedTracer()
        ctx = downstream.extract_context(headers)
        remote = downstream.start_span(
            "battle", trace_id=ctx["trace_id"], parent_span_id=ctx["span_id"]
        )
        assert remote.trace_id == self.system.current_span().trace_id
        assert remote.parent_span_id == self.system.current_span().span_id

    def test_case_09(self):
        """上下文可序列化为 JSON 字符串在网络上传输并还原。"""
        span = self.system.start_span("gateway")
        payload = self.system.serialize_context()
        assert isinstance(payload, str)
        restored = DistributedTracer().deserialize_context(payload)
        assert restored == {"trace_id": span.trace_id, "span_id": span.span_id}

    def test_case_10(self):
        """gRPC 传播：元数据键值对同样能携带并还原追踪上下文。"""
        span = self.system.start_span("gateway")
        metadata = self.system.inject_grpc_metadata()
        assert ("x-trace-id", span.trace_id) in metadata
        ctx = DistributedTracer().extract_context(metadata)
        assert ctx["trace_id"] == span.trace_id
        assert ctx["span_id"] == span.span_id

    # ---- 性能分析：统计与火焰图 ----
    def test_case_11(self):
        """性能统计：count / avg / min / max / P95 / P99。"""
        for i in range(1, 101):
            make_slow_span(self.system, "tick", float(i))
        stats = self.system.performance_analysis()["operations"]["tick"]
        assert stats["count"] == 100
        assert stats["avg_ms"] == pytest.approx(50.5, abs=1.0)
        assert stats["min_ms"] == pytest.approx(1.0, abs=1.0)
        assert stats["max_ms"] == pytest.approx(100.0, abs=1.0)
        assert stats["p95_ms"] == pytest.approx(95.0, abs=1.0)
        assert stats["p99_ms"] == pytest.approx(99.0, abs=1.0)

    def test_case_12(self):
        """火焰图反映调用关系与耗时占比。"""
        root = self.system.start_span("login")
        root.start_time = time.time() - 0.1
        child = self.system.start_span("db")
        child.start_time = time.time() - 0.04
        self.system.end_span(child)
        self.system.end_span(root)

        graph = self.system.performance_analysis()["flame_graph"]
        assert graph["name"] == "login"
        assert graph["value"] == pytest.approx(100.0, abs=5.0)
        assert [c["name"] for c in graph["children"]] == ["db"]
        assert graph["children"][0]["value"] == pytest.approx(40.0, abs=5.0)

    # ---- 告警：阈值与去重 ----
    def test_case_13(self):
        """响应时间超过阈值触发慢调用告警，告警中带阈值信息。"""
        tracer = DistributedTracer({"alert_threshold_ms": 50})
        make_slow_span(tracer, "battle_tick", 200.0)
        alerts = tracer.alerting()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "slow_span"
        assert alerts[0].operation == "battle_tick"
        assert alerts[0].threshold_ms == 50
        assert alerts[0].value_ms >= 200.0

    def test_case_14(self):
        """冷却窗口内同一问题只告警一次，不同接口互不影响。"""
        tracer = DistributedTracer(
            {"alert_threshold_ms": 50, "alert_cooldown_s": 60}
        )
        for _ in range(3):
            make_slow_span(tracer, "battle_tick", 200.0)
        make_slow_span(tracer, "login", 200.0)
        alerts = tracer.alerting()
        assert len(alerts) == 2
        assert {a.operation for a in alerts} == {"battle_tick", "login"}

    def test_case_15(self):
        """异常调用触发错误告警；reset 清空全部状态。"""
        span = self.system.start_span("save")
        self.system.end_span(span, error=RuntimeError("boom"))
        alerts = self.system.alerting()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "error"
        assert alerts[0].trace_id == span.trace_id

        self.system.reset()
        assert self.system.alerting() == []
        assert self.system.reported == []
        assert self.system.trace_context() is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
