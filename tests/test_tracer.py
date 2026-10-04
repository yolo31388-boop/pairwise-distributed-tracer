"""
游戏内分布式追踪与链路监控系统 - 验收测试
运行方式：python -m pytest tests/test_tracer.py -q
共 15 个测试用例
"""
import json
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from monitoring.tracer import DistributedTracer, Span, TraceContext


class TestDistributedTracer:
    def setup_method(self):
        self.system = DistributedTracer()

    # ---- 跨度开始：trace_id / span_id / 父节点 / 标签 ----

    def test_case_01(self):
        """start_span 生成 trace_id 和 span_id"""
        span = self.system.start_span("login")
        assert span.trace_id
        assert span.span_id
        assert span.trace_id != span.span_id
        assert span.start_time > 0

    def test_case_02(self):
        """子跨度继承 trace_id 并记录父节点，可构建调用链"""
        root = self.system.start_span("battle")
        child = self.system.start_span("damage_calc", parent=root)
        grandchild = self.system.start_span("rng_roll")
        assert child.trace_id == root.trace_id
        assert child.parent_id == root.span_id
        # 隐式父节点：默认挂到当前活跃跨度栈顶
        assert grandchild.parent_id == child.span_id
        assert grandchild.trace_id == root.trace_id

    def test_case_03(self):
        """跨度带标签：服务名、接口名、自定义参数"""
        span = self.system.start_span(
            "matchmaking", service="lobby", tags={"player_id": 42, "mode": "ranked"}
        )
        assert span.service == "lobby"
        assert span.tags["service"] == "lobby"
        assert span.tags["operation"] == "matchmaking"
        assert span.tags["player_id"] == 42
        assert span.tags["mode"] == "ranked"

    # ---- 跨度结束：状态 / 错误标记 / 上报 ----

    def test_case_04(self):
        """end_span 记录结束时间与状态"""
        span = self.system.start_span("quest_complete")
        self.system.end_span(span, status="ok")
        assert span.end_time is not None
        assert span.end_time >= span.start_time
        assert span.status == "ok"
        assert span.error is False
        assert span.duration_ms >= 0

    def test_case_05(self):
        """end_span 异常时打错误标记"""
        span = self.system.start_span("payment")
        self.system.end_span(span, error=RuntimeError("gateway timeout"))
        assert span.status == "error"
        assert span.error is True
        assert "gateway timeout" in span.error_message

    def test_case_06(self):
        """end_span 后跨度被上报，不只在内存活跃栈里"""
        exported = []
        tracer = DistributedTracer(config={"exporter": exported.append})
        span = tracer.start_span("inventory_sync")
        tracer.end_span(span)
        assert span in tracer.reported_spans
        assert exported == [span]
        assert span not in tracer._active

    # ---- 追踪上下文：传播 / 序列化 ----

    def test_case_07(self):
        """HTTP 传播：注入 traceparent 头，下游提取后 trace_id 不丢失"""
        span = self.system.start_span("gateway")
        headers = self.system.inject_context({}, format="http")
        assert "traceparent" in headers
        ctx = self.system.extract_context(headers, format="http")
        assert ctx is not None
        assert ctx.trace_id == span.trace_id
        assert ctx.span_id == span.span_id
        # 下游服务基于提取的上下文继续追踪
        downstream = self.system.start_span("auth", context=ctx)
        assert downstream.trace_id == span.trace_id
        assert downstream.parent_id == span.span_id

    def test_case_08(self):
        """gRPC 传播：metadata 携带 trace_id/span_id"""
        span = self.system.start_span("rank_service")
        metadata = self.system.inject_context({}, format="grpc")
        assert metadata["x-trace-id"] == span.trace_id
        assert metadata["x-span-id"] == span.span_id
        ctx = self.system.extract_context(metadata, format="grpc")
        assert ctx.trace_id == span.trace_id
        assert ctx.span_id == span.span_id

    def test_case_09(self):
        """上下文可序列化为 JSON 并在网络传输后还原"""
        span = self.system.start_span("chat")
        ctx = self.system.trace_context(span)
        payload = ctx.serialize()
        assert isinstance(payload, str)
        json.loads(payload)  # 必须是合法 JSON
        restored = TraceContext.deserialize(payload)
        assert restored.trace_id == ctx.trace_id
        assert restored.span_id == ctx.span_id
        assert restored.to_dict() == ctx.to_dict()

    # ---- 性能分析：统计 / 火焰图 ----

    def test_case_10(self):
        """performance_analysis 统计 count/avg/P95/P99"""
        for i in range(100):
            span = self.system.start_span("frame_tick")
            span.start_time -= 0.001 * (i + 1)  # 模拟 1~100ms 耗时
            self.system.end_span(span)
        stats = self.system.performance_analysis()["operations"]["frame_tick"]
        assert stats["count"] == 100
        assert stats["avg_ms"] == pytest.approx(50.5, abs=0.5)
        assert stats["min_ms"] == pytest.approx(1.0, abs=0.1)
        assert stats["max_ms"] == pytest.approx(100.0, abs=0.1)
        assert stats["p95_ms"] == pytest.approx(95.05, abs=0.5)
        assert stats["p99_ms"] == pytest.approx(99.01, abs=0.5)
        assert stats["p99_ms"] >= stats["p95_ms"] >= stats["avg_ms"]

    def test_case_11(self):
        """火焰图按父子关系组织，调用链可见"""
        root = self.system.start_span("battle_round")
        child = self.system.start_span("skill_cast", parent=root)
        leaf = self.system.start_span("buff_apply", parent=child)
        for s in (leaf, child, root):
            self.system.end_span(s)
        flame = self.system.performance_analysis()["flame_graph"]
        assert len(flame) == 1
        assert flame[0]["name"] == "battle_round"
        assert flame[0]["children"][0]["name"] == "skill_cast"
        assert flame[0]["children"][0]["children"][0]["name"] == "buff_apply"
        assert flame[0]["value"] >= 0

    # ---- 告警：阈值 / 错误 / 去重 ----

    def test_case_12(self):
        """响应时间超阈值触发慢调用告警"""
        tracer = DistributedTracer(config={"alert_threshold_ms": 50.0})
        span = tracer.start_span("db_query")
        span.start_time -= 0.2  # 模拟 200ms
        tracer.end_span(span)
        alerts = tracer.alerting()
        assert len(alerts) == 1
        assert alerts[0]["type"] == "slow"
        assert alerts[0]["operation"] == "db_query"
        assert alerts[0]["duration_ms"] >= 200.0
        assert alerts[0]["threshold_ms"] == 50.0

    def test_case_13(self):
        """异常跨度触发错误告警"""
        span = self.system.start_span("login")
        self.system.end_span(span, error="db connection lost")
        alerts = self.system.alerting()
        assert len(alerts) == 1
        assert alerts[0]["type"] == "error"
        assert "db connection lost" in alerts[0]["message"]

    def test_case_14(self):
        """同一问题在冷却期内去重，不重复告警"""
        tracer = DistributedTracer(
            config={"alert_threshold_ms": 10.0, "alert_cooldown_s": 60.0}
        )
        now = time.time()
        for _ in range(100):  # 同一个问题出现 100 次
            span = tracer.start_span("laggy_op")
            span.start_time -= 0.1
            tracer.end_span(span)
        first = tracer.alerting(now=now)
        second = tracer.alerting(now=now + 1)
        assert len(first) == 1          # 100 次慢调用只告警 1 次
        assert second == []             # 冷却期内不重复
        third = tracer.alerting(now=now + 61)
        assert len(third) == 1          # 冷却期过后可再次告警

    # ---- 端到端：完整链路 ----

    def test_case_15(self):
        """跨服务完整链路：上下文传播 -> 调用链 -> 分析 -> 告警 -> reset"""
        tracer = DistributedTracer(
            config={"alert_threshold_ms": 100.0, "service_name": "gateway"}
        )
        root = tracer.start_span("enter_battle", tags={"player_id": 7})
        headers = tracer.inject_context({}, format="http")

        # 下游服务从 HTTP 头恢复上下文，trace_id 不丢失
        ctx = tracer.extract_context(headers, format="http")
        remote = tracer.start_span("battle_service", service="battle", context=ctx)
        remote.start_time -= 0.5  # 模拟下游 500ms 慢调用
        tracer.end_span(remote, error="battle logic crashed")
        tracer.end_span(root)

        assert remote.trace_id == root.trace_id
        assert remote.parent_id == root.span_id

        analysis = tracer.performance_analysis()
        assert set(analysis["operations"]) == {"enter_battle", "battle_service"}
        assert analysis["operations"]["battle_service"]["error_count"] == 1
        assert analysis["flame_graph"][0]["children"][0]["name"] == "battle_service"

        alerts = tracer.alerting()
        types = {a["type"] for a in alerts}
        assert types == {"slow", "error"}

        assert len(tracer.reported_spans) == 2
        tracer.reset()
        assert tracer.reported_spans == []
        assert tracer.performance_analysis()["operations"] == {}
        assert tracer.alerts == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
