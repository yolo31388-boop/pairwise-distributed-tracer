"""
游戏内分布式追踪与链路监控系统 - 核心模块

功能：
- start_span / end_span：带 trace_id、span_id、父节点、标签、状态、错误标记的跨度生命周期
- trace_context / inject_context / extract_context：跨服务追踪上下文传播与序列化（HTTP / gRPC）
- performance_analysis：耗时统计（avg / P95 / P99 等）与火焰图
- alerting：基于阈值与错误的告警，支持去重
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
import json
import math
import time
import uuid


@dataclass
class Config:
    alert_threshold_ms: float = 1000.0
    alert_cooldown_s: float = 60.0
    service_name: str = "game-server"


def _new_trace_id() -> str:
    return uuid.uuid4().hex


def _new_span_id() -> str:
    return uuid.uuid4().hex[:16]


@dataclass
class Span:
    """一个跨度：trace_id + span_id + 父节点 + 标签 + 状态 + 错误标记。"""

    operation: str
    trace_id: str
    span_id: str
    parent_id: Optional[str] = None
    service: str = "game-server"
    tags: Dict = field(default_factory=dict)
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = "unset"  # ok / error
    error: bool = False
    error_message: Optional[str] = None

    @property
    def duration_ms(self) -> float:
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time) * 1000.0

    @property
    def finished(self) -> bool:
        return self.end_time is not None

    def to_dict(self) -> Dict:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "operation": self.operation,
            "service": self.service,
            "tags": dict(self.tags),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error": self.error,
            "error_message": self.error_message,
        }


@dataclass
class TraceContext:
    """追踪上下文：可序列化，可通过 HTTP 头 / gRPC metadata 传播。"""

    trace_id: str
    span_id: str
    parent_id: Optional[str] = None
    sampled: bool = True

    HTTP_HEADER = "traceparent"
    GRPC_TRACE_KEY = "x-trace-id"
    GRPC_SPAN_KEY = "x-span-id"

    def to_dict(self) -> Dict:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "sampled": self.sampled,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "TraceContext":
        return cls(
            trace_id=data["trace_id"],
            span_id=data["span_id"],
            parent_id=data.get("parent_id"),
            sampled=bool(data.get("sampled", True)),
        )

    def serialize(self) -> str:
        """序列化为 JSON 字符串，可在网络上传输。"""
        return json.dumps(self.to_dict())

    @classmethod
    def deserialize(cls, payload: str) -> "TraceContext":
        return cls.from_dict(json.loads(payload))

    def inject_http(self, carrier: Dict) -> Dict:
        """注入 HTTP 头（W3C traceparent 格式）。"""
        flags = "01" if self.sampled else "00"
        carrier[self.HTTP_HEADER] = "00-{}-{}-{}".format(
            self.trace_id, self.span_id, flags
        )
        return carrier

    @classmethod
    def extract_http(cls, carrier: Dict) -> Optional["TraceContext"]:
        header = carrier.get(cls.HTTP_HEADER)
        if not header:
            return None
        parts = header.split("-")
        if len(parts) != 4:
            return None
        _, trace_id, span_id, flags = parts
        return cls(trace_id=trace_id, span_id=span_id, sampled=(flags == "01"))

    def inject_grpc(self, metadata: Dict) -> Dict:
        """注入 gRPC metadata。"""
        metadata[self.GRPC_TRACE_KEY] = self.trace_id
        metadata[self.GRPC_SPAN_KEY] = self.span_id
        return metadata

    @classmethod
    def extract_grpc(cls, metadata: Dict) -> Optional["TraceContext"]:
        trace_id = metadata.get(cls.GRPC_TRACE_KEY)
        span_id = metadata.get(cls.GRPC_SPAN_KEY)
        if not trace_id or not span_id:
            return None
        return cls(trace_id=trace_id, span_id=span_id)


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * pct / 100.0
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return sorted_values[low]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (rank - low)


class DistributedTracer:
    """分布式追踪器：跨度管理、上下文传播、性能分析、告警。"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.service_name = self.config.get("service_name", Config.service_name)
        self.alert_threshold_ms = float(
            self.config.get("alert_threshold_ms", Config.alert_threshold_ms)
        )
        self.alert_cooldown_s = float(
            self.config.get("alert_cooldown_s", Config.alert_cooldown_s)
        )
        self._exporter: Optional[Callable[[Span], None]] = self.config.get("exporter")
        self._state = {}
        self._history = []
        self._active: List[Span] = []          # 当前活跃跨度栈
        self._finished: List[Span] = []        # 已结束跨度
        self._reported: List[Span] = []        # 已上报跨度
        self._alerts: List[Dict] = []          # 已触发告警
        self._alert_seen: Dict[tuple, float] = {}  # 告警指纹 -> 上次告警时间（去重）

    # ---------------- 跨度生命周期 ----------------

    def start_span(
        self,
        operation: str,
        service: Optional[str] = None,
        tags: Optional[Dict] = None,
        parent: Optional[object] = None,
        context: Optional[TraceContext] = None,
    ) -> Span:
        """开始跨度：生成 trace_id/span_id，挂到父节点，打上标签。"""
        parent_span: Optional[Span] = None
        parent_ctx: Optional[TraceContext] = context

        if isinstance(parent, Span):
            parent_span = parent
        elif isinstance(parent, TraceContext):
            parent_ctx = parent
        elif parent is None and context is None and self._active:
            parent_span = self._active[-1]

        if parent_span is not None:
            trace_id = parent_span.trace_id
            parent_id = parent_span.span_id
        elif parent_ctx is not None:
            trace_id = parent_ctx.trace_id
            parent_id = parent_ctx.span_id
        else:
            trace_id = _new_trace_id()
            parent_id = None

        merged_tags = {"service": service or self.service_name, "operation": operation}
        if tags:
            merged_tags.update(tags)

        span = Span(
            operation=operation,
            trace_id=trace_id,
            span_id=_new_span_id(),
            parent_id=parent_id,
            service=service or self.service_name,
            tags=merged_tags,
            start_time=time.time(),
            status="ok",
        )
        self._active.append(span)
        return span

    def end_span(
        self,
        span: Span,
        status: str = "ok",
        error: Optional[object] = None,
    ) -> Span:
        """结束跨度：记录结束时间、状态、错误标记，并上报。"""
        span.end_time = time.time()
        if error is not None:
            span.status = "error"
            span.error = True
            span.error_message = str(error)
        elif status == "error":
            span.status = "error"
            span.error = True
        else:
            span.status = status

        if span in self._active:
            self._active.remove(span)
        self._finished.append(span)
        self._report(span)
        return span

    def _report(self, span: Span) -> None:
        """上报跨度：交给导出器，并留档。"""
        self._reported.append(span)
        if self._exporter is not None:
            self._exporter(span)

    @property
    def reported_spans(self) -> List[Span]:
        return list(self._reported)

    @property
    def finished_spans(self) -> List[Span]:
        return list(self._finished)

    # ---------------- 追踪上下文 ----------------

    def trace_context(self, span: Optional[Span] = None) -> TraceContext:
        """获取当前（或指定跨度的）追踪上下文，用于跨服务传播。"""
        target = span or (self._active[-1] if self._active else None)
        if target is None:
            return TraceContext(trace_id=_new_trace_id(), span_id=_new_span_id())
        return TraceContext(
            trace_id=target.trace_id,
            span_id=target.span_id,
            parent_id=target.parent_id,
        )

    def inject_context(
        self,
        carrier: Dict,
        format: str = "http",
        context: Optional[TraceContext] = None,
    ) -> Dict:
        """把追踪上下文注入 HTTP 头或 gRPC metadata。"""
        ctx = context or self.trace_context()
        if format == "http":
            return ctx.inject_http(carrier)
        if format == "grpc":
            return ctx.inject_grpc(carrier)
        raise ValueError("unsupported format: {}".format(format))

    def extract_context(
        self, carrier: Dict, format: str = "http"
    ) -> Optional[TraceContext]:
        """从 HTTP 头或 gRPC metadata 还原追踪上下文。"""
        if format == "http":
            return TraceContext.extract_http(carrier)
        if format == "grpc":
            return TraceContext.extract_grpc(carrier)
        raise ValueError("unsupported format: {}".format(format))

    # ---------------- 性能分析 ----------------

    def performance_analysis(self) -> Dict:
        """按操作聚合统计（avg/P95/P99 等），并生成火焰图。"""
        operations: Dict[str, Dict] = {}
        grouped: Dict[str, List[Span]] = {}
        for span in self._finished:
            grouped.setdefault(span.operation, []).append(span)

        for name, spans in grouped.items():
            durations = sorted(s.duration_ms for s in spans)
            total = sum(durations)
            errors = sum(1 for s in spans if s.error)
            operations[name] = {
                "count": len(spans),
                "error_count": errors,
                "total_ms": total,
                "avg_ms": total / len(spans),
                "min_ms": durations[0],
                "max_ms": durations[-1],
                "p95_ms": _percentile(durations, 95),
                "p99_ms": _percentile(durations, 99),
            }

        return {
            "operations": operations,
            "flame_graph": self._build_flame_graph(),
            "slowest": self._slowest_spans(5),
        }

    def _build_flame_graph(self) -> List[Dict]:
        """按父子关系把跨度组织成火焰图树。"""
        nodes = {}
        for span in self._finished:
            nodes[span.span_id] = {
                "name": span.operation,
                "service": span.service,
                "value": span.duration_ms,
                "children": [],
            }
        roots = []
        for span in self._finished:
            node = nodes[span.span_id]
            if span.parent_id and span.parent_id in nodes:
                nodes[span.parent_id]["children"].append(node)
            else:
                roots.append(node)
        return roots

    def _slowest_spans(self, limit: int) -> List[Dict]:
        slowest = sorted(
            self._finished, key=lambda s: s.duration_ms, reverse=True
        )[:limit]
        return [
            {
                "operation": s.operation,
                "service": s.service,
                "duration_ms": s.duration_ms,
                "trace_id": s.trace_id,
                "span_id": s.span_id,
            }
            for s in slowest
        ]

    # ---------------- 告警 ----------------

    def alerting(self, now: Optional[float] = None) -> List[Dict]:
        """扫描已结束跨度，超阈值或出错则告警；同一问题在冷却期内去重。"""
        now = time.time() if now is None else now
        new_alerts: List[Dict] = []
        for span in self._finished:
            if span.error:
                self._maybe_alert(
                    fingerprint=("error", span.operation),
                    alert={
                        "type": "error",
                        "operation": span.operation,
                        "service": span.service,
                        "message": "span {} failed: {}".format(
                            span.operation, span.error_message or "unknown error"
                        ),
                        "trace_id": span.trace_id,
                        "span_id": span.span_id,
                        "timestamp": now,
                    },
                    now=now,
                    sink=new_alerts,
                )
            if span.duration_ms > self.alert_threshold_ms:
                self._maybe_alert(
                    fingerprint=("slow", span.operation),
                    alert={
                        "type": "slow",
                        "operation": span.operation,
                        "service": span.service,
                        "message": "span {} took {:.1f}ms (threshold {:.1f}ms)".format(
                            span.operation, span.duration_ms, self.alert_threshold_ms
                        ),
                        "duration_ms": span.duration_ms,
                        "threshold_ms": self.alert_threshold_ms,
                        "trace_id": span.trace_id,
                        "span_id": span.span_id,
                        "timestamp": now,
                    },
                    now=now,
                    sink=new_alerts,
                )
        return new_alerts

    def _maybe_alert(
        self,
        fingerprint: tuple,
        alert: Dict,
        now: float,
        sink: List[Dict],
    ) -> None:
        last = self._alert_seen.get(fingerprint)
        if last is not None and now - last < self.alert_cooldown_s:
            return  # 去重：冷却期内同一问题不重复告警
        self._alert_seen[fingerprint] = now
        alert["fingerprint"] = "{}:{}".format(*fingerprint)
        self._alerts.append(alert)
        sink.append(alert)

    @property
    def alerts(self) -> List[Dict]:
        return list(self._alerts)

    # ---------------- 框架接口 ----------------

    def update(self, dt: float):
        pass

    def reset(self):
        self._state = {}
        self._history = []
        self._active = []
        self._finished = []
        self._reported = []
        self._alerts = []
        self._alert_seen = {}
