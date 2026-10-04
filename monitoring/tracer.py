"""
游戏内分布式追踪与链路监控系统 - 核心模块

功能：
- 跨度管理：start_span / end_span，含 trace_id、span_id、父节点、标签、状态、错误标记与上报
- 追踪上下文：跨服务传播（HTTP 头 / gRPC 元数据）与序列化
- 性能分析：count / avg / min / max / P95 / P99 统计与火焰图
- 告警：阈值告警与冷却窗口内去重
"""
import json
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

TRACE_ID_HEADER = "x-trace-id"
SPAN_ID_HEADER = "x-span-id"
TRACEPARENT_HEADER = "traceparent"


def _new_span_id() -> str:
    return uuid.uuid4().hex[:16]


def _new_trace_id() -> str:
    return uuid.uuid4().hex


def _percentile(sorted_values: List[float], pct: float) -> float:
    """最近秩百分位，sorted_values 必须已升序排序且非空。"""
    rank = math.ceil(pct / 100.0 * len(sorted_values))
    index = min(max(rank - 1, 0), len(sorted_values) - 1)
    return sorted_values[index]


@dataclass
class Config:
    service_name: str = "game-server"
    alert_threshold_ms: float = 1000.0
    alert_cooldown_s: float = 60.0


@dataclass
class Span:
    """一个跨度：一次调用在调用链中的一个节点。"""
    name: str
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    service: str = ""
    tags: Dict = field(default_factory=dict)
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = "unset"  # unset | ok | error
    error: Optional[str] = None

    @property
    def duration_ms(self) -> Optional[float]:
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time) * 1000.0

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "service": self.service,
            "tags": dict(self.tags),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error": self.error,
        }


@dataclass
class Alert:
    alert_type: str  # slow_span | error
    operation: str
    message: str
    timestamp: float
    trace_id: str = ""
    value_ms: float = 0.0
    threshold_ms: float = 0.0

    def to_dict(self) -> Dict:
        return {
            "alert_type": self.alert_type,
            "operation": self.operation,
            "message": self.message,
            "timestamp": self.timestamp,
            "trace_id": self.trace_id,
            "value_ms": self.value_ms,
            "threshold_ms": self.threshold_ms,
        }


class DistributedTracer:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.service_name = self.config.get("service_name", Config.service_name)
        self.alert_threshold_ms = float(
            self.config.get("alert_threshold_ms", Config.alert_threshold_ms)
        )
        self.alert_cooldown_s = float(
            self.config.get("alert_cooldown_s", Config.alert_cooldown_s)
        )
        self._active: List[Span] = []          # 进行中的跨度栈（栈顶为当前跨度）
        self._finished: List[Span] = []        # 已结束并上报的跨度
        self._exporters: List[Callable[[Span], None]] = []
        self._alerts: List[Alert] = []
        self._alert_handlers: List[Callable[[Alert], None]] = []
        self._alerted_at: Dict[Tuple[str, str], float] = {}  # 告警去重：key -> 上次告警时间

    # ------------------------------------------------------------------
    # 跨度生命周期
    # ------------------------------------------------------------------
    def start_span(
        self,
        name: str,
        service: Optional[str] = None,
        tags: Optional[Dict] = None,
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
    ) -> Span:
        """开始一个跨度：分配 trace_id / span_id，关联父节点，打上标签。"""
        parent = self.current_span()
        if trace_id is None:
            trace_id = parent.trace_id if parent else _new_trace_id()
        if parent_span_id is None and parent is not None:
            parent_span_id = parent.span_id

        merged_tags = {"interface": name}
        if tags:
            merged_tags.update(tags)

        span = Span(
            name=name,
            trace_id=trace_id,
            span_id=_new_span_id(),
            parent_span_id=parent_span_id,
            service=service or self.service_name,
            tags=merged_tags,
            start_time=time.time(),
        )
        self._active.append(span)
        return span

    def end_span(self, span: Span, error: Optional[BaseException] = None) -> Span:
        """结束一个跨度：记录结束时间、状态、错误标记，并上报给导出器。"""
        span.end_time = time.time()
        if error is None:
            span.status = "ok"
        else:
            span.status = "error"
            span.error = f"{type(error).__name__}: {error}"
            span.tags["error"] = True

        if span in self._active:
            self._active.remove(span)
        self._finished.append(span)

        for exporter in self._exporters:
            exporter(span)

        self._check_alerts(span)
        return span

    def current_span(self) -> Optional[Span]:
        return self._active[-1] if self._active else None

    @property
    def reported(self) -> List[Span]:
        """已上报（结束）的跨度列表。"""
        return list(self._finished)

    def add_exporter(self, exporter: Callable[[Span], None]) -> None:
        self._exporters.append(exporter)

    def get_trace(self, trace_id: str) -> List[Span]:
        """按开始时间返回一条完整调用链上的所有跨度。"""
        spans = [s for s in self._finished if s.trace_id == trace_id]
        return sorted(spans, key=lambda s: s.start_time)

    # ------------------------------------------------------------------
    # 追踪上下文：传播与序列化
    # ------------------------------------------------------------------
    def trace_context(self) -> Optional[Dict]:
        """当前追踪上下文，随跨服务调用一起传递，保证 trace_id 不丢失。"""
        span = self.current_span()
        if span is None:
            return None
        return {"trace_id": span.trace_id, "span_id": span.span_id}

    def inject_context(self, carrier: Optional[Dict] = None) -> Dict:
        """把当前上下文注入 HTTP 请求头（含 W3C traceparent）。"""
        headers: Dict[str, str] = {}
        ctx = self.trace_context()
        if ctx:
            headers[TRACE_ID_HEADER] = ctx["trace_id"]
            headers[SPAN_ID_HEADER] = ctx["span_id"]
            headers[TRACEPARENT_HEADER] = "00-{}-{}-01".format(
                ctx["trace_id"], ctx["span_id"]
            )
        if carrier is not None:
            carrier.update(headers)
            return carrier
        return headers

    def inject_grpc_metadata(self) -> List[Tuple[str, str]]:
        """把当前上下文注入 gRPC 元数据（键值对列表）。"""
        return list(self.inject_context().items())

    @staticmethod
    def _normalize_carrier(carrier) -> Dict:
        if carrier is None:
            return {}
        if isinstance(carrier, str):
            return DistributedTracer._normalize_carrier(json.loads(carrier))
        if isinstance(carrier, dict):
            return {str(k).lower(): v for k, v in carrier.items()}
        # gRPC 元数据等键值对序列
        return {str(k).lower(): v for k, v in carrier}

    def extract_context(self, carrier) -> Optional[Dict]:
        """从 HTTP 头 / gRPC 元数据 / JSON 字符串中提取上游追踪上下文。

        返回 {"trace_id", "span_id"}，可作为 start_span 的 trace_id /
        parent_span_id，实现跨服务调用链串联。
        """
        headers = self._normalize_carrier(carrier)
        trace_id = headers.get(TRACE_ID_HEADER)
        span_id = headers.get(SPAN_ID_HEADER)

        traceparent = headers.get(TRACEPARENT_HEADER)
        if traceparent and (not trace_id or not span_id):
            parts = str(traceparent).split("-")
            if len(parts) >= 4:
                trace_id = trace_id or parts[1]
                span_id = span_id or parts[2]

        if not trace_id or not span_id:
            return None
        return {"trace_id": trace_id, "span_id": span_id}

    def serialize_context(self) -> Optional[str]:
        """把当前上下文序列化为 JSON 字符串，便于在网络上传输。"""
        ctx = self.trace_context()
        return json.dumps(ctx) if ctx else None

    def deserialize_context(self, payload) -> Optional[Dict]:
        """从 JSON 字符串 / 字典还原追踪上下文。"""
        if payload is None:
            return None
        data = json.loads(payload) if isinstance(payload, str) else dict(payload)
        if not data.get("trace_id") or not data.get("span_id"):
            return None
        return {"trace_id": data["trace_id"], "span_id": data["span_id"]}

    # ------------------------------------------------------------------
    # 性能分析：统计与火焰图
    # ------------------------------------------------------------------
    def performance_analysis(self) -> Dict:
        """按接口聚合统计（count/avg/min/max/P95/P99）并生成火焰图。"""
        durations: Dict[str, List[float]] = {}
        for span in self._finished:
            if span.duration_ms is not None:
                durations.setdefault(span.name, []).append(span.duration_ms)

        operations = {}
        for name, values in durations.items():
            ordered = sorted(values)
            operations[name] = {
                "count": len(ordered),
                "avg_ms": sum(ordered) / len(ordered),
                "min_ms": ordered[0],
                "max_ms": ordered[-1],
                "p95_ms": _percentile(ordered, 95),
                "p99_ms": _percentile(ordered, 99),
            }
        return {"operations": operations, "flame_graph": self.flame_graph()}

    def flame_graph(self) -> Dict:
        """由已结束跨度的父子关系构建火焰图（同名兄弟节点合并）。"""
        nodes: Dict[str, Dict] = {}
        roots: List[Dict] = []

        def make_node(span: Span) -> Dict:
            return {
                "name": span.name,
                "value": span.duration_ms or 0.0,
                "children": [],
                "_by_name": {},
            }

        for span in sorted(self._finished, key=lambda s: s.start_time):
            node = make_node(span)
            nodes[span.span_id] = node
            parent_node = nodes.get(span.parent_span_id or "")
            if parent_node is None:
                roots.append(node)
            else:
                sibling = parent_node["_by_name"].get(span.name)
                if sibling is None:
                    parent_node["children"].append(node)
                    parent_node["_by_name"][span.name] = node
                else:
                    sibling["value"] += node["value"]
                    sibling["children"].extend(node["children"])
                    nodes[span.span_id] = sibling

        def strip(node: Dict) -> Dict:
            node.pop("_by_name", None)
            for child in node["children"]:
                strip(child)
            return node

        if len(roots) == 1:
            return strip(roots[0])
        total = sum(r["value"] for r in roots)
        merged: Dict = {"name": "root", "value": total, "children": [], "_by_name": {}}
        for root in roots:
            existing = merged["_by_name"].get(root["name"])
            if existing is None:
                merged["children"].append(root)
                merged["_by_name"][root["name"]] = root
            else:
                existing["value"] += root["value"]
                existing["children"].extend(root["children"])
        return strip(merged)

    # ------------------------------------------------------------------
    # 告警：阈值与去重
    # ------------------------------------------------------------------
    def _check_alerts(self, span: Span) -> None:
        if span.status == "error":
            self._raise_alert(
                "error",
                span,
                "接口 {} 调用失败: {}".format(span.name, span.error),
            )
        duration = span.duration_ms
        if duration is not None and duration > self.alert_threshold_ms:
            self._raise_alert(
                "slow_span",
                span,
                "接口 {} 耗时 {:.1f}ms 超过阈值 {:.1f}ms".format(
                    span.name, duration, self.alert_threshold_ms
                ),
                value_ms=duration,
                threshold_ms=self.alert_threshold_ms,
            )

    def _raise_alert(
        self,
        alert_type: str,
        span: Span,
        message: str,
        value_ms: float = 0.0,
        threshold_ms: float = 0.0,
    ) -> Optional[Alert]:
        key = (alert_type, span.name)
        now = time.time()
        last = self._alerted_at.get(key)
        if last is not None and now - last < self.alert_cooldown_s:
            return None  # 冷却窗口内去重，同一问题不重复告警
        self._alerted_at[key] = now
        alert = Alert(
            alert_type=alert_type,
            operation=span.name,
            message=message,
            timestamp=now,
            trace_id=span.trace_id,
            value_ms=value_ms,
            threshold_ms=threshold_ms,
        )
        self._alerts.append(alert)
        for handler in self._alert_handlers:
            handler(alert)
        return alert

    def alerting(self) -> List[Alert]:
        """返回已触发的全部告警。"""
        return list(self._alerts)

    def add_alert_handler(self, handler: Callable[[Alert], None]) -> None:
        self._alert_handlers.append(handler)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def update(self, dt: float):
        pass

    def reset(self):
        self._active = []
        self._finished = []
        self._alerts = []
        self._alerted_at = {}
