from typing import Dict, Optional, Any, List
import time
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import json
from pathlib import Path
import psutil
from ..domain.interfaces import MetricsCollector
import logging


logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    name: str
    value: float
    timestamp: float
    tags: Dict[str, str] = field(default_factory=dict)
    metric_type: str = "gauge"


class PrometheusMetrics(MetricsCollector):
    def __init__(self, namespace: str = "airquality", port: int = 9090, max_histogram_size: int = 1000):
        self.namespace = namespace
        self.port = port
        self.max_histogram_size = max_histogram_size
        self._metrics: Dict[str, MetricPoint] = {}
        self._counters: Dict[str, float] = defaultdict(float)
        self._histograms: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.max_histogram_size))
        self._lock = threading.Lock()

    def increment_counter(self, name: str, value: int = 1, tags: Optional[Dict[str, str]] = None) -> None:
        metric_name = f"{self.namespace}_{name}"
        tags_str = self._tags_to_string(tags)
        key = f"{metric_name}{tags_str}"
        
        with self._lock:
            self._counters[key] += value
            
            self._metrics[key] = MetricPoint(
                name=metric_name,
                value=self._counters[key],
                timestamp=time.time(),
                tags=tags or {},
                metric_type="counter"
            )

    def record_gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        metric_name = f"{self.namespace}_{name}"
        tags_str = self._tags_to_string(tags)
        key = f"{metric_name}{tags_str}"
        
        with self._lock:
            self._metrics[key] = MetricPoint(
                name=metric_name,
                value=value,
                timestamp=time.time(),
                tags=tags or {},
                metric_type="gauge"
            )

    def record_histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        metric_name = f"{self.namespace}_{name}"
        tags_str = self._tags_to_string(tags)
        key = f"{metric_name}{tags_str}"
        
        with self._lock:
            self._histograms[key].append(value)
            
            values = list(self._histograms[key])
            if values:
                sorted_values = sorted(values)
                count = len(sorted_values)
                sum_value = sum(sorted_values)
                
                self._record_histogram_metrics(metric_name, sorted_values, tags)

    def _record_histogram_metrics(self, name: str, values: List[float], tags: Optional[Dict[str, str]]) -> None:
        count = len(values)
        if count == 0:
            return
            
        sum_value = sum(values)
        
        self.record_gauge(f"{name}_count", count, tags)
        self.record_gauge(f"{name}_sum", sum_value, tags)
        
        for quantile in [0.5, 0.9, 0.95, 0.99]:
            index = max(0, min(count - 1, int(count * quantile) - 1))
            value = values[index]
            quantile_tags = {**(tags or {}), "quantile": str(quantile)}
            self.record_gauge(f"{name}_quantile", value, quantile_tags)

    def flush(self) -> List[str]:
        if not self._metrics:
            return []
        
        output = []
        with self._lock:
            for key, metric in self._metrics.items():
                labels = self._tags_to_string(metric.tags)
                output.append(f"# TYPE {metric.name} {metric.metric_type}")
                output.append(f"{metric.name}{labels} {metric.value} {int(metric.timestamp * 1000)}")
        
        logger.debug(f"Flushed {len(self._metrics)} metrics")
        return output

    def _tags_to_string(self, tags: Optional[Dict[str, str]]) -> str:
        if not tags:
            return ""
        return "{" + ",".join(f'{k}="{v}"' for k, v in sorted(tags.items())) + "}"


    async def export_metrics(self) -> str:
        output = []
        with self._lock:
            for key, metric in self._metrics.items():
                labels = self._tags_to_string(metric.tags)
                output.append(f"# TYPE {metric.name} {metric.metric_type}")
                output.append(f"{metric.name}{labels} {metric.value} {int(metric.timestamp * 1000)}")
        
        return "\n".join(output)


class MetricsMiddleware:
    def __init__(self, metrics: MetricsCollector):
        self.metrics = metrics

    def track_download(self, source: str):
        def decorator(func):
            async def wrapper(*args, **kwargs):
                start_time = time.time()
                success = False
                
                try:
                    result = await func(*args, **kwargs)
                    success = True
                    return result
                except Exception as e:
                    self.metrics.increment_counter(
                        "download_errors",
                        tags={"source": source, "error_type": type(e).__name__}
                    )
                    raise
                finally:
                    duration = (time.time() - start_time) * 1000
                    self.metrics.record_histogram(
                        "download_duration_ms",
                        duration,
                        tags={"source": source, "success": str(success).lower()}
                    )
            
            return wrapper
        return decorator

    def track_api_call(self, endpoint: str):
        def decorator(func):
            async def wrapper(*args, **kwargs):
                start_time = time.time()
                response_status_code = 0
                
                try:
                    result = await func(*args, **kwargs)
                    response_status_code = 200
                    return result
                except Exception as e:
                    if hasattr(e, 'status_code'):
                        response_status_code = e.status_code
                    else:
                        response_status_code = 500
                    raise
                finally:
                    duration = (time.time() - start_time) * 1000
                    
                    self.metrics.increment_counter(
                        "api_requests",
                        tags={
                            "endpoint": endpoint,
                            "status": str(response_status_code),
                            "status_class": f"{response_status_code // 100}xx"
                        }
                    )
                    
                    self.metrics.record_histogram(
                        "api_request_duration_ms",
                        duration,
                        tags={"endpoint": endpoint}
                    )
            
            return wrapper
        return decorator


class MetricsReporter:
    def __init__(self, metrics: MetricsCollector, interval: int = 60):
        self.metrics = metrics
        self.interval = interval
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._report_loop())

    async def stop(self):
        self._running = False
        if self._task:
            await self._task

    async def _report_loop(self):
        while self._running:
            try:
                self._report_system_metrics()
                self.metrics.flush()
                await asyncio.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error reporting metrics: {e}")

    def _report_system_metrics(self):
        process = psutil.Process()
        
        self.metrics.record_gauge("memory_usage_mb", process.memory_info().rss / 1024 / 1024)
        self.metrics.record_gauge("cpu_percent", process.cpu_percent())
        self.metrics.record_gauge("num_threads", process.num_threads())
        
        disk_usage = psutil.disk_usage('/')
        self.metrics.record_gauge("disk_usage_percent", disk_usage.percent)