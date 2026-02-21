"""Shared application context. Holds references to all services and config."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.config import AppConfig
    from core.quarantine_manager import QuarantineManager
    from core.state import MarketState
    from core.arb_engine import ArbEngine
    from core.ws_cluster import BybitWSCluster
    from core.auto_tune.metrics import MetricsCollector
    from core.runtime_settings import RuntimeSettings
    from connectors.jupiter import JupiterClient
    from notifier.telegram import TelegramNotifier


class AppContext:
    """Holds all services and config. Built incrementally in bootstrap."""

    def __init__(self) -> None:
        self.cfg: AppConfig | None = None
        self.raw: dict[str, Any] = {}
        self.cfg_dir: Path = Path()
        self.settings_path: Path = Path()
        self.settings: RuntimeSettings | None = None
        self.state: MarketState | None = None
        self.q_manager: QuarantineManager | None = None
        self.tg: TelegramNotifier | None = None
        self.jup: JupiterClient | None = None
        self.engine: ArbEngine | None = None
        self.ws_cluster: BybitWSCluster | None = None
        self.exchange_enabled_event: Any = None
        self.metrics_collector: MetricsCollector | None = None
        self.auto_tune_history: list[dict] = []
        self.auto_tune_interval_sec: float = 900.0
        self.stats_bybit_sample: int = 1
        self.bybit_record_counter: list[int] = [0]
