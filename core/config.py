from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Dict, Optional, List


class TelegramCfg(BaseModel):
    chat_id: int
    thread_id: Optional[int] = None
    web_app_url: Optional[str] = None  # URL для кнопки "Навигация" (Web App)
    pinned_message_text: Optional[str] = None  # Текст закреплённого сообщения (редактируй в config.yaml)


class BybitCfg(BaseModel):
    ws_url: str
    ping_interval_sec: int = 20
    depth: int = 50
    symbols: List[str]


class JupiterCfg(BaseModel):
    base_url: str = "https://api.jup.ag/swap/v1"
    slippage_bps: int = 50
    restrict_intermediate_tokens: bool = True
    max_accounts: int = 64
    timeout_sec: float = 2.0
    poll_interval_sec: float = 1.5


class TokenCfg(BaseModel):
    bybit_symbol: str
    mint: str
    decimals: int


class StableCfg(BaseModel):
    symbol: str = "USDC"
    mint: str
    decimals: int = 6


class TradingCfg(BaseModel):
    notional_usd: float = 1000
    stable: StableCfg
    tokens: Dict[str, TokenCfg]


class ThresholdsCfg(BaseModel):
    bybit_taker_fee_bps: float = 10
    solana_tx_fee_usd: float = 0.05
    latency_buffer_bps: float = 5
    usdt_usdc_buffer_bps: float = 5
    min_profit_usd: float = 1.0


class FiltersCfg(BaseModel):
    max_cex_slippage_bps: float = 30
    max_dex_price_impact_pct: float = 0.50
    persistence_hits: int = 2
    cooldown_sec: int = 60
    min_delta_profit_usd_to_resend: float = 0.5
    price_ratio_max: float = 3.0
    gross_profit_cap_pct: float = 10.0
    max_spread_bps: float = 50.0
    min_depth_coverage_pct: float = 98.0
    denylist_symbols: List[str] = []
    denylist_regex: List[str] = []


class RuntimeCfg(BaseModel):
    engine_tick_hz: int = 10
    ws_snapshot_timeout_sec: float = 30.0
    status_interval_sec: float = 15.0  # интервал STATUS лога (меньше = больше нагрузка)
    stats_bybit_sample: int = 10  # записывать каждое N-е сообщение Bybit (1=все, 10=1/10 нагрузки)


class NotifierCfg(BaseModel):
    edit_mode: bool = True
    edit_min_interval_sec: float = 3.0
    stale_ttl_sec: float = 300.0
    delete_stale: bool = False


class RateLimitsCfg(BaseModel):
    jupiter_rps: float = 5.0
    jupiter_concurrency: int = 4
    jupiter_max_retries: int = 4


class LoggingCfg(BaseModel):
    level: str = "INFO"


class ApiCfg(BaseModel):
    """API security for Mini App. Protects /api/* endpoints."""
    auth_required: bool = True
    auth_ttl_sec: int = 3600  # initData older than this is rejected (replay protection)
    allowed_user_ids: List[int] = []  # empty = any valid Telegram user; non-empty = only these IDs
    rate_limit_per_min: int = 60  # max requests per IP per minute
    cors_origins: List[str] = []  # empty = *; non-empty = only these origins (e.g. https://market.example.com)
    logs_enabled: bool = True  # enable GET /api/logs (last N lines from buffer)
    logs_buffer_size: int = 1000  # max lines in ring buffer
    logs_max_line_len: int = 500  # truncate longer lines
    logs_rate_limit_per_min: int = 10  # separate limit for /api/logs


class AppConfig(BaseModel):
    telegram: TelegramCfg
    bybit: BybitCfg
    jupiter: JupiterCfg
    notifier: NotifierCfg = Field(default_factory=NotifierCfg)
    rate_limits: RateLimitsCfg = Field(default_factory=RateLimitsCfg)
    trading: TradingCfg
    thresholds: ThresholdsCfg
    filters: FiltersCfg
    runtime: RuntimeCfg
    logging: LoggingCfg
    api: ApiCfg = Field(default_factory=ApiCfg)
