"""
Runtime settings that can be changed via /settings command.
Stored in settings.json, merged with config.yaml defaults.
Defaults aligned with core.config (ThresholdsCfg, FiltersCfg, RuntimeCfg, JupiterCfg, NotifierCfg).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class RuntimeSettings:
    """Mutable settings affecting arb signals. All values can be updated at runtime."""

    # Thresholds (aligned with ThresholdsCfg)
    bybit_taker_fee_bps: float = 10
    solana_tx_fee_usd: float = 0.05
    latency_buffer_bps: float = 5
    usdt_usdc_buffer_bps: float = 5
    min_profit_usd: float = 1.0

    # Trading (aligned with TradingCfg)
    notional_usd: float = 1000

    # Filters (aligned with FiltersCfg)
    max_cex_slippage_bps: float = 30
    max_dex_price_impact_pct: float = 0.50
    persistence_hits: int = 2
    cooldown_sec: int = 60
    min_delta_profit_usd_to_resend: float = 0.5
    price_ratio_max: float = 3.0
    gross_profit_cap_pct: float = 10.0
    max_spread_bps: float = 50.0
    min_depth_coverage_pct: float = 98.0

    # Runtime (aligned with RuntimeCfg, JupiterCfg)
    engine_tick_hz: int = 10
    jupiter_poll_interval_sec: float = 1.5
    max_ob_age_ms: int = 2000

    # Notifier (aligned with NotifierCfg)
    stale_ttl_sec: int = 300  # 0 = выключено, иначе сек до "устарел"
    delete_stale: bool = False  # True = удалять, False = редактировать на "устарел"

    # Exchange: вкл/выкл биржевую логику (Jupiter, Bybit, арбитраж)
    exchange_enabled: bool = True

    # Auto-tune: вкл/выкл авто-подстройку параметров
    auto_tune_enabled: bool = True
    # Bounds для авто-подстройки: {"min_profit_usd": {"min": 0.1, "max": 50}, ...}
    auto_tune_bounds: dict | None = None

    # Human-readable labels for /settings
    LABELS: dict[str, str] = field(
        default_factory=lambda: {
            "bybit_taker_fee_bps": "Комиссия Bybit (bps)",
            "solana_tx_fee_usd": "Комиссия Solana ($)",
            "latency_buffer_bps": "Буфер задержки (bps)",
            "usdt_usdc_buffer_bps": "Буфер USDT/USDC (bps)",
            "min_profit_usd": "Мин. прибыль ($)",
            "notional_usd": "Объём сделки ($)",
            "max_cex_slippage_bps": "Макс. слип CEX (bps)",
            "max_dex_price_impact_pct": "Макс. импакт DEX (%)",
            "persistence_hits": "Порог persistence",
            "cooldown_sec": "Cooldown (сек)",
            "min_delta_profit_usd_to_resend": "Мин. дельта для ресэнда ($)",
            "price_ratio_max": "Макс. ratio цен",
            "gross_profit_cap_pct": "Макс. gross profit (%)",
            "max_spread_bps": "Макс. спред (bps)",
            "min_depth_coverage_pct": "Мин. depth coverage (%)",
            "engine_tick_hz": "Частота тика (Hz)",
            "jupiter_poll_interval_sec": "Интервал опроса Jupiter (сек)",
            "max_ob_age_ms": "Макс. возраст стакана (мс)",
            "stale_ttl_sec": "Время до устаревания сигнала (сек, 0=выкл)",
            "exchange_enabled": "Биржевая логика вкл (true/false)",
            "auto_tune_enabled": "Авто-подстройка параметров вкл (true/false)",
        }
    )

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("LABELS", None)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RuntimeSettings:
        s = cls()
        valid = {f for f in cls.__dataclass_fields__ if f != "LABELS"}
        for k, v in (d or {}).items():
            if k in valid:
                s.update(k, v)
        return s

    def update(self, key: str, value: Any) -> bool:
        if not hasattr(self, key) or key == "LABELS":
            return False
        if key == "exchange_enabled":
            value = str(value).lower() in ("true", "1", "yes", "да", "on")
        elif key == "auto_tune_enabled":
            value = str(value).lower() in ("true", "1", "yes", "да", "on")
        elif key == "auto_tune_bounds":
            if value is not None and not isinstance(value, dict):
                return False
            value = value
        elif key == "delete_stale":
            value = str(value).lower() in ("true", "1", "yes", "да", "on")
        elif key in ("persistence_hits", "cooldown_sec", "engine_tick_hz", "max_ob_age_ms", "stale_ttl_sec"):
            try:
                value = int(float(value))
            except (TypeError, ValueError):
                return False
        else:
            try:
                value = float(value) if isinstance(value, (int, float, str)) else value
            except (TypeError, ValueError):
                return False
        setattr(self, key, value)
        return True

    def format_for_telegram(self) -> str:
        lines = [
            '<b><tg-emoji emoji-id="5341715473882955310">⚙️</tg-emoji> Настройки бота</b>',
            "",
            "<b>Доступные параметры (копируй для команды):</b>",
            "<code>min_profit_usd</code> — мин. прибыль ($)",
            "<code>notional_usd</code> — объём сделки ($)",
            "<code>max_spread_bps</code> — макс. спред (bps)",
            "<code>max_cex_slippage_bps</code> — макс. слип CEX (bps)",
            "<code>max_dex_price_impact_pct</code> — макс. импакт DEX (%)",
            "<code>persistence_hits</code> — порог persistence",
            "<code>cooldown_sec</code> — cooldown между сигналами (сек)",
            "<code>min_delta_profit_usd_to_resend</code> — мин. дельта для ресэнда ($)",
            "<code>price_ratio_max</code> — макс. ratio цен",
            "<code>gross_profit_cap_pct</code> — макс. gross profit (%)",
            "<code>min_depth_coverage_pct</code> — мин. depth coverage (%)",
            "<code>engine_tick_hz</code> — частота тика (Hz)",
            "<code>jupiter_poll_interval_sec</code> — интервал опроса Jupiter (сек)",
            "<code>max_ob_age_ms</code> — макс. возраст стакана (мс)",
            "<code>stale_ttl_sec</code> — время до устаревания (сек, 0=выкл)",
            "<code>exchange_enabled</code> — биржевая логика вкл (true/false). Быстрее: /exchange on|off",
            "<code>bybit_taker_fee_bps</code> — комиссия Bybit (bps)",
            "<code>solana_tx_fee_usd</code> — комиссия Solana ($)",
            "<code>latency_buffer_bps</code> — буфер задержки (bps)",
            "<code>usdt_usdc_buffer_bps</code> — буфер USDT/USDC (bps)",
            "",
            "<b>Текущие значения:</b>",
        ]
        for k, v in self.to_dict().items():
            if k == "LABELS":
                continue
            lines.append(f"• <code>{k}</code>: {v}")
        lines.append("")
        lines.append("<b>Изменить:</b> <code>/settings min_profit_usd 20</code>")
        lines.append("<i>Подробнее: /help</i>")
        return "\n".join(lines)

    @staticmethod
    def format_help() -> str:
        return """<b><tg-emoji emoji-id="5226512880362332956">📖</tg-emoji> Справка по параметрам</b>

<b>Прибыль и объём:</b>
• <code>min_profit_usd</code> — минимальная чистая прибыль в $ для отправки сигнала
• <code>notional_usd</code> — объём сделки в USDC (сколько тратим на арбитраж)

<b>Комиссии и буферы:</b>
• <code>bybit_taker_fee_bps</code> — комиссия Bybit в базисных пунктах (1 bps = 0.01%)
• <code>solana_tx_fee_usd</code> — примерная комиссия сети Solana в $
• <code>latency_buffer_bps</code> — запас на задержку исполнения (bps)
• <code>usdt_usdc_buffer_bps</code> — буфер на разницу USDT/USDC (bps)

<b>Фильтры качества:</b>
• <code>max_spread_bps</code> — макс. спред стакана (выше = пропускаем пару)
• <code>max_cex_slippage_bps</code> — макс. допустимый слип на CEX (Bybit)
• <code>max_dex_price_impact_pct</code> — макс. импакт на DEX (Jupiter) в %
• <code>min_depth_coverage_pct</code> — мин. % покрытия объёма глубиной стакана

<b>Поведение сигналов:</b>
• <code>persistence_hits</code> — сколько раз подряд должен быть профит перед отправкой
• <code>cooldown_sec</code> — пауза между повторными сигналами по одной паре (сек)
• <code>min_delta_profit_usd_to_resend</code> — на сколько $ должен вырасти профит для ресэнда

<b>Доп. фильтры:</b>
• <code>price_ratio_max</code> — макс. отношение цен Jupiter/Bybit (защита от аномалий)
• <code>gross_profit_cap_pct</code> — макс. gross profit в % от объёма

<b>Производительность:</b>
• <code>engine_tick_hz</code> — как часто проверять арбитраж (раз в секунду)
• <code>jupiter_poll_interval_sec</code> — интервал опроса котировок Jupiter
• <code>max_ob_age_ms</code> — макс. возраст стакана в мс (старше = пропускаем)

<b>Устаревшие сигналы:</b>
• <code>stale_ttl_sec</code> — через сколько сек сигнал считается устаревшим (0 = выключено)

<b>Пример:</b> <code>/settings min_profit_usd 20</code>
<b>Пример:</b> <code>/settings stale_ttl_sec 300</code> — устаревать через 5 мин"""


def load_runtime_settings(path: str, defaults: RuntimeSettings | None = None) -> RuntimeSettings:
    s = defaults or RuntimeSettings()
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        for k, v in (d or {}).items():
            s.update(k, v)
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        pass
    return s


def save_runtime_settings(path: str, s: RuntimeSettings) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    d = {k: v for k, v in s.to_dict().items() if k != "LABELS"}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2, ensure_ascii=False)
