from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Thresholds:
    bybit_taker_fee_bps: Decimal
    solana_tx_fee_usd: Decimal
    latency_buffer_bps: Decimal
    usdt_usdc_buffer_bps: Decimal

    # оставляем старое имя (как в твоём ThresholdsCfg / YAML)
    min_profit_usd: Decimal

    # алиас на будущее (если потом захочешь переименовать в YAML)
    @property
    def min_net_profit_usd(self) -> Decimal:
        return self.min_profit_usd

    def required_profit_usd(self, notional_usd: Decimal) -> Decimal:
        cex_fee = notional_usd * (self.bybit_taker_fee_bps / Decimal("10000"))
        buffers = notional_usd * ((self.latency_buffer_bps + self.usdt_usdc_buffer_bps) / Decimal("10000"))
        return cex_fee + buffers + self.solana_tx_fee_usd + self.min_profit_usd
