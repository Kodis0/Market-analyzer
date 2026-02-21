"""
AutoTuner: evaluates metrics and suggests parameter adjustments.

Rules:
- Few signals + many skip_profit_le0 → lower min_profit_usd
- Many signals → raise min_profit_usd
- Other skip categories can drive persistence_hits, cooldown_sec, etc.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

log = logging.getLogger("auto_tune.tuner")

if TYPE_CHECKING:
    from core.runtime_settings import RuntimeSettings


@dataclass
class ParamChange:
    """A suggested parameter change."""

    param: str
    old_value: float | int
    new_value: float | int
    reason: str


@dataclass
class TunerConfig:
    """Configuration for AutoTuner."""

    target_signals_min: int = 2
    target_signals_max: int = 15
    min_profit_usd_min: float = 0.1
    min_profit_usd_max: float = 50.0
    min_profit_step: float = 0.5
    persistence_hits_min: int = 1
    persistence_hits_max: int = 5
    cooldown_sec_min: int = 1
    cooldown_sec_max: int = 300
    max_spread_bps_min: float = 20.0
    max_spread_bps_max: float = 500.0


@dataclass
class TunerBounds:
    """Runtime bounds override (from API / settings)."""

    min_profit_usd: dict[str, float] | None = None  # {"min": x, "max": y}
    persistence_hits: dict[str, int] | None = None
    cooldown_sec: dict[str, int] | None = None
    max_spread_bps: dict[str, float] | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> TunerBounds:
        if not d:
            return cls()
        return cls(
            min_profit_usd=d.get("min_profit_usd"),
            persistence_hits=d.get("persistence_hits"),
            cooldown_sec=d.get("cooldown_sec"),
            max_spread_bps=d.get("max_spread_bps"),
        )


class AutoTuner:
    """
    Evaluates metrics and returns list of parameter changes to apply.

    Uses target_signals_min/max to decide if we have too few or too many signals.
    """

    def __init__(self, config: TunerConfig | None = None) -> None:
        self._config = config or TunerConfig()

    def evaluate(
        self,
        metrics: dict[str, Any],
        settings: RuntimeSettings,
        bounds: TunerBounds | None = None,
    ) -> list[ParamChange]:
        """
        Evaluate metrics and return suggested parameter changes.

        Args:
            metrics: From MetricsCollector.get_window_stats()
            settings: Current RuntimeSettings
            bounds: Optional runtime bounds override

        Returns:
            List of ParamChange to apply (param, old_value, new_value, reason)
        """
        bounds = bounds or TunerBounds()
        changes: list[ParamChange] = []

        signals_sent = metrics.get("signals_sent", 0)
        skip_profit_le0 = metrics.get("skip_profit_le0", 0)
        skip_persistence = metrics.get("skip_persistence", 0)
        skip_dedup = metrics.get("skip_dedup", 0)
        skip_spread = metrics.get("skip_spread", 0)

        # Bounds for min_profit_usd
        bp = bounds.min_profit_usd or {}
        min_profit_min = float(bp.get("min", self._config.min_profit_usd_min))
        min_profit_max = float(bp.get("max", self._config.min_profit_usd_max))

        current_min_profit = float(settings.min_profit_usd)

        # Rule: few signals + many skip_profit_le0 → lower min_profit
        if signals_sent < self._config.target_signals_min and skip_profit_le0 > 0:
            if current_min_profit > min_profit_min:
                step = self._config.min_profit_step
                new_val = max(min_profit_min, current_min_profit - step)
                if new_val < current_min_profit:
                    changes.append(
                        ParamChange(
                            param="min_profit_usd",
                            old_value=current_min_profit,
                            new_value=round(new_val, 2),
                            reason=f"Мало сигналов ({signals_sent}), много skip_profit_le0 ({skip_profit_le0}) → понижаем min_profit",
                        )
                    )

        # Rule: many signals → raise min_profit
        if signals_sent > self._config.target_signals_max:
            if current_min_profit < min_profit_max:
                step = self._config.min_profit_step
                new_val = min(min_profit_max, current_min_profit + step)
                if new_val > current_min_profit:
                    changes.append(
                        ParamChange(
                            param="min_profit_usd",
                            old_value=current_min_profit,
                            new_value=round(new_val, 2),
                            reason=f"Много сигналов ({signals_sent}) → повышаем min_profit",
                        )
                    )

        # Rule: many skip_persistence → consider lowering persistence_hits (if > 1)
        ph_b = bounds.persistence_hits or {}
        ph_min = int(ph_b.get("min", self._config.persistence_hits_min))
        ph_max = int(ph_b.get("max", self._config.persistence_hits_max))
        current_ph = int(settings.persistence_hits)
        if skip_persistence > 20 and current_ph > ph_min:
            new_ph = max(ph_min, current_ph - 1)
            if new_ph < current_ph:
                changes.append(
                    ParamChange(
                        param="persistence_hits",
                        old_value=current_ph,
                        new_value=new_ph,
                        reason=f"Много skip_persistence ({skip_persistence}) → понижаем persistence_hits",
                    )
                )

        # Rule: many skip_dedup → consider raising cooldown (reduce spam)
        cd_b = bounds.cooldown_sec or {}
        cd_min = int(cd_b.get("min", self._config.cooldown_sec_min))
        cd_max = int(cd_b.get("max", self._config.cooldown_sec_max))
        current_cd = int(settings.cooldown_sec)
        if skip_dedup > 30 and current_cd < cd_max:
            new_cd = min(cd_max, current_cd + 5)
            if new_cd > current_cd:
                changes.append(
                    ParamChange(
                        param="cooldown_sec",
                        old_value=current_cd,
                        new_value=new_cd,
                        reason=f"Много skip_dedup ({skip_dedup}) → повышаем cooldown",
                    )
                )

        # Rule: many skip_spread → consider raising max_spread_bps (if within bounds)
        sp_b = bounds.max_spread_bps or {}
        spread_min = float(sp_b.get("min", self._config.max_spread_bps_min))
        spread_max = float(sp_b.get("max", self._config.max_spread_bps_max))
        current_spread = float(settings.max_spread_bps)
        if skip_spread > 50 and current_spread < spread_max:
            new_spread = min(spread_max, current_spread + 10)
            if new_spread > current_spread:
                changes.append(
                    ParamChange(
                        param="max_spread_bps",
                        old_value=current_spread,
                        new_value=round(new_spread, 1),
                        reason=f"Много skip_spread ({skip_spread}) → повышаем max_spread_bps",
                    )
                )

        return changes
