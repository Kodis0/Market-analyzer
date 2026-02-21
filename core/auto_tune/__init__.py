"""
Auto-tuning module for arbitrage bot parameters.

MetricsCollector gathers signals and skip stats over a sliding window.
AutoTuner evaluates metrics and suggests parameter adjustments.
"""

from .metrics import MetricsCollector
from .tuner import AutoTuner

__all__ = ["MetricsCollector", "AutoTuner"]
