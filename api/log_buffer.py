"""
In-memory ring buffer for log lines. Used by /api/logs endpoint.
Thread-safe, fixed size, sanitizes sensitive data.
"""
from __future__ import annotations

import logging
import re
from collections import deque
from typing import Optional

# Sensitive patterns — lines matching these get redacted
_SENSITIVE_PATTERNS = [
    re.compile(r"\b(token|secret|password|api_key|apikey|auth)\s*[=:]\s*\S+", re.I),
    re.compile(r"Authorization:\s*Bearer\s+\S+", re.I),
    re.compile(r"[a-fA-F0-9]{32,}"),  # long hex (e.g. hashes)
]

_REDACT = "[REDACTED]"


def _sanitize(line: str, max_len: int = 500) -> str:
    """Redact sensitive data and truncate."""
    if len(line) > max_len:
        line = line[:max_len] + "..."
    for pat in _SENSITIVE_PATTERNS:
        line = pat.sub(_REDACT, line)
    return line


class LogBuffer:
    """Ring buffer for log lines. Thread-safe via deque."""

    def __init__(self, max_size: int = 1000, max_line_len: int = 500) -> None:
        self._lines: deque[str] = deque(maxlen=max_size)
        self._max_line_len = max_line_len

    def append(self, line: str) -> None:
        self._lines.append(_sanitize(line, self._max_line_len))

    def get_tail(self, limit: int = 100) -> list[str]:
        """Return last `limit` lines (newest last)."""
        limit = min(max(1, limit), 200)
        n = len(self._lines)
        if n <= limit:
            return list(self._lines)
        return list(self._lines)[-limit:]

    def __len__(self) -> int:
        return len(self._lines)


class LogBufferHandler(logging.Handler):
    """Logging handler that writes to LogBuffer."""

    def __init__(self, buffer: LogBuffer) -> None:
        super().__init__()
        self._buffer = buffer

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._buffer.append(msg)
        except Exception:
            self.handleError(record)


# Global buffer — created when logs_enabled, used by API
_buffer: Optional[LogBuffer] = None
_handler: Optional[LogBufferHandler] = None


def init_log_buffer(enabled: bool, buffer_size: int = 1000, max_line_len: int = 500) -> None:
    """Initialize or clear the log buffer. Call from app startup."""
    global _buffer, _handler
    root = logging.getLogger()
    if _handler:
        root.removeHandler(_handler)
        _handler = None
    _buffer = None
    if enabled:
        _buffer = LogBuffer(max_size=buffer_size, max_line_len=max_line_len)
        _handler = LogBufferHandler(_buffer)
        _handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        root.addHandler(_handler)


def get_logs(limit: int = 100) -> Optional[list[str]]:
    """Return last `limit` lines, or None if logs disabled."""
    if _buffer is None:
        return None
    return _buffer.get_tail(limit=limit)
