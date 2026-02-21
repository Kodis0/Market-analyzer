from __future__ import annotations


class Persistence:
    """
    Require N consecutive 'ok' hits before triggering a signal.
    """

    def __init__(self, hits: int) -> None:
        self.hits = max(1, int(hits))
        self._cnt: dict[str, int] = {}

    def hit(self, key: str, ok: bool) -> bool:
        if not ok:
            self._cnt[key] = 0
            return False
        self._cnt[key] = self._cnt.get(key, 0) + 1
        return self._cnt[key] >= self.hits
