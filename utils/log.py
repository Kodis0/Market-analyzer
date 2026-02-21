import asyncio
import logging


def log_task_exception(task: asyncio.Task, logger: logging.Logger | None = None) -> None:
    """Callback for add_done_callback: log exceptions from fire-and-forget tasks."""
    if task.cancelled():
        return
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    if exc is not None:
        log = logger or logging.getLogger("app")
        log.warning("Background task failed: %s", exc)


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
