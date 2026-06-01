import gc
import logging
import resource
import sys
from ctypes import CDLL, c_int, c_size_t
from typing import Any

_LIBC: Any = None
_LIBC_UNAVAILABLE = object()


def process_rss_mb() -> float | None:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        rss_kb = float(usage.ru_maxrss or 0)
        if rss_kb <= 0:
            return None
        return rss_kb / 1024.0
    except Exception:
        return None


def _get_libc() -> Any:
    global _LIBC
    if _LIBC is _LIBC_UNAVAILABLE:
        return None
    if _LIBC is not None:
        return _LIBC
    if not sys.platform.startswith("linux"):
        _LIBC = _LIBC_UNAVAILABLE
        return None
    try:
        libc = CDLL("libc.so.6")
        libc.malloc_trim.argtypes = [c_size_t]
        libc.malloc_trim.restype = c_int
        _LIBC = libc
        return libc
    except Exception:
        _LIBC = _LIBC_UNAVAILABLE
        return None


def trim_process_memory(logger: logging.Logger, *, context: str) -> None:
    collected = 0
    malloc_trim_result: bool | None = None
    try:
        collected = gc.collect()
    except Exception:
        collected = 0
    libc = _get_libc()
    if libc is not None:
        try:
            malloc_trim_result = bool(libc.malloc_trim(0))
        except Exception:
            malloc_trim_result = None
    rss_mb = process_rss_mb()
    logger.info(
        "Runtime memory cleanup finished: context=%s gc_collected=%s malloc_trim=%s rss_mb=%s",
        context,
        collected,
        (
            str(malloc_trim_result).lower()
            if malloc_trim_result is not None
            else "n/a"
        ),
        f"{rss_mb:.1f}" if rss_mb is not None else "n/a",
    )
