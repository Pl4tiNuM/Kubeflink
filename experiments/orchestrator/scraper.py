"""
Scraper interface and base implementation.

All metric scrapers implement the Scraper protocol.
"""

import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Protocol
from datetime import datetime


class ScraperResult:
    """
    Standard result from a scraper tick.

    Always includes:
    - t_unix_ms, iso_time, tick, source
    - ok: bool
    - scrape_duration_ms: int

    If ok=True: data: Dict[str, Any]
    If ok=False: error: str, error_type: str
    """

    def __init__(
        self,
        source: str,
        tick: int,
        ok: bool,
        scrape_duration_ms: int,
        data: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        error_type: Optional[str] = None
    ):
        self.source = source
        self.tick = tick
        self.ok = ok
        self.scrape_duration_ms = scrape_duration_ms
        self.data = data or {}
        self.error = error
        self.error_type = error_type

        # Timestamps
        self.t_unix_ms = int(time.time() * 1000)
        self.iso_time = datetime.utcnow().isoformat() + "Z"

    @classmethod
    def ok(cls, tick: int, source: str, data: Dict[str, Any], duration_ms: int = 0):
        """Create a successful result."""
        return cls(
            source=source,
            tick=tick,
            ok=True,
            scrape_duration_ms=duration_ms,
            data=data
        )

    @classmethod
    def error(cls, tick: int, source: str, error: str, error_type: str = "ScraperError"):
        """Create an error result."""
        return cls(
            source=source,
            tick=tick,
            ok=False,
            scrape_duration_ms=0,
            error=error,
            error_type=error_type
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSONL-writable dict"""
        result = {
            "t_unix_ms": self.t_unix_ms,
            "iso_time": self.iso_time,
            "tick": self.tick,
            "source": self.source,
            "ok": self.ok,
            "scrape_duration_ms": self.scrape_duration_ms,
        }

        if self.ok:
            result["data"] = self.data
        else:
            result["error"] = self.error
            result["error_type"] = self.error_type

        return result


class Scraper(ABC):
    """
    Base scraper interface.

    All scrapers must implement:
    - name: str property
    - scrape(ctx) -> ScraperResult
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Source name for JSONL records"""
        pass

    @abstractmethod
    def scrape(self, ctx) -> ScraperResult:
        """
        Perform one scrape operation.

        Args:
            ctx: RunContext with session, topology, config

        Returns:
            ScraperResult with ok=True/False and data or error

        Must not raise exceptions - catch and return ScraperResult(ok=False)
        """
        pass

    def _timed_scrape(self, ctx, scrape_func) -> ScraperResult:
        """
        Helper to time a scrape function and catch exceptions.

        Usage:
            def _do_scrape(ctx):
                # ... actual scraping logic
                return {"metric1": value1, ...}

            return self._timed_scrape(ctx, _do_scrape)
        """
        start_ms = int(time.time() * 1000)

        try:
            data = scrape_func(ctx)
            duration_ms = int(time.time() * 1000) - start_ms

            return ScraperResult(
                source=self.name,
                tick=ctx.tick_idx,
                ok=True,
                scrape_duration_ms=duration_ms,
                data=data
            )

        except Exception as e:
            duration_ms = int(time.time() * 1000) - start_ms

            return ScraperResult(
                source=self.name,
                tick=ctx.tick_idx,
                ok=False,
                scrape_duration_ms=duration_ms,
                error=str(e),
                error_type=type(e).__name__
            )


class DummyScraper(Scraper):
    """Example scraper for testing"""

    def __init__(self, scraper_name: str):
        self._name = scraper_name

    @property
    def name(self) -> str:
        return self._name

    def scrape(self, ctx) -> ScraperResult:
        def _do_scrape(ctx):
            return {
                "tick": ctx.tick_idx,
                "dummy": True,
                "message": f"Scraper {self.name} tick {ctx.tick_idx}"
            }

        return self._timed_scrape(ctx, _do_scrape)
