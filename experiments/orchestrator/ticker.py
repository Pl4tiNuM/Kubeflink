"""
Ticker: Single authoritative periodic clock for metric scraping.

Responsibilities:
- Drive scraping at fixed tick_seconds interval
- Call all registered scrapers per tick
- Write JSONL records (even on scraper failure)
- Enforce per-scraper timeout budgets
- Robust: one scraper failure must not block others
"""

import time
import json
import threading
from pathlib import Path
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed

from .scraper import Scraper, ScraperResult


class Ticker:
    """
    Periodic scraper driver with per-scraper timeout protection.
    
    Usage:
        ticker = Ticker(ctx, scrapers, tick_seconds=5, scraper_timeout_seconds=4)
        ticker.start()
        # ... run experiment ...
        ticker.stop()
    """
    
    def __init__(
        self,
        ctx,
        scrapers: List[Scraper],
        tick_seconds: int,
        scraper_timeout_seconds: int = None
    ):
        """
        Args:
            ctx: RunContext
            scrapers: List of Scraper instances
            tick_seconds: Interval between ticks
            scraper_timeout_seconds: Max time per scraper (default: tick_seconds - 1)
        """
        self.ctx = ctx
        self.scrapers = scrapers
        self.tick_seconds = tick_seconds
        
        # Default timeout: leave 1s buffer before next tick
        if scraper_timeout_seconds is None:
            self.scraper_timeout_seconds = max(1, tick_seconds - 1)
        else:
            self.scraper_timeout_seconds = scraper_timeout_seconds
        
        # Output file handles (keyed by scraper name)
        self.output_handles: Dict[str, any] = {}
        
        # Control
        self._running = False
        self._thread = None
        
        # Stats
        self.total_ticks = 0
        self.scraper_failures: Dict[str, int] = {s.name: 0 for s in scrapers}
    
    def start(self):
        """Start ticker in background thread"""
        if self._running:
            return
        
        self._running = True
        
        # Open output files
        for scraper in self.scrapers:
            path = self._get_output_path(scraper.name)
            self.output_handles[scraper.name] = open(path, "a")
        
        # Start ticker thread
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        
        self.ctx.log_event("TICKER_START", tick_seconds=self.tick_seconds)
    
    def stop(self):
        """Stop ticker and close files"""
        if not self._running:
            return
        
        self._running = False
        
        if self._thread:
            self._thread.join(timeout=self.tick_seconds + 2)
        
        # Close output files
        for handle in self.output_handles.values():
            handle.close()
        
        self.ctx.log_event(
            "TICKER_STOP",
            total_ticks=self.total_ticks,
            scraper_failures=self.scraper_failures
        )
    
    def _run_loop(self):
        """Main ticker loop"""
        next_tick_time = time.time()
        
        while self._running:
            # Advance tick
            self.ctx.next_tick()
            self.total_ticks += 1
            
            # Execute all scrapers in parallel with timeout
            self._execute_tick()
            
            # Sleep until next tick
            next_tick_time += self.tick_seconds
            sleep_time = next_tick_time - time.time()
            
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                # We're falling behind - log but continue
                self.ctx.log_event(
                    "TICKER_BEHIND",
                    tick=self.ctx.tick_idx,
                    behind_seconds=-sleep_time
                )
    
    def _execute_tick(self):
        """
        Execute all scrapers for current tick in parallel with timeout.
        
        Each scraper runs in separate thread with timeout protection.
        Writes JSONL record even if scraper fails/times out.
        """
        with ThreadPoolExecutor(max_workers=len(self.scrapers)) as executor:
            # Submit all scraper tasks
            future_to_scraper = {
                executor.submit(self._scrape_with_timeout, scraper): scraper
                for scraper in self.scrapers
            }
            
            # Collect results
            for future in as_completed(future_to_scraper):
                scraper = future_to_scraper[future]
                
                try:
                    result = future.result(timeout=self.scraper_timeout_seconds)
                except TimeoutError:
                    # Scraper timed out - create error result
                    result = ScraperResult(
                        source=scraper.name,
                        tick=self.ctx.tick_idx,
                        ok=False,
                        scrape_duration_ms=self.scraper_timeout_seconds * 1000,
                        error=f"Scraper timeout after {self.scraper_timeout_seconds}s",
                        error_type="TimeoutError"
                    )
                    self.scraper_failures[scraper.name] += 1
                except Exception as e:
                    # Unexpected error in future handling
                    result = ScraperResult(
                        source=scraper.name,
                        tick=self.ctx.tick_idx,
                        ok=False,
                        scrape_duration_ms=0,
                        error=str(e),
                        error_type=type(e).__name__
                    )
                    self.scraper_failures[scraper.name] += 1
                
                # Track failures
                if not result.ok:
                    self.scraper_failures[scraper.name] += 1
                
                # Write JSONL record
                self._write_result(result)
    
    def _scrape_with_timeout(self, scraper: Scraper) -> ScraperResult:
        """
        Call scraper.scrape() with internal timeout handling.
        
        This runs in a thread pool executor which provides the outer timeout.
        """
        return scraper.scrape(self.ctx)
    
    def _write_result(self, result: ScraperResult):
        """Write ScraperResult as JSONL line"""
        if not self._running:
            return  # Don't write if ticker is stopping
        
        handle = self.output_handles.get(result.source)
        if not handle or handle.closed:
            return
        
        try:
            line = json.dumps(result.to_dict())
            handle.write(line + "\n")
            handle.flush()  # Ensure written immediately
        except (ValueError, IOError):
            # File closed or I/O error during shutdown
            pass
    
    def _get_output_path(self, source_name: str) -> Path:
        """Map scraper name to output file path"""
        # Standard mapping
        path_map = {
            "flink_rest": self.ctx.flink_rest_path,
            "vm_util": self.ctx.vm_util_path,
            "power": self.ctx.power_path,
            "workload": self.ctx.workload_path,
        }
        
        # Return mapped path or create custom path
        if source_name in path_map:
            return path_map[source_name]
        else:
            # Custom scraper - create file in metrics dir
            return self.ctx.metrics_dir / f"{source_name}.jsonl"
