# -*- coding: utf-8 -*-
"""
Simple per-process adaptive rate limiter.

Designed to be lightweight and safe for multiprocessing: each worker can keep
its own instance and adjust delays based on recent successes or rate-limit
responses (429). This implementation follows the plan discussed in the
conversation: reduce delay slowly on sustained success and back off quickly
on rate-limit responses.
"""
from __future__ import annotations

from typing import Optional


class AdaptiveRateLimiter:
    def __init__(self, base_delay: float = 30.0, min_delay: float = 5.0, max_delay: float = 120.0):
        self.base_delay = float(base_delay)
        self.current_delay = float(base_delay)
        self.min_delay = float(min_delay)
        self.max_delay = float(max_delay)
        self.success_count = 0
        self.rate_limit_count = 0

    def get_delay(self) -> float:
        return float(self.current_delay)

    def on_success(self) -> None:
        # Reduce delay slowly: 10 successes -> -10%
        self.success_count += 1
        if self.success_count >= 10:
            new_delay = max(self.min_delay, self.current_delay * 0.9)
            # Avoid flapping
            if new_delay < self.current_delay:
                self.current_delay = new_delay
            self.success_count = 0

    def on_rate_limit(self) -> None:
        # Aggressive backoff on detecting rate-limit
        self.rate_limit_count += 1
        self.current_delay = min(self.max_delay, max(self.current_delay * 2.0, self.base_delay))
        self.success_count = 0

    def on_manual_increase(self, factor: float = 2.0) -> None:
        self.current_delay = min(self.max_delay, self.current_delay * factor)
