"""performace timers which deal with dynamic timing"""

from __future__ import annotations
import time

class PerfTimerError(Exception):
    """A custom exception used to report errors in use of BatchPerfTimer class."""


class PerfTimer:
    """A Basic Performance Timer Class """

    _start_time: float = None
    _stop_time: float = None
    _lap_time: float = None

    @property
    def start_time(self):
        return self._start_time

    @property
    def stop_time(self):
        return self._stop_time

    @property
    def lap_time(self):
        return self._lap_time

    def start(self) -> None:
        """Start a new timer"""
        
        if self._start_time is not None:
            raise PerfTimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self) -> None:
        """Stop the timer, and report the elapsed time"""
        
        if self._start_time is None:
            raise PerfTimerError(f"Timer is not running. Use .start() to start it")
           
        self._stop_time = time.perf_counter()
        self._lap_time = self._stop_time - self._start_time
        self._start_time = None


class BatchPerfTimer(PerfTimer):
    """The Performance Timer for Target Dynamic Batching"""

    def __init__(
        self,
        max_size: int | None = None,
        max_perf_counter: float = 1
    ) -> None:
        self._sink_max_size: int = max_size
        self._max_perf_counter = max_perf_counter
    
    # The max size a bulk insert can be
    SINK_MAX_SIZE_CELING: int = 100000    
    
    # The current MAX_SIZE_DEFAULT
    @property
    def sink_max_size(self):
        return self._sink_max_size
    
    # How many seconds can pass before a insert
    @property
    def max_perf_counter(self):
        return self._max_perf_counter
    
    # The mininum negative variance allowed
    # 1/3 worse than wanted
    @property
    def perf_diff_allowed_min(self):
        return -1.0*(self.max_perf_counter * 0.33)
    
    # The maximum postive variace allowed
    # 3/4 better than wanted
    @property
    def perf_diff_allowed_max(self):
        return self.max_perf_counter * 0.75
    
    # The difference between the wanted elaped time before an insert
    # and the actual time of the last insert. 
    @property
    def perf_diff(self) -> float:
        if self._lap_time:
            return self.max_perf_counter - self.lap_time

    # Determine if a correction is needed and how much that correction should be
    def counter_based_max_size(self) -> int:
        correction = 0
        if self.perf_diff < self.perf_diff_allowed_min:
            if self.sink_max_size >= 15000:
                correction = -5000
            if self.sink_max_size >= 10000:
                correction = -1000
            elif self.sink_max_size >= 1000:
                correction = -100
            elif self.sink_max_size > 10:
                correction = 10
        if self.perf_diff >= self.perf_diff_allowed_max and self.sink_max_size < self.SINK_MAX_SIZE_CELING:
            if self.sink_max_size >= 10000:
                correction = 10000
            if self.sink_max_size >= 1000:
                correction = 1000
            elif self.sink_max_size >= 100:
                correction = 100
            elif self.sink_max_size >= 10:
                correction = 10
        self._sink_max_size += correction
        return self.sink_max_size   

