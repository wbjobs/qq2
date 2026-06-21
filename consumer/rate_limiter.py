import time
import threading


class TokenBucket:
    def __init__(self, rate: int, capacity: int = None):
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self._tokens = float(self.capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.rate
        self._tokens = min(self.capacity, self._tokens + new_tokens)
        self._last_refill = now

    def acquire(self, tokens: int = 1, blocking: bool = True) -> bool:
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
            if not blocking:
                return False
            needed = tokens - self._tokens
            wait_time = needed / self.rate
            time.sleep(wait_time)
