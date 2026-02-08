import ipaddress
import logging
import threading
import time
from dataclasses import dataclass

from fastapi import Request

from .auth_tokens import decode_access_token
from .auth_utils import extract_bearer_token


_LOGGER = logging.getLogger("api.rate_limit")


@dataclass
class _WindowState:
    start_ts: float
    count: int


class InMemoryRateLimiter:
    def __init__(self, requests: int, window_seconds: int) -> None:
        self.requests = max(1, int(requests))
        self.window_seconds = max(1, int(window_seconds))
        self._lock = threading.Lock()
        self._state: dict[str, _WindowState] = {}

    def _cleanup_expired(self, now_ts: float) -> None:
        expire_before = now_ts - self.window_seconds
        expired_keys = [
            key
            for key, value in self._state.items()
            if value.start_ts <= expire_before
        ]
        for key in expired_keys:
            self._state.pop(key, None)

    def check(self, key: str) -> tuple[bool, int, int, int]:
        now_ts = time.time()
        with self._lock:
            if len(self._state) > 10000:
                self._cleanup_expired(now_ts)

            current = self._state.get(key)
            if current is None or (now_ts - current.start_ts) >= self.window_seconds:
                current = _WindowState(start_ts=now_ts, count=0)
                self._state[key] = current

            reset_ts = int(current.start_ts + self.window_seconds)
            retry_after = max(
                1,
                int((current.start_ts + self.window_seconds) - now_ts + 0.999),
            )

            if current.count >= self.requests:
                return False, retry_after, 0, reset_ts

            current.count += 1
            remaining = max(0, self.requests - current.count)
            return True, retry_after, remaining, reset_ts


def _normalize_forwarded_candidate(candidate: str) -> str:
    value = (candidate or "").strip()
    if not value:
        return ""

    # RFC-style forwarded IPv6: [2001:db8::1] or [2001:db8::1]:443
    if value.startswith("["):
        close_idx = value.find("]")
        if close_idx > 1:
            return value[1:close_idx].strip()

    # IPv4 with port: 203.0.113.10:443
    if value.count(":") == 1:
        host, port = value.rsplit(":", 1)
        if host and port.isdigit():
            return host.strip()

    return value


def _extract_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        for candidate in forwarded.split(","):
            value = _normalize_forwarded_candidate(candidate)
            if not value:
                continue
            try:
                ipaddress.ip_address(value)
            except ValueError:
                continue
            return value
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def rate_limit_identity_key(request: Request) -> str:
    authorization = request.headers.get("authorization")
    token = extract_bearer_token(authorization)
    if token:
        try:
            payload = decode_access_token(token)
            subject = str(payload.get("sub", "")).strip().lower()
            if subject:
                return f"sub:{subject}"
        except ValueError:
            pass

    legacy_candidates = [
        request.headers.get("x-access-key"),
        request.headers.get("x-admin-key"),
    ]
    for raw in legacy_candidates:
        value = str(raw or "").strip().lower()
        if value:
            return f"legacy:{value}"

    return f"ip:{_extract_client_ip(request)}"


def is_rate_limited_path(path: str) -> bool:
    normalized = path or "/"
    if normalized != "/" and normalized.endswith("/"):
        normalized = normalized.rstrip("/")

    exempt_paths = {
        "/health",
        "/auth/login",
        "/auth/token",
        "/auth/refresh",
    }
    if normalized in exempt_paths:
        return False

    # Safe by default for API surface: every auth/data/meta route is limited
    # unless explicitly exempted above.
    api_prefixes = ("/auth", "/data", "/meta")
    return normalized.startswith(api_prefixes)


def log_rate_limit_hit(identity_key: str, method: str, path: str) -> None:
    _LOGGER.warning(
        "rate_limit_exceeded key=%s method=%s path=%s",
        identity_key,
        method,
        path,
    )
