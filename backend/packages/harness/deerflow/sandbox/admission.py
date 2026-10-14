"""Private host lifecycle capability; no resource or business policy."""

import threading

from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError


class ProviderAdmission:
    """Memory-only exact identity. Publication owns the provider metadata lock."""

    def __init__(self):
        self._lock = threading.Lock()
        self._provider = None
        self._ready = False
        self._sealed = False
        self._revision = 0

    def bind(self, provider):
        with self._lock:
            if provider is None or self._sealed or (self._provider is not None and self._provider is not provider):
                raise SandboxRuntimeAdmissionError("Sandbox lifecycle belongs to another provider")
            if self._provider is None:
                self._revision += 1
            self._provider = provider

    def open(self, provider):
        with self._lock:
            if self._sealed or self._provider is not provider or provider is None:
                raise SandboxRuntimeAdmissionError("Sandbox lifecycle cannot open this provider")
            self._ready = True
            self._revision += 1

    def require(self, provider):
        with self._lock:
            if self._sealed or not self._ready or self._provider is not provider or provider is None:
                raise SandboxRuntimeAdmissionError("Sandbox lifecycle admission is not ready")

    def close(self):
        with self._lock:
            if self._ready:
                self._revision += 1
            self._ready = False

    def require_closed(self, provider, *, revision=None):
        """Private accounting restoration requires its exact live closed lifecycle."""
        with self._lock:
            if self._sealed or self._ready or self._provider is not provider or provider is None or (revision is not None and revision != self._revision):
                raise SandboxRuntimeAdmissionError("Sandbox accounting restoration is not admitted")
            return self._revision

    def seal(self):
        with self._lock:
            if not self._sealed:
                self._revision += 1
            self._ready = False
            self._sealed = True
