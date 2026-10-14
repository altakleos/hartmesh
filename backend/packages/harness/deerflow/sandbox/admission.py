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

    def bind(self, provider):
        with self._lock:
            if provider is None or self._sealed or (self._provider is not None and self._provider is not provider):
                raise SandboxRuntimeAdmissionError("Sandbox lifecycle belongs to another provider")
            self._provider = provider

    def open(self, provider):
        with self._lock:
            if self._sealed or self._provider is not provider or provider is None:
                raise SandboxRuntimeAdmissionError("Sandbox lifecycle cannot open this provider")
            self._ready = True

    def require(self, provider):
        with self._lock:
            if self._sealed or not self._ready or self._provider is not provider or provider is None:
                raise SandboxRuntimeAdmissionError("Sandbox lifecycle admission is not ready")

    def close(self):
        with self._lock:
            self._ready = False

    def seal(self):
        with self._lock:
            self._ready = False
            self._sealed = True
