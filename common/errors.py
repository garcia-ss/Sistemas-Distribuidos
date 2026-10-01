"""Erro de protocolo com error_code, reaproveitado por transport e envelope."""

from __future__ import annotations


class ProtocolError(ValueError):
    def __init__(self, error_code: str, message: str):
        super().__init__(message)
        self.error_code = error_code
