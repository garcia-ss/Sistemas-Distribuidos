"""Registro em memoria dos workers cadastrados neste master.

Sprint 1: so cadastro e liveness (sem fila, sem reserva -- isso e Sprint 2/3).
Compartilhado entre a thread de aceitacao e as threads de conexao, um lock
por instancia protege o dict inteiro. Esse lock so protege o dict em si;
NUNCA e segurado durante I/O de rede (regra do plano, secao 2.2).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from common.transport import NdjsonConnection


@dataclass
class WorkerRegistration:
    node_id: str
    node_label: str
    group_label: str
    assignment_epoch: int
    connection: NdjsonConnection
    last_heartbeat_at: float = field(default_factory=time.monotonic)
    missed_heartbeats: int = 0
    suspect: bool = False


class WorkerRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._workers: dict[str, WorkerRegistration] = {}

    def register(self, registration: WorkerRegistration) -> WorkerRegistration | None:
        """Instala o cadastro; devolve o cadastro anterior (se havia) para o
        chamador decidir o que fazer com a conexao antiga (fechar)."""
        with self._lock:
            previous = self._workers.get(registration.node_id)
            self._workers[registration.node_id] = registration
            return previous

    def get(self, node_id: str) -> WorkerRegistration | None:
        with self._lock:
            return self._workers.get(node_id)

    def remove(self, node_id: str) -> None:
        with self._lock:
            self._workers.pop(node_id, None)

    def mark_heartbeat(self, node_id: str) -> None:
        with self._lock:
            entry = self._workers.get(node_id)
            if entry is not None:
                entry.last_heartbeat_at = time.monotonic()
                entry.missed_heartbeats = 0
                entry.suspect = False

    def snapshot(self) -> list[WorkerRegistration]:
        with self._lock:
            return list(self._workers.values())
