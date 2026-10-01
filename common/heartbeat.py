"""Envio periodico de heartbeat e deteccao de peer suspeito (catalogo 4.1).

Comum a master e worker: os dois enviam heartbeat a cada 10s ao peer
cadastrado e, apos 3 periodos consecutivos sem resposta valida, marcam a
sessao como suspeita. O QUE fazer com uma sessao suspeita (worker reconecta;
master remove do registro) e decisao de cada role, feita fora daqui -- este
modulo so entrega o sinal via o atributo `suspect`.

tick() nunca bloqueia: e chamado a cada volta do mesmo laco com timeout que
ja le o socket (recv_lines), para que o envio/deteccao de heartbeat nunca
impeca a thread de rede de continuar respondendo a outras mensagens --
inclusive ao heartbeat que o peer manda na direcao contraria.
"""

from __future__ import annotations

import logging
import time
from typing import Callable

from common import envelope as envelope_mod
from common.dispatcher import PendingResponses
from common.envelope import NodeRef
from common.logging_setup import log_envelope
from common.transport import NdjsonConnection

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL_S = 10.0
RESPONSE_TIMEOUT_S = 5.0
MAX_MISSED_HEARTBEATS = 3


class HeartbeatSender:
    """Um por conexao (uma sessao de controle = um HeartbeatSender)."""

    def __init__(
        self,
        *,
        self_ref: NodeRef,
        peer_ref: NodeRef,
        connection: NdjsonConnection,
        pending: PendingResponses,
        assignment_epoch_provider: Callable[[], int | None] = lambda: None,
        interval_s: float = HEARTBEAT_INTERVAL_S,
        response_timeout_s: float = RESPONSE_TIMEOUT_S,
        max_missed: int = MAX_MISSED_HEARTBEATS,
    ):
        self._self_ref = self_ref
        self._peer_ref = peer_ref
        self._connection = connection
        self._pending = pending
        self._assignment_epoch_provider = assignment_epoch_provider
        self._interval_s = interval_s
        self._response_timeout_s = response_timeout_s
        self._max_missed = max_missed
        self._sequence = 0
        self._outstanding_request_id: str | None = None
        self._outstanding_sent_at: float | None = None
        self._last_cycle_started = time.monotonic()
        self.missed_count = 0
        self.suspect = False

    def tick(self) -> None:
        now = time.monotonic()

        if self._outstanding_request_id is not None:
            self._check_outstanding(now)

        if self._outstanding_request_id is None and now - self._last_cycle_started >= self._interval_s:
            self._send(now)

    def _check_outstanding(self, now: float) -> None:
        result = self._pending.try_take(self._outstanding_request_id)
        if result is not None:
            self.missed_count = 0
            self.suspect = False
            self._outstanding_request_id = None
            return

        if now - self._outstanding_sent_at > self._response_timeout_s:
            self._pending.cancel_wait(self._outstanding_request_id)
            self._outstanding_request_id = None
            self.missed_count += 1
            if self.missed_count >= self._max_missed:
                self.suspect = True
                logger.warning(
                    "%s suspeito: %d heartbeats consecutivos sem resposta",
                    self._peer_ref.node_label, self.missed_count,
                )

    def _send(self, now: float) -> None:
        self._sequence += 1
        message = envelope_mod.new_request_envelope(
            type="heartbeat",
            source=self._self_ref,
            destination=self._peer_ref,
            payload={
                "sequence": self._sequence,
                "assignment_epoch": self._assignment_epoch_provider(),
            },
        )
        request_id = message["request_id"]
        self._pending.begin_wait(request_id)
        log_envelope(logger, direction="send", env=envelope_mod.parse_envelope(message))
        self._connection.send_message(message)
        self._outstanding_request_id = request_id
        self._outstanding_sent_at = now
        self._last_cycle_started = now
