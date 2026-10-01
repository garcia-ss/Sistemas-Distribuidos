"""Handlers do worker para mensagens que chegam do master (Sprint 1).

Nao ha handle_register_worker aqui: quem MANDA register_worker e o worker
(worker/agent.py trata a resposta registration_ack diretamente, via
PendingResponses, nao pelo dispatcher -- ver a nota em common/dispatcher.py
sobre respostas vs solicitacoes novas).
"""

from __future__ import annotations

import logging

from common.envelope import Envelope

logger = logging.getLogger(__name__)


def handle_heartbeat(env: Envelope):
    """Heartbeat enviado pelo master para este worker (deteccao de quem sumiu)."""
    payload = env.payload
    sequence = payload.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        return "protocol_error", {
            "error_code": "invalid_message",
            "error_message": "payload.sequence deve ser inteiro",
        }
    return "heartbeat_ack", {"sequence": sequence, "status": "alive"}
