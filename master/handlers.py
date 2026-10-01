"""Handlers do master para o catalogo de registro e liveness (Sprint 1).

Funcoes puras o bastante para testar sem socket real: recebem o envelope ja
validado e as dependencias (registry, self_master_ref, connection) como
parametros explicitos, em vez de ler estado global. O dispatcher (common/)
so chama estas funcoes; nao sabe o que "register_worker" significa.
"""

from __future__ import annotations

import logging

from common import envelope as envelope_mod
from common.envelope import Envelope, MasterRef
from common.transport import NdjsonConnection
from master.registry import WorkerRegistration, WorkerRegistry

logger = logging.getLogger(__name__)


def handle_register_worker(
    env: Envelope,
    *,
    connection: NdjsonConnection,
    registry: WorkerRegistry,
    self_master_ref: MasterRef,
):
    payload = env.payload

    if "loan_id" not in payload:
        return _register_rejected("payload.loan_id ausente (use null para registro normal)", payload)
    if payload["loan_id"] is not None:
        return _register_rejected(
            "loan_id deve ser null na Sprint 1: emprestimos ainda nao existem", payload
        )

    home_master = payload.get("home_master")
    if not isinstance(home_master, dict):
        return _register_rejected("payload.home_master ausente ou invalido", payload)
    try:
        home_master_ref = envelope_mod.parse_master_ref(home_master, field_name="payload.home_master")
    except envelope_mod.EnvelopeError as exc:
        return _register_rejected(f"payload.home_master invalido: {exc}", payload)
    if home_master_ref.node_id != self_master_ref.node_id:
        return _register_rejected(
            "home_master nao e este master (emprestimos ainda nao existem na Sprint 1)", payload
        )

    assignment_epoch = payload.get("assignment_epoch")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        return _register_rejected("payload.assignment_epoch deve ser inteiro nao negativo", payload)

    existing = registry.get(env.source.node_id)
    if existing is not None and existing.node_label != env.source.node_label:
        # mesmo node_id reaparecendo com label diferente: identidade
        # inconsistente, nao e so uma reconexao normal do mesmo worker.
        return _register_rejected(
            f"node_id {env.source.node_id} ja registrado com label diferente "
            f"({existing.node_label!r} != {env.source.node_label!r})",
            payload,
            assignment_epoch=existing.assignment_epoch,
        )
    if existing is not None and existing.assignment_epoch != assignment_epoch:
        return _register_rejected(
            f"assignment_epoch {assignment_epoch} diverge do cadastro atual ({existing.assignment_epoch})",
            payload,
            assignment_epoch=existing.assignment_epoch,
        )

    registration = WorkerRegistration(
        node_id=env.source.node_id,
        node_label=env.source.node_label,
        group_label=env.source.group_label,
        assignment_epoch=assignment_epoch,
        connection=connection,
    )
    previous = registry.register(registration)
    if previous is not None and previous.connection is not connection:
        logger.info("%s reconectou: encerrando sessao anterior", env.source.node_label)
        previous.connection.close()

    logger.info("worker registrado: %s (epoch=%d)", env.source.node_label, assignment_epoch)
    return "registration_ack", {"accepted": True, "assignment_epoch": assignment_epoch, "reason": None}


def _register_rejected(reason: str, payload: dict, *, assignment_epoch: int | None = None):
    logger.warning("register_worker rejeitado: %s", reason)
    if assignment_epoch is None:
        raw_epoch = payload.get("assignment_epoch")
        assignment_epoch = raw_epoch if isinstance(raw_epoch, int) and not isinstance(raw_epoch, bool) else 0
    return "registration_ack", {"accepted": False, "assignment_epoch": assignment_epoch, "reason": reason}


def handle_heartbeat(env: Envelope, *, registry: WorkerRegistry):
    """Heartbeat enviado pelo worker para este master (nó -> peer cadastrado).

    Responde heartbeat_ack mesmo se o node_id nao estiver (mais) registrado:
    o heartbeat so confirma que o master esta vivo, quem decide se a sessao
    de execucao continua valida e o fluxo de register_worker/epoch, nao este
    handler. Payload invalido (sequence/assignment_epoch fora do formato)
    gera protocol_error em vez de heartbeat_ack.
    """
    payload = env.payload

    sequence = payload.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        return "protocol_error", {
            "error_code": "invalid_message",
            "error_message": "payload.sequence deve ser inteiro",
        }

    if "assignment_epoch" not in payload:
        return "protocol_error", {
            "error_code": "invalid_message",
            "error_message": "payload.assignment_epoch ausente",
        }
    assignment_epoch = payload["assignment_epoch"]
    if assignment_epoch is not None and (
        not isinstance(assignment_epoch, int)
        or isinstance(assignment_epoch, bool)
        or assignment_epoch < 0
    ):
        return "protocol_error", {
            "error_code": "invalid_message",
            "error_message": "payload.assignment_epoch deve ser inteiro nao negativo ou null",
        }

    registry.mark_heartbeat(env.source.node_id)
    return "heartbeat_ack", {"sequence": sequence, "status": "alive"}
