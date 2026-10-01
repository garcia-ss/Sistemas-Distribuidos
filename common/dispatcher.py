"""Dispatcher generico por 'type', dedup de solicitacoes e correlacao de
respostas pendentes (secao 3.2 do plano).

Modulo comum: a mecanica de rotear por type, gerar protocol_error, deduplicar
retransmissoes e casar uma resposta com quem esta esperando por ela e
IDENTICA para master e worker. Quem difere sao os handlers registrados --
master registra um handler para register_worker (item 3), worker e master
registram um handler para heartbeat (item 4). Este modulo nao sabe o que
register_worker ou heartbeat significam.

Duas mecanicas distintas, deliberadamente separadas:
  - Dedup (Deduplicator): protege quem RECEBE uma solicitacao contra
    reprocessar retransmissoes da mesma (source.node_id, request_id, type).
  - Correlacao (PendingResponses): permite que quem ENVIA uma solicitacao
    com resposta prevista espere (com timeout) pela resposta certa, casada
    por request_id. A politica de reenvio (ate 3 envios, 5s de timeout)
    fica para quem usa isso de verdade -- os itens 3 e 4.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Callable

from common import envelope as envelope_mod
from common import transport
from common.envelope import Envelope, NodeRef
from common.logging_setup import log_envelope

logger = logging.getLogger(__name__)

HandlerResult = tuple[str, dict] | None
Handler = Callable[[Envelope], HandlerResult]

DedupKey = tuple[str, str, str]  # (source.node_id, request_id, type)


@dataclass(frozen=True)
class _DedupEntry:
    payload: dict
    response: dict | None


class Deduplicator:
    """Cache de (source.node_id, request_id, type) -> resposta ja enviada.

    Nao e o estado da farm: e local ao dispatcher, guarda so o necessario
    para responder retransmissoes sem repetir efeito colateral do handler.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: dict[DedupKey, _DedupEntry] = {}

    def check(self, key: DedupKey, payload: dict) -> str:
        """Devolve 'new', 'duplicate' ou 'conflict' para a chave/payload dados."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return "new"
            return "duplicate" if entry.payload == payload else "conflict"

    def cached_response(self, key: DedupKey) -> dict | None:
        with self._lock:
            entry = self._entries.get(key)
            return entry.response if entry is not None else None

    def store(self, key: DedupKey, payload: dict, response: dict | None) -> None:
        with self._lock:
            self._entries[key] = _DedupEntry(payload=payload, response=response)


class PendingResponses:
    """Correlaciona respostas recebidas com solicitacoes pendentes por request_id.

    Quem envia uma solicitacao com resposta prevista chama begin_wait() antes
    de enviar. Depois, tem duas formas de esperar: wait() bloqueia a thread
    ate a resposta chegar ou o timeout estourar -- so serve quando a thread
    nao tem mais nada a fazer nesse meio tempo (ex: handshake inicial). Numa
    thread de rede que precisa continuar lendo o socket enquanto espera
    (ex: heartbeat, ver common/heartbeat.py), use try_take() dentro do
    proprio laco de leitura: ele nunca bloqueia. O lado que recebe mensagens
    chama claim() antes de tratar um envelope como solicitacao nova: se
    alguem estiver esperando por esse request_id, a mensagem e entregue a
    essa espera em vez de cair no dispatch por type.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._waiters: dict[str, threading.Event] = {}
        self._results: dict[str, Envelope] = {}

    def begin_wait(self, request_id: str) -> None:
        with self._lock:
            self._waiters[request_id] = threading.Event()

    def claim(self, env: Envelope) -> bool:
        with self._lock:
            waiter = self._waiters.get(env.request_id)
            if waiter is None:
                return False
            self._results[env.request_id] = env
        waiter.set()
        return True

    def wait(self, request_id: str, timeout: float) -> Envelope | None:
        with self._lock:
            waiter = self._waiters.get(request_id)
        if waiter is None:
            return None
        if waiter.wait(timeout):
            with self._lock:
                self._waiters.pop(request_id, None)
                return self._results.pop(request_id, None)
        return None

    def try_take(self, request_id: str) -> Envelope | None:
        """Versao nao bloqueante de wait(): devolve None de imediato se a
        resposta ainda nao chegou, sem nunca suspender a thread chamadora."""
        with self._lock:
            waiter = self._waiters.get(request_id)
            if waiter is None or not waiter.is_set():
                return None
            self._waiters.pop(request_id, None)
            return self._results.pop(request_id, None)

    def cancel_wait(self, request_id: str) -> None:
        with self._lock:
            self._waiters.pop(request_id, None)
            self._results.pop(request_id, None)


class Dispatcher:
    """Roteia envelopes validados (ja passaram por PendingResponses.claim) para
    handlers registrados por type; aplica dedup e a regra anti-loop do
    protocol_error.
    """

    def __init__(self, self_ref: NodeRef):
        self.self_ref = self_ref
        self._handlers: dict[str, Handler] = {}
        self._dedup = Deduplicator()

    def register_handler(self, type_: str, handler: Handler) -> None:
        if type_ in self._handlers:
            raise ValueError(f"handler ja registrado para type={type_!r}")
        self._handlers[type_] = handler

    def dispatch(self, env: Envelope) -> dict | None:
        if env.type == "protocol_error":
            logger.warning(
                "protocol_error recebido de %s (request_id=%s): %s",
                env.source.node_label, env.request_id, env.payload,
            )
            return None  # uma protocol_error recebida nunca gera outra

        handler = self._handlers.get(env.type)
        if handler is None:
            return self._protocol_error(
                env, error_code="unsupported_type", error_message=f"type nao suportado: {env.type!r}"
            )

        key = (env.source.node_id, env.request_id, env.type)
        status = self._dedup.check(key, env.payload)

        if status == "conflict":
            return self._protocol_error(
                env,
                error_code="request_conflict",
                error_message="mesma (node_id, request_id, type) com payload divergente",
            )

        if status == "duplicate":
            return self._dedup.cached_response(key)

        response = handler(env)
        response_message = None
        if response is not None:
            response_type, response_payload = response
            response_message = envelope_mod.response_envelope(
                type=response_type,
                source=self.self_ref,
                destination=env.source,
                payload=response_payload,
                request_id=env.request_id,
            )
        self._dedup.store(key, env.payload, response_message)
        return response_message

    def _protocol_error(self, env: Envelope, *, error_code: str, error_message: str) -> dict:
        return envelope_mod.response_envelope(
            type="protocol_error",
            source=self.self_ref,
            destination=env.source,
            payload={"error_code": error_code, "error_message": error_message},
            request_id=env.request_id,
        )


def process_raw_line(
    raw_line: bytes, *, dispatcher: Dispatcher, pending_responses: PendingResponses
) -> dict | None:
    """Decodifica, valida e despacha uma linha NDJSON crua.

    Devolve a resposta a enviar (dict) ou None. Nunca levanta excecao para o
    chamador: e o ponto de entrada pensado para o loop de leitura de socket,
    e enquadramento/JSON invalido nao pode derrubar o processo.
    """
    try:
        raw_message = transport.decode_json_line(raw_line)
    except transport.MessageDecodeError as exc:
        logger.warning("linha NDJSON rejeitada (%s): %s", exc.error_code, exc)
        return None  # sem source/request_id confiaveis: nao ha como correlacionar

    try:
        env = envelope_mod.parse_envelope(raw_message)
    except envelope_mod.EnvelopeError as exc:
        return _best_effort_protocol_error(raw_message, dispatcher, exc)

    log_envelope(logger, direction="recv", env=env)

    if pending_responses.claim(env):
        return None  # era a resposta de uma solicitacao pendente, ja entregue

    return dispatcher.dispatch(env)


def _best_effort_protocol_error(
    raw_message: dict, dispatcher: Dispatcher, exc: envelope_mod.EnvelopeError
) -> dict | None:
    """Tenta responder protocol_error mesmo com envelope parcialmente invalido.

    So e possivel quando request_id e source ainda dao para extrair (regra:
    "erro correlacionado quando o envelope puder ser lido"). Se nem isso for
    possivel, ou se a mensagem quebrada ja era um protocol_error, so loga.
    """
    if raw_message.get("type") == "protocol_error":
        logger.warning("protocol_error malformado recebido, descartando: %s", exc)
        return None

    request_id = raw_message.get("request_id")
    source = raw_message.get("source")
    if not envelope_mod.is_uuid4_string(request_id) or not isinstance(source, dict):
        logger.warning("envelope invalido demais para correlacionar resposta: %s", exc)
        return None

    try:
        source_ref = envelope_mod.parse_node_ref(source, field_name="source")
    except envelope_mod.EnvelopeError:
        logger.warning("source do envelope invalido, nao ha como correlacionar: %s", exc)
        return None

    return envelope_mod.response_envelope(
        type="protocol_error",
        source=dispatcher.self_ref,
        destination=source_ref,
        payload={"error_code": exc.error_code, "error_message": str(exc)},
        request_id=request_id,
    )
