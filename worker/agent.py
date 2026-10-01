"""Agente de rede do worker: conecta ao master, registra e mantem heartbeat.

Secao 2.1: "uma thread atende rede/heartbeat; outra executa uma tarefa por
vez." A thread de execucao de tarefa e Sprint 2 -- ainda nao existe. O que
importa aqui, desde ja, e que o laco de rede NUNCA bloqueia indefinidamente
(recv com timeout curto), para que uma futura thread de tarefa possa
coexistir sem travar o heartbeat (criterio de pronto da Sprint 1).

Ciclo de vida: run() conecta, registra (ate 3 envios/5s de timeout cada,
por register_worker ser uma "operacao com resposta prevista" - secao 3.2),
mantem a sessao com heartbeat, e se a sessao cair reconecta com espera
crescente ate 5s. So desiste de vez se o master REJEITAR o registro
explicitamente (accepted:false) -- isso nao e falha de rede, e nao adianta
insistir sozinho.
"""

from __future__ import annotations

import logging
import socket
import threading
import time

from common import envelope as envelope_mod
from common import heartbeat as heartbeat_mod
from common.dispatcher import Dispatcher, PendingResponses, process_raw_line
from common.envelope import Envelope, MasterRef
from common.heartbeat import HeartbeatSender
from common.identity import Identity
from common.logging_setup import log_envelope
from common.transport import ConnectionClosed, FramingError, NdjsonConnection
from worker import handlers

logger = logging.getLogger(__name__)

SOCKET_POLL_S = 1.0
CONNECT_TIMEOUT_S = 5.0
REGISTER_RESPONSE_TIMEOUT_S = 5.0
REGISTER_MAX_ATTEMPTS = 3
RECONNECT_BACKOFF_INITIAL_S = 0.5
RECONNECT_BACKOFF_MAX_S = 5.0


class RegistrationRejected(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class WorkerAgent:
    def __init__(
        self,
        *,
        identity: Identity,
        home_master: MasterRef,
        socket_poll_s: float = SOCKET_POLL_S,
        connect_timeout_s: float = CONNECT_TIMEOUT_S,
        register_response_timeout_s: float = REGISTER_RESPONSE_TIMEOUT_S,
        register_max_attempts: int = REGISTER_MAX_ATTEMPTS,
        reconnect_backoff_initial_s: float = RECONNECT_BACKOFF_INITIAL_S,
        reconnect_backoff_max_s: float = RECONNECT_BACKOFF_MAX_S,
        heartbeat_interval_s: float = heartbeat_mod.HEARTBEAT_INTERVAL_S,
        heartbeat_response_timeout_s: float = heartbeat_mod.RESPONSE_TIMEOUT_S,
        heartbeat_max_missed: int = heartbeat_mod.MAX_MISSED_HEARTBEATS,
    ):
        self.identity = identity
        self.home_master = home_master
        self.self_ref = envelope_mod.node_ref_from_identity(identity)
        self.master_ref = home_master.to_node_ref()
        self.assignment_epoch = 0
        self._socket_poll_s = socket_poll_s
        self._connect_timeout_s = connect_timeout_s
        self._register_response_timeout_s = register_response_timeout_s
        self._register_max_attempts = register_max_attempts
        self._reconnect_backoff_initial_s = reconnect_backoff_initial_s
        self._reconnect_backoff_max_s = reconnect_backoff_max_s
        self._heartbeat_interval_s = heartbeat_interval_s
        self._heartbeat_response_timeout_s = heartbeat_response_timeout_s
        self._heartbeat_max_missed = heartbeat_max_missed
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        """Laco principal: conecta, registra, mantem heartbeat; reconecta com
        espera crescente ate 5s se a sessao cair (nunca por rejeicao)."""
        backoff = self._reconnect_backoff_initial_s
        while not self._stop_event.is_set():
            try:
                self._run_one_session()
                backoff = self._reconnect_backoff_initial_s  # sessao chegou a rodar: zera o backoff
            except RegistrationRejected as exc:
                logger.error("registro rejeitado pelo master: %s", exc.reason)
                return
            except (OSError, ConnectionClosed, FramingError) as exc:
                logger.warning("sessao com o master encerrada (%s); reconectando em %.1fs", exc, backoff)

            if self._stop_event.is_set():
                break
            self._stop_event.wait(backoff)
            backoff = min(backoff * 2, self._reconnect_backoff_max_s)

    def _run_one_session(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._connect_timeout_s)
        sock.connect((self.home_master.host, self.home_master.port))
        sock.settimeout(self._socket_poll_s)
        conn = NdjsonConnection(sock)
        pending = PendingResponses()
        try:
            self._register(conn, pending)
            self._steady_state(conn, pending)
        finally:
            conn.close()

    def _register(self, conn: NdjsonConnection, pending: PendingResponses) -> None:
        payload = {
            "home_master": self.home_master.to_dict(),
            "assignment_epoch": self.assignment_epoch,
            "loan_id": None,
        }
        message = envelope_mod.new_request_envelope(
            type="register_worker", source=self.self_ref, destination=self.master_ref, payload=payload
        )
        request_id = message["request_id"]
        dispatcher = Dispatcher(self_ref=self.self_ref)  # sem handlers: nada deveria chegar antes do ack

        for attempt in range(1, self._register_max_attempts + 1):
            pending.begin_wait(request_id)
            log_envelope(logger, direction="send", env=envelope_mod.parse_envelope(message))
            conn.send_message(message)

            response_env = self._poll_until(
                conn, dispatcher, pending, self._register_response_timeout_s,
                lambda: pending.try_take(request_id),
            )
            pending.cancel_wait(request_id)

            if response_env is not None:
                self._handle_registration_ack(response_env)
                return
            logger.warning(
                "sem registration_ack (tentativa %d/%d)", attempt, self._register_max_attempts
            )

        raise ConnectionClosed("register_worker sem resposta apos todas as tentativas")

    def _poll_until(self, conn, dispatcher, pending, timeout_s, condition):
        """Le linhas disponiveis (despachando o que chegar) ate condition()
        devolver algo diferente de None ou o timeout estourar. Nunca deixa
        o socket bloqueado por mais que SOCKET_POLL_S por vez."""
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                lines = conn.recv_lines()
            except socket.timeout:
                lines = []
            for raw_line in lines:
                response = process_raw_line(raw_line, dispatcher=dispatcher, pending_responses=pending)
                if response is not None:
                    conn.send_message(response)

            result = condition()
            if result is not None:
                return result
            if time.monotonic() >= deadline:
                return None

    def _handle_registration_ack(self, env: Envelope) -> None:
        payload = env.payload
        if not payload.get("accepted"):
            raise RegistrationRejected(payload.get("reason") or "rejeitado sem motivo informado")
        epoch = payload.get("assignment_epoch")
        if isinstance(epoch, int) and not isinstance(epoch, bool):
            self.assignment_epoch = epoch
        logger.info(
            "registrado com sucesso: %s (epoch=%d)", self.self_ref.node_label, self.assignment_epoch
        )

    def _steady_state(self, conn: NdjsonConnection, pending: PendingResponses) -> None:
        dispatcher = Dispatcher(self_ref=self.self_ref)
        dispatcher.register_handler("heartbeat", handlers.handle_heartbeat)

        heartbeat_sender = HeartbeatSender(
            self_ref=self.self_ref,
            peer_ref=self.master_ref,
            connection=conn,
            pending=pending,
            assignment_epoch_provider=lambda: self.assignment_epoch,
            interval_s=self._heartbeat_interval_s,
            response_timeout_s=self._heartbeat_response_timeout_s,
            max_missed=self._heartbeat_max_missed,
        )

        while not self._stop_event.is_set():
            try:
                lines = conn.recv_lines()
            except socket.timeout:
                lines = []

            for raw_line in lines:
                response = process_raw_line(raw_line, dispatcher=dispatcher, pending_responses=pending)
                if response is None:
                    continue
                response_env = envelope_mod.parse_envelope(response)
                log_envelope(logger, direction="send", env=response_env)
                conn.send_message(response)

            heartbeat_sender.tick()
            if heartbeat_sender.suspect:
                raise ConnectionClosed("heartbeat: 3 periodos consecutivos sem resposta valida do master")
