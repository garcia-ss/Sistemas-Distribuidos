"""Servidor TCP do master: aceita conexoes de workers, uma thread por conexao.

Cada thread de conexao le o socket com timeout curto (nunca bloqueia
indefinidamente em recv()) para poder, na mesma volta do laco, tambem
verificar se e hora de mandar o proximo heartbeat aquele worker -- assim
o envio de heartbeat do master nunca depende do worker falar primeiro,
sem precisar de uma thread separada so pra isso.

O master NAO executa tarefas (secao 2.1: "nao executa o trabalho destinado
ao worker") -- so esta thread de rede existe por conexao, nao ha uma
segunda thread de execucao aqui como no worker.
"""

from __future__ import annotations

import logging
import socket
import threading

from common import envelope as envelope_mod
from common import heartbeat as heartbeat_mod
from common import naming
from common.dispatcher import Dispatcher, PendingResponses, process_raw_line
from common.envelope import MasterRef, NodeRef
from common.heartbeat import HeartbeatSender
from common.logging_setup import log_envelope
from common.transport import ConnectionClosed, FramingError, NdjsonConnection
from master import handlers
from master.registry import WorkerRegistry

logger = logging.getLogger(__name__)

SOCKET_POLL_S = 1.0


class MasterServer:
    def __init__(
        self,
        *,
        self_master_ref: MasterRef,
        registry: WorkerRegistry | None = None,
        socket_poll_s: float = SOCKET_POLL_S,
        heartbeat_interval_s: float = heartbeat_mod.HEARTBEAT_INTERVAL_S,
        heartbeat_response_timeout_s: float = heartbeat_mod.RESPONSE_TIMEOUT_S,
        heartbeat_max_missed: int = heartbeat_mod.MAX_MISSED_HEARTBEATS,
    ):
        self.self_master_ref = self_master_ref
        self.self_ref: NodeRef = self_master_ref.to_node_ref()
        self.registry = registry or WorkerRegistry()
        self._socket_poll_s = socket_poll_s
        self._heartbeat_interval_s = heartbeat_interval_s
        self._heartbeat_response_timeout_s = heartbeat_response_timeout_s
        self._heartbeat_max_missed = heartbeat_max_missed
        self._listen_socket: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._connection_threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self.bound_port: int | None = None

    def start(self) -> None:
        self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen_socket.bind((self.self_master_ref.host, self.self_master_ref.port))
        self.bound_port = self._listen_socket.getsockname()[1]
        self._listen_socket.listen()
        self._listen_socket.settimeout(self._socket_poll_s)
        logger.info(
            "master %s escutando em %s:%d",
            self.self_master_ref.node_label, self.self_master_ref.host, self.bound_port,
        )
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._listen_socket is not None:
            self._listen_socket.close()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=self._socket_poll_s + 1.0)
        for entry in self.registry.snapshot():
            entry.connection.close()
        for thread in self._connection_threads:
            thread.join(timeout=self._socket_poll_s + 1.0)

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                client_socket, addr = self._listen_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break  # socket de escuta foi fechado por stop()

            thread = threading.Thread(target=self._connection_loop, args=(client_socket, addr), daemon=True)
            self._connection_threads.append(thread)
            thread.start()

    def _connection_loop(self, client_socket: socket.socket, addr) -> None:
        client_socket.settimeout(self._socket_poll_s)
        conn = NdjsonConnection(client_socket)
        pending = PendingResponses()
        dispatcher = Dispatcher(self_ref=self.self_ref)
        dispatcher.register_handler(
            "register_worker",
            lambda env: handlers.handle_register_worker(
                env, connection=conn, registry=self.registry, self_master_ref=self.self_master_ref
            ),
        )
        dispatcher.register_handler(
            "heartbeat", lambda env: handlers.handle_heartbeat(env, registry=self.registry)
        )

        registered_node_id: str | None = None
        heartbeat_sender: HeartbeatSender | None = None

        try:
            while not self._stop_event.is_set():
                try:
                    lines = conn.recv_lines()
                except socket.timeout:
                    lines = []
                except (ConnectionClosed, FramingError) as exc:
                    logger.info("conexao de %s encerrada: %s", addr, exc)
                    break
                except OSError as exc:
                    # socket fechado por fora, reset pelo peer, etc: mesmo
                    # tratamento de sessao encerrada, nao derruba a thread.
                    logger.info("conexao de %s encerrada (erro de socket): %s", addr, exc)
                    break

                for raw_line in lines:
                    response = process_raw_line(raw_line, dispatcher=dispatcher, pending_responses=pending)
                    if response is None:
                        continue
                    response_env = envelope_mod.parse_envelope(response)
                    log_envelope(logger, direction="send", env=response_env)
                    conn.send_message(response)

                    if (
                        registered_node_id is None
                        and response_env.type == "registration_ack"
                        and response_env.payload.get("accepted")
                    ):
                        registered_node_id = response_env.destination.node_id
                        worker_ref = NodeRef(
                            group_label=response_env.destination.group_label,
                            node_id=response_env.destination.node_id,
                            node_label=response_env.destination.node_label,
                            node_role=naming.ROLE_WORKER,
                        )
                        heartbeat_sender = HeartbeatSender(
                            self_ref=self.self_ref, peer_ref=worker_ref, connection=conn, pending=pending,
                            interval_s=self._heartbeat_interval_s,
                            response_timeout_s=self._heartbeat_response_timeout_s,
                            max_missed=self._heartbeat_max_missed,
                        )

                if heartbeat_sender is not None:
                    heartbeat_sender.tick()
                    if heartbeat_sender.suspect:
                        logger.warning("encerrando sessao com %s: heartbeat suspeito", addr)
                        break
        finally:
            conn.close()
            if registered_node_id is not None:
                entry = self.registry.get(registered_node_id)
                if entry is not None and entry.connection is conn:
                    self.registry.remove(registered_node_id)
                    logger.info("worker removido do registro: node_id=%s", registered_node_id)
