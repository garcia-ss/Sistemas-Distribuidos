"""Testes de integracao ponta a ponta: MasterServer + WorkerAgent reais sobre
TCP em 127.0.0.1 (porta efemera). Cobrem os criterios de "pronto quando" da
Sprint 1 que so fazem sentido com socket de verdade: dois workers registrando
como sessoes distintas, heartbeat mantendo a sessao viva por varios ciclos, e
reconexao apos reinicio do worker sem duplicar o cadastro no master.

Intervalos de heartbeat/timeout sao reduzidos via os parametros que
MasterServer/WorkerAgent expoem (a mesma configurabilidade pedida na secao
2.2 do plano) para o teste rodar em fracoes de segundo, nao em 10s/30s reais.
"""

from __future__ import annotations

import threading
import time

from common import envelope as envelope_mod
from common import identity as identity_mod
from common import naming
from master.server import MasterServer
from worker.agent import WorkerAgent


def _make_master(tmp_path, **overrides):
    identity = identity_mod.load_or_create_identity(
        tmp_path / "master" / "identity.json",
        node_label="grupo_01_master_01", node_role=naming.ROLE_MASTER, group_label="grupo_01",
    )
    self_master_ref = envelope_mod.master_ref_from_identity(identity, host="127.0.0.1", port=0)
    kwargs = dict(socket_poll_s=0.05, heartbeat_interval_s=0.1, heartbeat_response_timeout_s=0.3,
                  heartbeat_max_missed=3)
    kwargs.update(overrides)
    server = MasterServer(self_master_ref=self_master_ref, **kwargs)
    server.start()
    return server, identity


def _make_worker(tmp_path, *, worker_seq, master_node_id, master_port, **overrides):
    label = naming.make_worker_label("grupo_01", 1, worker_seq)
    identity = identity_mod.load_or_create_identity(
        tmp_path / f"worker{worker_seq}" / "identity.json",
        node_label=label, node_role=naming.ROLE_WORKER, group_label="grupo_01",
    )
    home_master = envelope_mod.MasterRef(
        group_label="grupo_01", node_id=master_node_id, node_label="grupo_01_master_01",
        host="127.0.0.1", port=master_port,
    )
    kwargs = dict(
        socket_poll_s=0.05, connect_timeout_s=2.0, register_response_timeout_s=0.5,
        register_max_attempts=3, reconnect_backoff_initial_s=0.05, reconnect_backoff_max_s=0.2,
        heartbeat_interval_s=0.1, heartbeat_response_timeout_s=0.3, heartbeat_max_missed=3,
    )
    kwargs.update(overrides)
    return WorkerAgent(identity=identity, home_master=home_master, **kwargs)


def _wait_until(condition, timeout_s=3.0, interval_s=0.02) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval_s)
    return False


def test_master_and_two_local_workers_register_as_distinct_sessions(tmp_path):
    server, master_identity = _make_master(tmp_path)
    agent1 = _make_worker(tmp_path, worker_seq=1, master_node_id=master_identity.node_id, master_port=server.bound_port)
    agent2 = _make_worker(tmp_path, worker_seq=2, master_node_id=master_identity.node_id, master_port=server.bound_port)
    thread1 = threading.Thread(target=agent1.run, daemon=True)
    thread2 = threading.Thread(target=agent2.run, daemon=True)
    try:
        thread1.start()
        thread2.start()

        assert _wait_until(lambda: len(server.registry.snapshot()) == 2)

        labels = {entry.node_label for entry in server.registry.snapshot()}
        node_ids = {entry.node_id for entry in server.registry.snapshot()}
        assert labels == {"grupo_01_master_01_worker_01", "grupo_01_master_01_worker_02"}
        assert len(node_ids) == 2  # sessoes distintas, nenhuma se sobrepoe
    finally:
        agent1.stop()
        agent2.stop()
        thread1.join(timeout=2.0)
        thread2.join(timeout=2.0)
        server.stop()


def test_heartbeat_keeps_session_alive_across_multiple_cycles(tmp_path):
    server, master_identity = _make_master(tmp_path)
    agent = _make_worker(tmp_path, worker_seq=1, master_node_id=master_identity.node_id, master_port=server.bound_port)
    thread = threading.Thread(target=agent.run, daemon=True)
    try:
        thread.start()
        assert _wait_until(lambda: len(server.registry.snapshot()) == 1)

        time.sleep(0.5)  # varios ciclos de heartbeat de 0.1s nesse intervalo

        entry = server.registry.snapshot()[0]
        assert entry.suspect is False
        assert entry.missed_heartbeats == 0
    finally:
        agent.stop()
        thread.join(timeout=2.0)
        server.stop()


def test_worker_restart_reconnects_without_duplicating_registration(tmp_path):
    server, master_identity = _make_master(tmp_path)
    agent1 = _make_worker(tmp_path, worker_seq=1, master_node_id=master_identity.node_id, master_port=server.bound_port)
    thread1 = threading.Thread(target=agent1.run, daemon=True)
    agent2 = None
    thread2 = None
    try:
        thread1.start()
        assert _wait_until(lambda: len(server.registry.snapshot()) == 1)
        first_connection = server.registry.snapshot()[0].connection

        # "processo worker encerrado": para de forma limpa (equivalente a
        # matar e reiniciar o processo, ja que a identidade persiste em disco)
        agent1.stop()
        thread1.join(timeout=2.0)
        assert _wait_until(lambda: len(server.registry.snapshot()) == 0)

        agent2 = _make_worker(
            tmp_path, worker_seq=1, master_node_id=master_identity.node_id, master_port=server.bound_port
        )
        assert agent2.identity.node_id == agent1.identity.node_id  # mesma identidade, carregada do disco

        thread2 = threading.Thread(target=agent2.run, daemon=True)
        thread2.start()

        assert _wait_until(lambda: len(server.registry.snapshot()) == 1)
        entries = server.registry.snapshot()
        assert len(entries) == 1  # nunca duplicou o cadastro
        assert entries[0].node_label == "grupo_01_master_01_worker_01"
        assert entries[0].connection is not first_connection
    finally:
        agent1.stop()
        thread1.join(timeout=2.0)
        if agent2 is not None:
            agent2.stop()
        if thread2 is not None:
            thread2.join(timeout=2.0)
        server.stop()
