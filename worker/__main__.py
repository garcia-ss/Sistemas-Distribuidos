"""Ponto de entrada: python -m worker --group-number 1 --master-seq 1 --worker-seq 1 --master-port 9101"""

from __future__ import annotations

import logging
import signal
import sys
import threading

from common import naming
from common.envelope import MasterRef
from common.identity import load_or_create_identity
from common.logging_setup import configure_logging
from worker.agent import WorkerAgent
from worker.config import load_config

logger = logging.getLogger("worker")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    config = load_config(argv)

    identity = load_or_create_identity(
        config.identity_path,
        node_label=config.node_label,
        node_role=naming.ROLE_WORKER,
        group_label=config.group_label,
    )

    # home_master.node_id/node_label sao descobertos daqui a pouco, no
    # register_worker de verdade nao ha ambiguidade porque a Sprint 1 so tem
    # um master por config; identificamos pelo label/sequencia declarados.
    home_master_label = naming.make_master_label(config.group_label, config.master_seq)
    home_master = MasterRef(
        group_label=config.group_label,
        node_id=_resolve_master_node_id(config),
        node_label=home_master_label,
        host=config.master_host,
        port=config.master_port,
    )

    agent = WorkerAgent(identity=identity, home_master=home_master)

    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: (stop_event.set(), agent.stop()))
    try:
        signal.signal(signal.SIGTERM, lambda *_: (stop_event.set(), agent.stop()))
    except (ValueError, AttributeError):
        pass

    run_thread = threading.Thread(target=agent.run, daemon=True)
    run_thread.start()
    logger.info("worker %s pronto (node_id=%s)", identity.node_label, identity.node_id)

    stop_event.wait()
    logger.info("encerrando worker %s", identity.node_label)
    run_thread.join(timeout=5.0)
    return 0


def _resolve_master_node_id(config) -> str:
    """Le o node_id do master de origem a partir da identidade persistida
    dele em disco (config.master_identity_path -- ver --master-data-dir se o
    master usa um --data-dir customizado). Evita que o worker precise
    adivinhar ou copiar um UUID por fora."""
    import json

    master_identity_path = config.master_identity_path
    if not master_identity_path.exists():
        raise SystemExit(
            f"identidade do master de origem nao encontrada em {master_identity_path}; "
            "inicie o master antes do worker, e se o master usa --data-dir customizado "
            "passe o mesmo caminho aqui via --master-data-dir"
        )
    data = json.loads(master_identity_path.read_text(encoding="utf-8"))
    return data["node_id"]


if __name__ == "__main__":
    sys.exit(main())
