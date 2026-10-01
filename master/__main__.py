"""Ponto de entrada: python -m master --group-number 1 --master-seq 1 --port 9101"""

from __future__ import annotations

import logging
import signal
import sys
import threading

from common import envelope as envelope_mod
from common import naming
from common.identity import load_or_create_identity
from common.logging_setup import configure_logging
from master.config import load_config
from master.server import MasterServer

logger = logging.getLogger("master")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    config = load_config(argv)

    identity = load_or_create_identity(
        config.identity_path,
        node_label=config.node_label,
        node_role=naming.ROLE_MASTER,
        group_label=config.group_label,
    )
    self_master_ref = envelope_mod.master_ref_from_identity(identity, host=config.host, port=config.port)

    server = MasterServer(self_master_ref=self_master_ref)
    server.start()

    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    try:
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    except (ValueError, AttributeError):
        pass  # SIGTERM nao disponivel em todas as plataformas/threads

    logger.info("master %s pronto (node_id=%s)", identity.node_label, identity.node_id)
    stop_event.wait()
    logger.info("encerrando master %s", identity.node_label)
    server.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
