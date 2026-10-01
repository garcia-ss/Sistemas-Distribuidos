"""Configuracao de logging e helper para registrar eventos de mensagem.

Todo log de uma mensagem do protocolo inclui group, origem, destino, type e
request_id -- exigencia explicita da Sprint 1 ("registrar logs com grupo,
origem, destino, type e request_id"). Comum a master e worker.
"""

from __future__ import annotations

import logging

from common.envelope import Envelope


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def log_envelope(logger: logging.Logger, *, direction: str, env: Envelope) -> None:
    logger.info(
        "%s group=%s origem=%s destino=%s type=%s request_id=%s",
        direction,
        env.source.group_label,
        env.source.node_label,
        env.destination.node_label,
        env.type,
        env.request_id,
    )
