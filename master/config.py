"""Configuracao do processo master: grupo, sequencia, host/port, identidade.

So o master le isso. worker/config.py tem o equivalente do lado do worker,
com os campos que fazem sentido para um worker (inclui home_master).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from common import naming

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class MasterConfig:
    group_label: str
    master_seq: int
    host: str
    port: int
    data_dir: Path

    @property
    def node_label(self) -> str:
        return naming.make_master_label(self.group_label, self.master_seq)

    @property
    def identity_path(self) -> Path:
        return self.data_dir / "identity.json"


def load_config(argv: list[str] | None = None) -> MasterConfig:
    parser = argparse.ArgumentParser(description="Processo master da farm P2P (Sprint 1)")
    parser.add_argument("--group-number", type=int, required=True, help="numero do grupo (1-99)")
    parser.add_argument("--master-seq", type=int, required=True, help="sequencia do master (ex: 1)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    group_label = naming.make_group_label(args.group_number)
    node_label = naming.make_master_label(group_label, args.master_seq)
    data_dir = args.data_dir or (REPO_ROOT / "data" / node_label)

    return MasterConfig(
        group_label=group_label,
        master_seq=args.master_seq,
        host=args.host,
        port=args.port,
        data_dir=data_dir,
    )
