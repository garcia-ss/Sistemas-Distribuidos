"""Configuracao do processo worker: grupo, master de origem, identidade.

Diferente do master, o worker declara TAMBEM o master a que pertence
(home_master: host/port) -- e ele quem inicia a conexao (secao 2.1: "o
worker inicia sua conexao com o master; nao precisa publicar uma porta
de escuta").
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from common import naming

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class WorkerConfig:
    group_label: str
    master_seq: int
    worker_seq: int
    master_host: str
    master_port: int
    data_dir: Path
    master_data_dir: Path

    @property
    def node_label(self) -> str:
        return naming.make_worker_label(self.group_label, self.master_seq, self.worker_seq)

    @property
    def identity_path(self) -> Path:
        return self.data_dir / "identity.json"

    @property
    def master_identity_path(self) -> Path:
        """Onde ler o node_id do master de origem (identity.json dele) para
        montar o home_master do register_worker. So funciona por convencao
        se o master usa o data-dir padrao; --master-data-dir sobrescreve
        quando o master foi iniciado com --data-dir customizado."""
        return self.master_data_dir / "identity.json"


def load_config(argv: list[str] | None = None) -> WorkerConfig:
    parser = argparse.ArgumentParser(description="Processo worker da farm P2P (Sprint 1)")
    parser.add_argument("--group-number", type=int, required=True)
    parser.add_argument("--master-seq", type=int, required=True, help="sequencia do MASTER de origem")
    parser.add_argument("--worker-seq", type=int, required=True, help="sequencia deste worker")
    parser.add_argument("--master-host", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, required=True)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument(
        "--master-data-dir", type=Path, default=None,
        help="data-dir usado pelo master de origem (so precisa ser informado se o "
             "master foi iniciado com --data-dir customizado; por padrao usa a "
             "mesma convencao REPO_ROOT/data/<label> de master/config.py)",
    )
    args = parser.parse_args(argv)

    group_label = naming.make_group_label(args.group_number)
    node_label = naming.make_worker_label(group_label, args.master_seq, args.worker_seq)
    master_label = naming.make_master_label(group_label, args.master_seq)
    data_dir = args.data_dir or (REPO_ROOT / "data" / node_label)
    master_data_dir = args.master_data_dir or (REPO_ROOT / "data" / master_label)

    return WorkerConfig(
        group_label=group_label,
        master_seq=args.master_seq,
        worker_seq=args.worker_seq,
        master_host=args.master_host,
        master_port=args.master_port,
        data_dir=data_dir,
        master_data_dir=master_data_dir,
    )
