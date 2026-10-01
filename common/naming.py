"""Convencao de nomes do projeto: group_label e node_label (lower_snake_case)."""

from __future__ import annotations

import re
from dataclasses import dataclass

ROLE_MASTER = "master"
ROLE_WORKER = "worker"
ROLE_CLIENT = "client"
ROLE_COLLECTOR = "collector"

ALL_ROLES = (ROLE_MASTER, ROLE_WORKER, ROLE_CLIENT, ROLE_COLLECTOR)

_LOWER_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")

_GROUP_LABEL_RE = re.compile(r"^grupo_(\d{2})$")
_MASTER_LABEL_RE = re.compile(r"^(grupo_\d{2})_master_(\d{2})$")
_WORKER_LABEL_RE = re.compile(r"^(grupo_\d{2})_master_(\d{2})_worker_(\d{2})$")
_CLIENT_LABEL_RE = re.compile(r"^(grupo_\d{2})_client_(\d{2})$")
_COLLECTOR_LABEL_RE = re.compile(r"^(grupo_\d{2})_collector_(\d{2})$")


class InvalidLabelError(ValueError):
    """Label ou group_label fora da convencao lower_snake_case do projeto."""


def is_lower_snake_case(value: str) -> bool:
    return bool(_LOWER_SNAKE_CASE_RE.match(value))


def make_group_label(group_number: int) -> str:
    if not (1 <= group_number <= 99):
        raise InvalidLabelError(f"group_number fora do intervalo 1-99: {group_number}")
    return f"grupo_{group_number:02d}"


def validate_group_label(group_label: str) -> None:
    if not _GROUP_LABEL_RE.match(group_label):
        raise InvalidLabelError(f"group_label invalido: {group_label!r} (esperado grupo_XX)")


def make_master_label(group_label: str, master_seq: int) -> str:
    validate_group_label(group_label)
    if not (0 <= master_seq <= 99):
        raise InvalidLabelError(f"master_seq fora do intervalo 0-99: {master_seq}")
    return f"{group_label}_master_{master_seq:02d}"


def make_worker_label(group_label: str, master_seq: int, worker_seq: int) -> str:
    validate_group_label(group_label)
    if not (0 <= worker_seq <= 99):
        raise InvalidLabelError(f"worker_seq fora do intervalo 0-99: {worker_seq}")
    master_label = make_master_label(group_label, master_seq)
    return f"{master_label}_worker_{worker_seq:02d}"


def make_client_label(group_label: str, client_seq: int) -> str:
    validate_group_label(group_label)
    if not (0 <= client_seq <= 99):
        raise InvalidLabelError(f"client_seq fora do intervalo 0-99: {client_seq}")
    return f"{group_label}_client_{client_seq:02d}"


def make_collector_label(group_label: str, collector_seq: int) -> str:
    validate_group_label(group_label)
    if not (0 <= collector_seq <= 99):
        raise InvalidLabelError(f"collector_seq fora do intervalo 0-99: {collector_seq}")
    return f"{group_label}_collector_{collector_seq:02d}"


@dataclass(frozen=True)
class ParsedLabel:
    role: str
    group_label: str
    master_seq: int | None = None
    worker_seq: int | None = None
    seq: int | None = None


def parse_label(label: str) -> ParsedLabel:
    """Identifica role/group_label/sequencias a partir de um node_label completo.

    A ordem de tentativa nao importa: cada regex usa $ (fim de string), entao
    um worker_label (que contem "_master_XX" no meio) nunca casa com o
    padrao de master, que exige que a string termine logo apos "_master_XX".
    """
    if match := _WORKER_LABEL_RE.match(label):
        group_label, master_seq, worker_seq = match.groups()
        return ParsedLabel(
            role=ROLE_WORKER,
            group_label=group_label,
            master_seq=int(master_seq),
            worker_seq=int(worker_seq),
        )
    if match := _MASTER_LABEL_RE.match(label):
        group_label, master_seq = match.groups()
        return ParsedLabel(role=ROLE_MASTER, group_label=group_label, master_seq=int(master_seq))
    if match := _CLIENT_LABEL_RE.match(label):
        group_label, seq = match.groups()
        return ParsedLabel(role=ROLE_CLIENT, group_label=group_label, seq=int(seq))
    if match := _COLLECTOR_LABEL_RE.match(label):
        group_label, seq = match.groups()
        return ParsedLabel(role=ROLE_COLLECTOR, group_label=group_label, seq=int(seq))
    raise InvalidLabelError(f"node_label nao segue nenhuma convencao conhecida: {label!r}")
