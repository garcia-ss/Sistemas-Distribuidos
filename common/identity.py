"""Identidade persistida de um no (master/worker/client/collector).

O node_id (UUID4) e o node_label sao gerados uma unica vez e gravados em
disco. Toda inicializacao subsequente do processo carrega esse arquivo em
vez de recriar a identidade -- e valida que o que esta declarado na config
(role, group_label, label) bate com o que foi persistido.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from common import naming


class IdentityError(ValueError):
    """Config divergente do que ja foi persistido, ou label/role invalidos."""


@dataclass(frozen=True)
class Identity:
    node_id: str
    node_label: str
    node_role: str
    group_label: str


def _atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def load_or_create_identity(
    path: Path,
    *,
    node_label: str,
    node_role: str,
    group_label: str,
) -> Identity:
    if node_role not in naming.ALL_ROLES:
        raise IdentityError(f"node_role desconhecido: {node_role!r}")

    parsed = naming.parse_label(node_label)
    if parsed.role != node_role:
        raise IdentityError(
            f"node_label {node_label!r} corresponde ao role {parsed.role!r}, "
            f"mas node_role declarado e {node_role!r}"
        )
    if parsed.group_label != group_label:
        raise IdentityError(
            f"group_label declarado ({group_label!r}) diverge do prefixo "
            f"embutido em node_label ({parsed.group_label!r})"
        )

    if path.exists():
        stored = _load(path)
        if stored.node_label != node_label:
            raise IdentityError(
                f"colisao de identidade: arquivo {path} ja pertence ao label "
                f"{stored.node_label!r}, nao a {node_label!r}"
            )
        if stored.node_role != node_role:
            raise IdentityError(
                f"divergencia de role: identidade persistida e {stored.node_role!r}, "
                f"declarado agora e {node_role!r}"
            )
        if stored.group_label != group_label:
            raise IdentityError(
                f"divergencia de group_label: identidade persistida e "
                f"{stored.group_label!r}, declarado agora e {group_label!r}"
            )
        return stored

    identity = Identity(
        node_id=str(uuid.uuid4()),
        node_label=node_label,
        node_role=node_role,
        group_label=group_label,
    )
    _atomic_write_json(path, asdict(identity))
    return identity


def _load(path: Path) -> Identity:
    data = json.loads(path.read_text(encoding="utf-8"))
    return Identity(
        node_id=data["node_id"],
        node_label=data["node_label"],
        node_role=data["node_role"],
        group_label=data["group_label"],
    )
