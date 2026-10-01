"""Envelope obrigatorio do protocolo (secao 3.3 do plano).

Modulo comum: master, worker, client e collector usam exatamente este
mesmo formato de envelope -- e por isso que fica em common/, nao em
master/ ou worker/. Aqui so validamos a ESTRUTURA do envelope (version,
ids, source/destination, presenca do payload); decidir se um "type" e
suportado e o que fazer com o payload e responsabilidade do dispatcher
(common/dispatcher.py) e dos handlers especificos de cada role.
"""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass

from common import naming
from common.errors import ProtocolError

PROTOCOL_VERSION = "sd_2026_2_v1"

_UUID4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

_REQUIRED_TOP_FIELDS = {
    "version",
    "type",
    "message_id",
    "request_id",
    "timestamp_ms",
    "source",
    "destination",
    "payload",
}

_REQUIRED_NODE_REF_FIELDS = {"group_label", "node_id", "node_label", "node_role"}


class EnvelopeError(ProtocolError):
    """Envelope estruturalmente invalido: campo ausente, tipo errado, etc."""


def is_uuid4_string(value: object) -> bool:
    return isinstance(value, str) and bool(_UUID4_RE.match(value))


def new_uuid4() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class NodeRef:
    group_label: str
    node_id: str
    node_label: str
    node_role: str

    def to_dict(self) -> dict:
        return {
            "group_label": self.group_label,
            "node_id": self.node_id,
            "node_label": self.node_label,
            "node_role": self.node_role,
        }


def node_ref_from_identity(identity) -> NodeRef:
    return NodeRef(
        group_label=identity.group_label,
        node_id=identity.node_id,
        node_label=identity.node_label,
        node_role=identity.node_role,
    )


@dataclass(frozen=True)
class MasterRef:
    """node_ref + host/port (secao 3.4). Usado no payload de register_worker
    (home_master) para o worker dizer a que master ele pertence e onde
    alcanca-lo -- nao e o source/destination do envelope, que continua
    sendo so NodeRef."""

    group_label: str
    node_id: str
    node_label: str
    host: str
    port: int
    node_role: str = naming.ROLE_MASTER

    def to_dict(self) -> dict:
        return {
            "group_label": self.group_label,
            "node_id": self.node_id,
            "node_label": self.node_label,
            "node_role": self.node_role,
            "host": self.host,
            "port": self.port,
        }

    def to_node_ref(self) -> NodeRef:
        return NodeRef(
            group_label=self.group_label,
            node_id=self.node_id,
            node_label=self.node_label,
            node_role=self.node_role,
        )


def master_ref_from_identity(identity, *, host: str, port: int) -> MasterRef:
    if identity.node_role != naming.ROLE_MASTER:
        raise ValueError(f"identity.node_role deve ser master, recebido {identity.node_role!r}")
    return MasterRef(
        group_label=identity.group_label,
        node_id=identity.node_id,
        node_label=identity.node_label,
        host=host,
        port=port,
    )


def parse_master_ref(value: object, *, field_name: str) -> MasterRef:
    node_ref = parse_node_ref(value, field_name=field_name)
    if node_ref.node_role != naming.ROLE_MASTER:
        raise EnvelopeError("invalid_message", f"{field_name}.node_role deve ser master")

    host = value.get("host")
    port = value.get("port")
    if not isinstance(host, str) or not host:
        raise EnvelopeError("invalid_message", f"{field_name}.host deve ser string nao vazia")
    if not isinstance(port, int) or isinstance(port, bool) or not (1 <= port <= 65535):
        raise EnvelopeError("invalid_message", f"{field_name}.port deve ser inteiro entre 1 e 65535")

    return MasterRef(
        group_label=node_ref.group_label,
        node_id=node_ref.node_id,
        node_label=node_ref.node_label,
        host=host,
        port=port,
    )


@dataclass(frozen=True)
class Envelope:
    version: str
    type: str
    message_id: str
    request_id: str
    timestamp_ms: int
    source: NodeRef
    destination: NodeRef
    payload: dict


def build_envelope(
    *,
    type: str,
    source: NodeRef,
    destination: NodeRef,
    payload: dict,
    request_id: str,
    message_id: str | None = None,
    timestamp_ms: int | None = None,
) -> dict:
    return {
        "version": PROTOCOL_VERSION,
        "type": type,
        "message_id": message_id or new_uuid4(),
        "request_id": request_id,
        "timestamp_ms": timestamp_ms if timestamp_ms is not None else _now_ms(),
        "source": source.to_dict(),
        "destination": destination.to_dict(),
        "payload": payload,
    }


def new_request_envelope(*, type: str, source: NodeRef, destination: NodeRef, payload: dict) -> dict:
    """Abre uma nova solicitacao: gera um request_id novo."""
    return build_envelope(
        type=type, source=source, destination=destination, payload=payload, request_id=new_uuid4()
    )


def response_envelope(
    *, type: str, source: NodeRef, destination: NodeRef, payload: dict, request_id: str
) -> dict:
    """Responde a uma solicitacao: preserva o request_id recebido, novo message_id."""
    return build_envelope(
        type=type, source=source, destination=destination, payload=payload, request_id=request_id
    )


def parse_node_ref(value: object, *, field_name: str) -> NodeRef:
    if not isinstance(value, dict):
        raise EnvelopeError("invalid_message", f"{field_name} deve ser um objeto JSON")

    missing = _REQUIRED_NODE_REF_FIELDS - value.keys()
    if missing:
        raise EnvelopeError(
            "invalid_message", f"{field_name} com campos ausentes: {sorted(missing)}"
        )

    group_label = value["group_label"]
    node_id = value["node_id"]
    node_label = value["node_label"]
    node_role = value["node_role"]

    if not isinstance(group_label, str):
        raise EnvelopeError("invalid_message", f"{field_name}.group_label deve ser string")
    try:
        naming.validate_group_label(group_label)
    except naming.InvalidLabelError as exc:
        raise EnvelopeError("invalid_message", f"{field_name}.group_label invalido: {exc}") from exc

    if not is_uuid4_string(node_id):
        raise EnvelopeError("invalid_message", f"{field_name}.node_id nao e um UUID4 valido")

    if node_role not in naming.ALL_ROLES:
        raise EnvelopeError("invalid_message", f"{field_name}.node_role desconhecido: {node_role!r}")

    if not isinstance(node_label, str):
        raise EnvelopeError("invalid_message", f"{field_name}.node_label deve ser string")
    try:
        parsed_label = naming.parse_label(node_label)
    except naming.InvalidLabelError as exc:
        raise EnvelopeError("invalid_message", f"{field_name}.node_label invalido: {exc}") from exc

    if parsed_label.role != node_role:
        raise EnvelopeError(
            "invalid_message",
            f"{field_name}.node_label ({node_label!r}) nao corresponde a node_role ({node_role!r})",
        )
    if parsed_label.group_label != group_label:
        raise EnvelopeError(
            "invalid_message",
            f"{field_name}.node_label ({node_label!r}) nao corresponde a group_label ({group_label!r})",
        )

    return NodeRef(group_label=group_label, node_id=node_id, node_label=node_label, node_role=node_role)


def parse_envelope(message: dict) -> Envelope:
    if not isinstance(message, dict):
        raise EnvelopeError("invalid_message", "mensagem deve ser um objeto JSON")

    missing = _REQUIRED_TOP_FIELDS - message.keys()
    if missing:
        raise EnvelopeError("invalid_message", f"campos obrigatorios ausentes: {sorted(missing)}")

    version = message["version"]
    if version != PROTOCOL_VERSION:
        raise EnvelopeError("unsupported_version", f"version nao suportada: {version!r}")

    type_ = message["type"]
    if not isinstance(type_, str) or not naming.is_lower_snake_case(type_):
        raise EnvelopeError("invalid_message", f"type invalido: {type_!r}")

    message_id = message["message_id"]
    if not is_uuid4_string(message_id):
        raise EnvelopeError("invalid_message", f"message_id nao e um UUID4 valido: {message_id!r}")

    request_id = message["request_id"]
    if not is_uuid4_string(request_id):
        raise EnvelopeError("invalid_message", f"request_id nao e um UUID4 valido: {request_id!r}")

    timestamp_ms = message["timestamp_ms"]
    if not isinstance(timestamp_ms, int) or isinstance(timestamp_ms, bool) or timestamp_ms < 0:
        raise EnvelopeError("invalid_message", f"timestamp_ms invalido: {timestamp_ms!r}")

    source = parse_node_ref(message["source"], field_name="source")
    destination = parse_node_ref(message["destination"], field_name="destination")

    payload = message["payload"]
    if not isinstance(payload, dict):
        raise EnvelopeError("invalid_message", "payload deve ser um objeto JSON")

    return Envelope(
        version=version,
        type=type_,
        message_id=message_id,
        request_id=request_id,
        timestamp_ms=timestamp_ms,
        source=source,
        destination=destination,
        payload=payload,
    )
