import pytest

from common import envelope, naming
from master import handlers
from master.registry import WorkerRegistry

SELF_MASTER_REF = envelope.MasterRef(
    group_label="grupo_01",
    node_id="10d07f9e-ab14-4ee7-8b4f-1439c4987118",
    node_label="grupo_01_master_01",
    host="127.0.0.1",
    port=9101,
)
WORKER_REF = envelope.NodeRef(
    group_label="grupo_01",
    node_id="a7a1abe4-afdd-4d3d-96b8-d81b057ca325",
    node_label="grupo_01_master_01_worker_01",
    node_role=naming.ROLE_WORKER,
)
OTHER_WORKER_REF = envelope.NodeRef(
    group_label="grupo_01",
    node_id="6e47c4ea-a223-411e-b4ce-08a5cfe4d31f",
    node_label="grupo_01_master_01_worker_02",
    node_role=naming.ROLE_WORKER,
)


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def _register_env(*, source=WORKER_REF, home_master=None, assignment_epoch=0, loan_id="__omit__"):
    payload = {
        "home_master": (home_master or SELF_MASTER_REF).to_dict(),
        "assignment_epoch": assignment_epoch,
    }
    if loan_id != "__omit__":
        payload["loan_id"] = loan_id
    else:
        payload["loan_id"] = None
    message = envelope.new_request_envelope(
        type="register_worker", source=source, destination=SELF_MASTER_REF.to_node_ref(), payload=payload
    )
    return envelope.parse_envelope(message)


def _heartbeat_env(*, source=WORKER_REF, sequence=1, assignment_epoch=0):
    message = envelope.new_request_envelope(
        type="heartbeat",
        source=source,
        destination=SELF_MASTER_REF.to_node_ref(),
        payload={"sequence": sequence, "assignment_epoch": assignment_epoch},
    )
    return envelope.parse_envelope(message)


# -- handle_register_worker -------------------------------------------------


def test_register_worker_accepts_valid_registration():
    registry = WorkerRegistry()
    conn = FakeConnection()
    env = _register_env()

    response_type, payload = handlers.handle_register_worker(
        env, connection=conn, registry=registry, self_master_ref=SELF_MASTER_REF
    )

    assert response_type == "registration_ack"
    assert payload == {"accepted": True, "assignment_epoch": 0, "reason": None}

    entry = registry.get(WORKER_REF.node_id)
    assert entry is not None
    assert entry.node_label == WORKER_REF.node_label
    assert entry.connection is conn


def test_register_worker_rejects_wrong_home_master():
    registry = WorkerRegistry()
    wrong_master = envelope.MasterRef(
        group_label="grupo_01",
        node_id=envelope.new_uuid4(),
        node_label="grupo_01_master_02",
        host="127.0.0.1",
        port=9199,
    )
    env = _register_env(home_master=wrong_master)

    response_type, payload = handlers.handle_register_worker(
        env, connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF
    )

    assert response_type == "registration_ack"
    assert payload["accepted"] is False
    assert registry.get(WORKER_REF.node_id) is None


def test_register_worker_rejects_non_null_loan_id():
    registry = WorkerRegistry()
    env = _register_env(loan_id=envelope.new_uuid4())

    _, payload = handlers.handle_register_worker(
        env, connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF
    )

    assert payload["accepted"] is False
    assert "loan_id" in payload["reason"]


def test_register_worker_rejects_missing_loan_id_key():
    registry = WorkerRegistry()
    message = envelope.new_request_envelope(
        type="register_worker",
        source=WORKER_REF,
        destination=SELF_MASTER_REF.to_node_ref(),
        payload={"home_master": SELF_MASTER_REF.to_dict(), "assignment_epoch": 0},
    )
    env = envelope.parse_envelope(message)

    _, payload = handlers.handle_register_worker(
        env, connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF
    )
    assert payload["accepted"] is False


def test_register_worker_rejects_epoch_divergent_from_existing_registration():
    registry = WorkerRegistry()
    handlers.handle_register_worker(
        _register_env(assignment_epoch=0),
        connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF,
    )

    _, payload = handlers.handle_register_worker(
        _register_env(assignment_epoch=5),
        connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF,
    )

    assert payload["accepted"] is False
    assert payload["assignment_epoch"] == 0  # ecoa o epoch atual, nao o divergente


def test_register_worker_reconnect_closes_previous_connection_same_node():
    registry = WorkerRegistry()
    first_conn = FakeConnection()
    second_conn = FakeConnection()

    handlers.handle_register_worker(
        _register_env(), connection=first_conn, registry=registry, self_master_ref=SELF_MASTER_REF
    )
    handlers.handle_register_worker(
        _register_env(), connection=second_conn, registry=registry, self_master_ref=SELF_MASTER_REF
    )

    assert first_conn.closed is True
    assert second_conn.closed is False
    assert registry.get(WORKER_REF.node_id).connection is second_conn


def test_register_worker_rejects_same_node_id_with_different_label():
    """Identidade duplicada: um node_id ja cadastrado reaparece com outro
    node_label -- isso NAO e uma reconexao normal, e uma inconsistencia."""
    registry = WorkerRegistry()
    handlers.handle_register_worker(
        _register_env(), connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF
    )

    forged_source = envelope.NodeRef(
        group_label=WORKER_REF.group_label,
        node_id=WORKER_REF.node_id,  # mesmo node_id
        node_label="grupo_01_master_01_worker_02",  # label diferente do cadastrado
        node_role=naming.ROLE_WORKER,
    )
    _, payload = handlers.handle_register_worker(
        _register_env(source=forged_source),
        connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF,
    )

    assert payload["accepted"] is False
    assert registry.get(WORKER_REF.node_id).node_label == WORKER_REF.node_label  # cadastro original intacto


def test_register_worker_does_not_confuse_two_different_workers():
    registry = WorkerRegistry()
    conn_a = FakeConnection()
    conn_b = FakeConnection()

    handlers.handle_register_worker(
        _register_env(source=WORKER_REF), connection=conn_a, registry=registry, self_master_ref=SELF_MASTER_REF
    )
    handlers.handle_register_worker(
        _register_env(source=OTHER_WORKER_REF), connection=conn_b, registry=registry, self_master_ref=SELF_MASTER_REF
    )

    assert conn_a.closed is False  # workers diferentes: nenhuma sessao e invalidada
    assert conn_b.closed is False
    assert registry.get(WORKER_REF.node_id).connection is conn_a
    assert registry.get(OTHER_WORKER_REF.node_id).connection is conn_b


# -- handle_heartbeat ---------------------------------------------------


def test_master_handle_heartbeat_acks_and_marks_registry():
    registry = WorkerRegistry()
    handlers.handle_register_worker(
        _register_env(), connection=FakeConnection(), registry=registry, self_master_ref=SELF_MASTER_REF
    )
    entry = registry.get(WORKER_REF.node_id)
    entry.missed_heartbeats = 2
    entry.suspect = True

    response_type, payload = handlers.handle_heartbeat(_heartbeat_env(sequence=7), registry=registry)

    assert response_type == "heartbeat_ack"
    assert payload == {"sequence": 7, "status": "alive"}
    assert entry.missed_heartbeats == 0
    assert entry.suspect is False


def test_master_handle_heartbeat_rejects_invalid_sequence():
    registry = WorkerRegistry()
    message = envelope.new_request_envelope(
        type="heartbeat", source=WORKER_REF, destination=SELF_MASTER_REF.to_node_ref(),
        payload={"sequence": "not-an-int", "assignment_epoch": 0},
    )
    env = envelope.parse_envelope(message)

    response_type, payload = handlers.handle_heartbeat(env, registry=registry)
    assert response_type == "protocol_error"
    assert payload["error_code"] == "invalid_message"


def test_master_handle_heartbeat_rejects_missing_assignment_epoch():
    registry = WorkerRegistry()
    message = envelope.new_request_envelope(
        type="heartbeat", source=WORKER_REF, destination=SELF_MASTER_REF.to_node_ref(),
        payload={"sequence": 1},
    )
    env = envelope.parse_envelope(message)

    response_type, _ = handlers.handle_heartbeat(env, registry=registry)
    assert response_type == "protocol_error"
