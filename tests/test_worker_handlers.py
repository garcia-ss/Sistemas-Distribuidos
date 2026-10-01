from common import envelope, naming
from worker import handlers

MASTER_REF = envelope.NodeRef(
    group_label="grupo_01",
    node_id="10d07f9e-ab14-4ee7-8b4f-1439c4987118",
    node_label="grupo_01_master_01",
    node_role=naming.ROLE_MASTER,
)
WORKER_REF = envelope.NodeRef(
    group_label="grupo_01",
    node_id="a7a1abe4-afdd-4d3d-96b8-d81b057ca325",
    node_label="grupo_01_master_01_worker_01",
    node_role=naming.ROLE_WORKER,
)


def test_worker_handle_heartbeat_acks():
    message = envelope.new_request_envelope(
        type="heartbeat", source=MASTER_REF, destination=WORKER_REF,
        payload={"sequence": 3, "assignment_epoch": None},
    )
    env = envelope.parse_envelope(message)

    response_type, payload = handlers.handle_heartbeat(env)

    assert response_type == "heartbeat_ack"
    assert payload == {"sequence": 3, "status": "alive"}


def test_worker_handle_heartbeat_rejects_invalid_sequence():
    message = envelope.new_request_envelope(
        type="heartbeat", source=MASTER_REF, destination=WORKER_REF,
        payload={"sequence": None, "assignment_epoch": None},
    )
    env = envelope.parse_envelope(message)

    response_type, payload = handlers.handle_heartbeat(env)

    assert response_type == "protocol_error"
    assert payload["error_code"] == "invalid_message"
