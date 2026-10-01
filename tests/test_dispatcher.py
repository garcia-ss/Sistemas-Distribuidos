import json
import threading
import time

import pytest

from common import dispatcher, envelope, naming

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


def _heartbeat_envelope(payload=None) -> envelope.Envelope:
    message = envelope.new_request_envelope(
        type="heartbeat",
        source=WORKER_REF,
        destination=MASTER_REF,
        payload=payload or {"sequence": 1, "assignment_epoch": 0},
    )
    return envelope.parse_envelope(message)


# -- Dispatcher: roteamento por type --------------------------------------


def test_dispatch_unregistered_type_returns_unsupported_type():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    env = _heartbeat_envelope()

    response = d.dispatch(env)

    assert response["type"] == "protocol_error"
    assert response["payload"]["error_code"] == "unsupported_type"
    assert response["request_id"] == env.request_id
    assert response["destination"] == WORKER_REF.to_dict()


def test_dispatch_calls_registered_handler_and_wraps_response():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    calls = []

    def handle_heartbeat(env):
        calls.append(env)
        return ("heartbeat_ack", {"sequence": env.payload["sequence"], "status": "alive"})

    d.register_handler("heartbeat", handle_heartbeat)
    env = _heartbeat_envelope()

    response = d.dispatch(env)

    assert len(calls) == 1
    assert response["type"] == "heartbeat_ack"
    assert response["request_id"] == env.request_id
    assert response["source"] == MASTER_REF.to_dict()
    assert response["destination"] == WORKER_REF.to_dict()
    assert response["payload"] == {"sequence": 1, "status": "alive"}


def test_dispatch_protocol_error_never_generates_another_protocol_error():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    calls = []
    d.register_handler("protocol_error", lambda env: calls.append(env))

    message = envelope.new_request_envelope(
        type="protocol_error",
        source=WORKER_REF,
        destination=MASTER_REF,
        payload={"error_code": "unsupported_type", "error_message": "x"},
    )
    env = envelope.parse_envelope(message)

    assert d.dispatch(env) is None
    assert calls == []  # nem chama o handler registrado


# -- Dispatcher: dedup ------------------------------------------------------


def test_dispatch_deduplicates_retransmission_with_same_payload():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    call_count = 0

    def handle_heartbeat(env):
        nonlocal call_count
        call_count += 1
        return ("heartbeat_ack", {"sequence": env.payload["sequence"], "status": "alive"})

    d.register_handler("heartbeat", handle_heartbeat)
    env = _heartbeat_envelope()

    first_response = d.dispatch(env)
    second_response = d.dispatch(env)  # retransmissao: mesmo request_id/type/payload

    assert call_count == 1  # handler nao roda de novo
    assert second_response == first_response


def test_dispatch_rejects_same_key_with_different_payload_as_conflict():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    d.register_handler("heartbeat", lambda env: ("heartbeat_ack", {"status": "alive"}))

    message = envelope.new_request_envelope(
        type="heartbeat", source=WORKER_REF, destination=MASTER_REF, payload={"sequence": 1}
    )
    env = envelope.parse_envelope(message)
    d.dispatch(env)

    conflicting = dict(message)
    conflicting["payload"] = {"sequence": 2}  # mesmo request_id, conteudo diferente
    conflicting_env = envelope.parse_envelope(conflicting)

    response = d.dispatch(conflicting_env)
    assert response["payload"]["error_code"] == "request_conflict"


# -- PendingResponses: correlacao de respostas ------------------------------


def test_pending_responses_claim_delivers_to_waiting_request_id():
    pending = dispatcher.PendingResponses()
    request_id = envelope.new_uuid4()
    pending.begin_wait(request_id)

    ack_message = envelope.response_envelope(
        type="heartbeat_ack",
        source=MASTER_REF,
        destination=WORKER_REF,
        payload={"sequence": 1, "status": "alive"},
        request_id=request_id,
    )
    ack_env = envelope.parse_envelope(ack_message)

    assert pending.claim(ack_env) is True
    result = pending.wait(request_id, timeout=1.0)
    assert result.payload == {"sequence": 1, "status": "alive"}


def test_pending_responses_claim_returns_false_without_waiter():
    pending = dispatcher.PendingResponses()
    env = _heartbeat_envelope()
    assert pending.claim(env) is False


def test_pending_responses_wait_times_out_when_never_claimed():
    pending = dispatcher.PendingResponses()
    request_id = envelope.new_uuid4()
    pending.begin_wait(request_id)

    start = time.monotonic()
    result = pending.wait(request_id, timeout=0.05)
    elapsed = time.monotonic() - start

    assert result is None
    assert elapsed < 1.0


def test_pending_responses_claim_from_another_thread():
    pending = dispatcher.PendingResponses()
    request_id = envelope.new_uuid4()
    pending.begin_wait(request_id)

    ack_message = envelope.response_envelope(
        type="heartbeat_ack",
        source=MASTER_REF,
        destination=WORKER_REF,
        payload={"sequence": 1, "status": "alive"},
        request_id=request_id,
    )
    ack_env = envelope.parse_envelope(ack_message)

    def claim_later():
        time.sleep(0.02)
        pending.claim(ack_env)

    threading.Thread(target=claim_later).start()
    result = pending.wait(request_id, timeout=1.0)
    assert result is not None


# -- process_raw_line: pipeline completo ------------------------------------


def test_process_raw_line_drops_malformed_json_without_raising():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    pending = dispatcher.PendingResponses()
    response = dispatcher.process_raw_line(
        b'{"type": "heartbeat",}', dispatcher=d, pending_responses=pending
    )
    assert response is None


def test_process_raw_line_returns_correlated_protocol_error_for_bad_version():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    pending = dispatcher.PendingResponses()

    message = envelope.new_request_envelope(
        type="heartbeat", source=WORKER_REF, destination=MASTER_REF, payload={"sequence": 1}
    )
    message["version"] = "unknown_version"
    raw_line = json.dumps(message).encode("utf-8")

    response = dispatcher.process_raw_line(raw_line, dispatcher=d, pending_responses=pending)

    assert response["type"] == "protocol_error"
    assert response["payload"]["error_code"] == "unsupported_version"
    assert response["request_id"] == message["request_id"]
    assert response["destination"] == WORKER_REF.to_dict()


def test_process_raw_line_delivers_claimed_response_and_returns_none():
    d = dispatcher.Dispatcher(self_ref=WORKER_REF)
    pending = dispatcher.PendingResponses()

    request_id = envelope.new_uuid4()
    pending.begin_wait(request_id)

    ack_message = envelope.response_envelope(
        type="heartbeat_ack",
        source=MASTER_REF,
        destination=WORKER_REF,
        payload={"sequence": 1, "status": "alive"},
        request_id=request_id,
    )
    raw_line = json.dumps(ack_message).encode("utf-8")

    response = dispatcher.process_raw_line(raw_line, dispatcher=d, pending_responses=pending)
    assert response is None  # entregue ao waiter, nao ao dispatcher

    result = pending.wait(request_id, timeout=1.0)
    assert result.payload == {"sequence": 1, "status": "alive"}


def test_process_raw_line_dispatches_fresh_request_type():
    d = dispatcher.Dispatcher(self_ref=MASTER_REF)
    d.register_handler("heartbeat", lambda env: ("heartbeat_ack", {"status": "alive"}))
    pending = dispatcher.PendingResponses()

    message = envelope.new_request_envelope(
        type="heartbeat", source=WORKER_REF, destination=MASTER_REF, payload={"sequence": 1}
    )
    raw_line = json.dumps(message).encode("utf-8")

    response = dispatcher.process_raw_line(raw_line, dispatcher=d, pending_responses=pending)
    assert response["type"] == "heartbeat_ack"
