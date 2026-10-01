import pytest

from common import envelope, naming
from common.identity import Identity


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


def _valid_message(**overrides) -> dict:
    message = envelope.new_request_envelope(
        type="heartbeat",
        source=WORKER_REF,
        destination=MASTER_REF,
        payload={"sequence": 1, "assignment_epoch": 0},
    )
    message.update(overrides)
    return message


# -- construcao ---------------------------------------------------------


def test_node_ref_from_identity_matches_fields():
    identity = Identity(
        node_id="a7a1abe4-afdd-4d3d-96b8-d81b057ca325",
        node_label="grupo_01_master_01_worker_01",
        node_role=naming.ROLE_WORKER,
        group_label="grupo_01",
    )
    ref = envelope.node_ref_from_identity(identity)
    assert ref == WORKER_REF


def test_new_request_envelope_has_fresh_ids_and_fixed_version():
    message = envelope.new_request_envelope(
        type="heartbeat", source=WORKER_REF, destination=MASTER_REF, payload={"sequence": 1}
    )
    assert message["version"] == envelope.PROTOCOL_VERSION
    assert envelope.is_uuid4_string(message["message_id"])
    assert envelope.is_uuid4_string(message["request_id"])
    assert message["source"] == WORKER_REF.to_dict()
    assert message["destination"] == MASTER_REF.to_dict()


def test_response_envelope_preserves_request_id_new_message_id():
    request = envelope.new_request_envelope(
        type="register_worker", source=WORKER_REF, destination=MASTER_REF, payload={}
    )
    response = envelope.response_envelope(
        type="registration_ack",
        source=MASTER_REF,
        destination=WORKER_REF,
        payload={"accepted": True, "assignment_epoch": 0, "reason": None},
        request_id=request["request_id"],
    )
    assert response["request_id"] == request["request_id"]
    assert response["message_id"] != request["message_id"]


# -- parse_envelope: caminho feliz ---------------------------------------


def test_parse_envelope_roundtrip():
    message = _valid_message()
    parsed = envelope.parse_envelope(message)
    assert parsed.type == "heartbeat"
    assert parsed.source == WORKER_REF
    assert parsed.destination == MASTER_REF
    assert parsed.payload == {"sequence": 1, "assignment_epoch": 0}


# -- parse_envelope: rejeicoes --------------------------------------------


def test_parse_envelope_rejects_missing_field():
    message = _valid_message()
    del message["request_id"]
    with pytest.raises(envelope.EnvelopeError) as exc_info:
        envelope.parse_envelope(message)
    assert exc_info.value.error_code == "invalid_message"


def test_parse_envelope_rejects_unknown_version():
    message = _valid_message(version="sd_2025_1_v1")
    with pytest.raises(envelope.EnvelopeError) as exc_info:
        envelope.parse_envelope(message)
    assert exc_info.value.error_code == "unsupported_version"


@pytest.mark.parametrize("bad_id", ["not-a-uuid", "10d07f9e-ab14-4ee7-8b4f", "", 123])
def test_parse_envelope_rejects_invalid_message_id(bad_id):
    message = _valid_message(message_id=bad_id)
    with pytest.raises(envelope.EnvelopeError) as exc_info:
        envelope.parse_envelope(message)
    assert exc_info.value.error_code == "invalid_message"


def test_parse_envelope_rejects_negative_timestamp():
    message = _valid_message(timestamp_ms=-1)
    with pytest.raises(envelope.EnvelopeError):
        envelope.parse_envelope(message)


def test_parse_envelope_rejects_non_dict_payload():
    message = _valid_message(payload="not-a-dict")
    with pytest.raises(envelope.EnvelopeError):
        envelope.parse_envelope(message)


def test_parse_envelope_rejects_node_label_role_mismatch():
    bad_source = WORKER_REF.to_dict()
    bad_source["node_role"] = naming.ROLE_MASTER  # label diz worker, role diz master
    message = _valid_message(source=bad_source)
    with pytest.raises(envelope.EnvelopeError) as exc_info:
        envelope.parse_envelope(message)
    assert exc_info.value.error_code == "invalid_message"


def test_parse_envelope_rejects_node_label_group_mismatch():
    bad_source = WORKER_REF.to_dict()
    bad_source["group_label"] = "grupo_02"  # label embute grupo_01
    message = _valid_message(source=bad_source)
    with pytest.raises(envelope.EnvelopeError):
        envelope.parse_envelope(message)


def test_parse_envelope_rejects_unknown_node_role():
    bad_destination = MASTER_REF.to_dict()
    bad_destination["node_role"] = "supervisor"
    message = _valid_message(destination=bad_destination)
    with pytest.raises(envelope.EnvelopeError):
        envelope.parse_envelope(message)
