import socket
import time

from common import dispatcher, envelope, heartbeat, naming, transport

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


def _recv_one(conn, timeout_s=1.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            lines = conn.recv_lines()
        except socket.timeout:
            continue
        if lines:
            return lines[0]
    raise AssertionError("nenhuma linha recebida dentro do timeout")


def test_heartbeat_sender_completes_cycle_on_timely_ack(socket_pair):
    left, right = socket_pair
    left.settimeout(0.05)
    right.settimeout(0.05)
    conn_left = transport.NdjsonConnection(left)
    conn_right = transport.NdjsonConnection(right)
    pending = dispatcher.PendingResponses()

    sender = heartbeat.HeartbeatSender(
        self_ref=WORKER_REF, peer_ref=MASTER_REF, connection=conn_left, pending=pending,
        interval_s=0.0, response_timeout_s=0.5, max_missed=3,
    )
    sender.tick()  # interval_s=0: dispara o envio imediatamente

    request_env = envelope.parse_envelope(transport.decode_json_line(_recv_one(conn_right)))
    assert request_env.type == "heartbeat"
    assert request_env.payload["sequence"] == 1

    ack_message = envelope.response_envelope(
        type="heartbeat_ack", source=MASTER_REF, destination=WORKER_REF,
        payload={"sequence": request_env.payload["sequence"], "status": "alive"},
        request_id=request_env.request_id,
    )
    conn_right.send_message(ack_message)

    raw_ack = _recv_one(conn_left)
    d = dispatcher.Dispatcher(self_ref=WORKER_REF)
    result = dispatcher.process_raw_line(raw_ack, dispatcher=d, pending_responses=pending)
    assert result is None  # foi entregue ao waiter, nao caiu no dispatch

    sender.tick()
    assert sender.missed_count == 0
    assert sender.suspect is False


def test_heartbeat_sender_marks_suspect_after_max_missed_without_ack(socket_pair):
    left, _right = socket_pair
    conn_left = transport.NdjsonConnection(left)
    pending = dispatcher.PendingResponses()

    sender = heartbeat.HeartbeatSender(
        self_ref=WORKER_REF, peer_ref=MASTER_REF, connection=conn_left, pending=pending,
        interval_s=0.0, response_timeout_s=0.02, max_missed=3,
    )
    sender.tick()  # ninguem no outro lado responde: todo ciclo estoura o timeout

    deadline = time.monotonic() + 2.0
    while not sender.suspect and time.monotonic() < deadline:
        time.sleep(0.03)
        sender.tick()

    assert sender.suspect is True
    assert sender.missed_count == 3
