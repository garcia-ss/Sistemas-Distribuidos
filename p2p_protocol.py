import json
import socket
import uuid
from datetime import datetime, timezone
from pathlib import Path

REQUIRED_MESSAGE_FIELDS = (
    "type",
    "msg_id",
    "request_id",
    "origin",
    "timestamp",
    "payload",
)


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(group, origin, destination, event_type, request_id, result):
    origin_value = origin.get("label") if isinstance(origin, dict) else str(origin)
    destination_value = destination.get("label") if isinstance(destination, dict) else str(destination)
    print(f"{group} | {origin_value} | {destination_value} | {event_type} | {request_id} | {result}")


def _valid_uuid(value):
    try:
        uuid.UUID(str(value))
        return True
    except (TypeError, ValueError, AttributeError):
        return False


def validate_origin(origin):
    if not isinstance(origin, dict):
        return False
    if "uuid" not in origin or "label" not in origin:
        return False
    return bool(origin.get("uuid")) and bool(origin.get("label")) and _valid_uuid(origin["uuid"])


def validate_message(message):
    if not isinstance(message, dict):
        return False
    for field in REQUIRED_MESSAGE_FIELDS:
        if field not in message:
            return False
    if not isinstance(message["type"], str) or not message["type"].strip():
        return False
    if not isinstance(message["msg_id"], (str, int)):
        return False
    if not isinstance(message["request_id"], (str, int)):
        return False
    if not validate_origin(message["origin"]):
        return False
    if not isinstance(message["timestamp"], str) or not message["timestamp"].strip():
        return False
    if "payload" not in message:
        return False
    return True


def build_message(message_type, payload, origin, request_id=None, msg_id=None, timestamp=None):
    if not validate_origin(origin):
        raise ValueError("origin must include a valid uuid and label")

    return {
        "type": message_type,
        "msg_id": str(msg_id) if msg_id is not None else str(uuid.uuid4()),
        "request_id": str(request_id) if request_id is not None else str(uuid.uuid4()),
        "origin": {"uuid": str(origin["uuid"]), "label": str(origin["label"])},
        "timestamp": timestamp or now_iso_utc(),
        "payload": payload,
    }


def send_msg(sock, message):
    payload = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
    sock.sendall((payload + "\n").encode("utf-8"))


def recv_msgs(sock, buffer):
    data = bytearray(buffer)
    try:
        chunk = sock.recv(4096)
    except socket.timeout:
        return [], data
    except OSError:
        return [], data

    if chunk:
        data.extend(chunk)

    messages = []
    while True:
        newline_index = data.find(b"\n")
        if newline_index == -1:
            break

        raw_line = bytes(data[:newline_index])
        del data[: newline_index + 1]

        if not raw_line.strip():
            continue

        try:
            decoded = raw_line.decode("utf-8")
            message = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            log_event("p2p", "socket", "socket", "invalid_json", "n/a", "discarded")
            continue

        if not validate_message(message):
            log_event("p2p", "socket", "socket", "invalid_message", "n/a", "discarded")
            continue

        messages.append(message)

    return messages, bytes(data)


def ensure_config(path, defaults):
    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        try:
            with config_path.open("r", encoding="utf-8") as handle:
                config = json.load(handle)
        except json.JSONDecodeError:
            config = {}
    else:
        config = {}

    if not isinstance(config, dict):
        config = {}

    updated = dict(defaults)
    updated.update(config)
    updated["uuid"] = str(updated.get("uuid") or uuid.uuid4())
    if not updated.get("label"):
        updated["label"] = defaults.get("label", "worker")
    if not updated.get("host"):
        updated["host"] = defaults.get("host", "127.0.0.1")
    if not updated.get("port"):
        updated["port"] = defaults.get("port", 5001)

    with config_path.open("w", encoding="utf-8") as handle:
        json.dump(updated, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return updated


def update_worker_registry(workers, message):
    if not validate_message(message):
        return False
    if message["type"] != "register_worker":
        return False

    origin = message["origin"]
    uuid_value = origin["uuid"]
    payload = message.get("payload") or {}

    if not isinstance(payload, dict):
        return False

    entry = {
        "uuid": uuid_value,
        "label": origin["label"],
        "host": payload.get("host", "127.0.0.1"),
        "port": payload.get("port", 5001),
        "last_seen": message["timestamp"],
        "request_id": message["request_id"],
    }
    workers[uuid_value] = entry
    return True


def build_registration_ack(master_origin, worker_message, worker_count):
    return build_message(
        "registration_ack",
        {
            "status": "ok",
            "uuid": worker_message["origin"]["uuid"],
            "label": worker_message["origin"]["label"],
            "worker_count": worker_count,
        },
        master_origin,
        request_id=worker_message["request_id"],
    )
