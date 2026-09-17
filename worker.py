import socket
from pathlib import Path

from p2p_protocol import (
    build_message,
    ensure_config,
    log_event,
    recv_msgs,
    send_msg,
)


def main():
    config = ensure_config(
        Path(__file__).with_name("worker_config.json"),
        {"uuid": None, "label": "worker-1", "host": "127.0.0.1", "port": 5001},
    )

    master_host = config["host"]
    master_port = int(config["port"])

    with socket.create_connection((master_host, master_port), timeout=5) as sock:
        sock.settimeout(1.0)
        registration = build_message(
            "register_worker",
            {"host": master_host, "port": master_port, "label": config["label"]},
            {"uuid": config["uuid"], "label": config["label"]},
            request_id=f"register-{config['uuid']}",
        )
        send_msg(sock, registration)
        log_event("p2p", config["label"], "master", "register_worker", registration["request_id"], "sent")

        buffer = bytearray()
        while True:
            try:
                messages, buffer = recv_msgs(sock, buffer)
            except (socket.timeout, OSError):
                continue

            for message in messages:
                if message.get("type") == "registration_ack":
                    log_event("p2p", config["label"], "master", "registration_ack", message["request_id"], "accepted")
                    return
                log_event("p2p", config["label"], "master", message.get("type", "unknown"), message.get("request_id", "n/a"), "ignored")


if __name__ == "__main__":
    main()
