import socket
from pathlib import Path

from p2p_protocol import (
    build_registration_ack,
    ensure_config,
    log_event,
    recv_msgs,
    send_msg,
    update_worker_registry,
)


def main():
    config = ensure_config(
        Path(__file__).with_name("master_config.json"),
        {"uuid": None, "label": "master", "host": "127.0.0.1", "port": 5001},
    )

    workers = {}
    listen_host = config["host"]
    listen_port = int(config["port"])

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((listen_host, listen_port))
    server.listen(5)
    server.settimeout(1.0)

    print(f"Master listening on {listen_host}:{listen_port} (uuid={config['uuid']}, label={config['label']})")

    while True:
        try:
            conn, addr = server.accept()
        except socket.timeout:
            continue

        with conn:
            buffer = bytearray()
            conn.settimeout(1.0)
            while True:
                try:
                    messages, buffer = recv_msgs(conn, buffer)
                except ConnectionResetError:
                    break
                except OSError:
                    break

                if not messages:
                    continue

                for message in messages:
                    event_name = message.get("type", "unknown")
                    origin = message.get("origin", {})
                    worker_label = origin.get("label", "unknown")
                    request_id = message.get("request_id", "n/a")

                    if event_name == "register_worker":
                        updated = update_worker_registry(workers, message)
                        if updated:
                            ack = build_registration_ack(
                                {"uuid": config["uuid"], "label": config["label"]},
                                message,
                                len(workers),
                            )
                            send_msg(conn, ack)
                            log_event("p2p", worker_label, config["label"], event_name, request_id, "registered")
                            log_event("p2p", config["label"], worker_label, "registration_ack", ack["request_id"], "ok")
                        else:
                            log_event("p2p", worker_label, config["label"], event_name, request_id, "rejected")
                    else:
                        log_event("p2p", worker_label, config["label"], event_name, request_id, "ignored")


if __name__ == "__main__":
    main()
