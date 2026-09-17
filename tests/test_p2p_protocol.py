import socket
import unittest

from p2p_protocol import (
    build_message,
    recv_msgs,
    send_msg,
    update_worker_registry,
    validate_message,
)


class P2PProtocolTests(unittest.TestCase):
    def test_send_and_recv_ndjson_fragmented_message(self):
        left, right = socket.socketpair()
        try:
            message = build_message(
                "ping",
                {"payload": "x" * 2000},
                {"uuid": "11111111-1111-4111-8111-111111111111", "label": "worker-a"},
                request_id="req-frag",
            )
            left.sendall((__import__('json').dumps(message, separators=(",", ":")) + "\n").encode("utf-8")[:10])
            left.sendall((__import__('json').dumps(message, separators=(",", ":")) + "\n").encode("utf-8")[10:])
            received, _ = recv_msgs(right, bytearray())
            self.assertEqual(len(received), 1)
            self.assertEqual(received[0]["payload"]["payload"], message["payload"]["payload"])
        finally:
            left.close()
            right.close()

    def test_coalesced_messages_are_delivered_separately(self):
        left, right = socket.socketpair()
        try:
            payloads = [
                build_message("ping", {"n": 1}, {"uuid": "22222222-2222-4222-8222-222222222222", "label": "worker-b"}, request_id="req-1"),
                build_message("ping", {"n": 2}, {"uuid": "22222222-2222-4222-8222-222222222222", "label": "worker-b"}, request_id="req-2"),
                build_message("ping", {"n": 3}, {"uuid": "22222222-2222-4222-8222-222222222222", "label": "worker-b"}, request_id="req-3"),
            ]
            frame = b"".join((__import__('json').dumps(item, separators=(",", ":")) + "\n").encode("utf-8") for item in payloads)
            left.sendall(frame)
            received, _ = recv_msgs(right, bytearray())
            self.assertEqual(len(received), 3)
            self.assertEqual([item["payload"]["n"] for item in received], [1, 2, 3])
        finally:
            left.close()
            right.close()

    def test_invalid_json_is_ignored(self):
        left, right = socket.socketpair()
        try:
            left.sendall(b'{"bad": "json"\n')
            received, _ = recv_msgs(right, bytearray())
            self.assertEqual(received, [])
        finally:
            left.close()
            right.close()

    def test_duplicate_uuid_updates_existing_worker_registration(self):
        workers = {}
        first = build_message(
            "register_worker",
            {"host": "127.0.0.1", "port": 7001, "label": "worker-1"},
            {"uuid": "33333333-3333-4333-8333-333333333333", "label": "worker-1"},
            request_id="req-reg-1",
        )
        second = build_message(
            "register_worker",
            {"host": "127.0.0.1", "port": 7002, "label": "worker-1"},
            {"uuid": "33333333-3333-4333-8333-333333333333", "label": "worker-1"},
            request_id="req-reg-2",
        )
        update_worker_registry(workers, first)
        update_worker_registry(workers, second)
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers["33333333-3333-4333-8333-333333333333"]["port"], 7002)

    def test_missing_required_fields_are_rejected(self):
        invalid = {"type": "register_worker", "payload": {}}
        self.assertFalse(validate_message(invalid))


if __name__ == "__main__":
    unittest.main()
