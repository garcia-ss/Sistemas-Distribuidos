import json

import pytest

from common import transport


# -- LineBuffer: enquadramento puro, sem socket -----------------------------


def test_feed_returns_nothing_for_incomplete_line():
    buf = transport.LineBuffer()
    assert buf.feed(b'{"a": 1}') == []  # sem \n ainda: mensagem incompleta


def test_feed_reassembles_message_fragmented_across_multiple_recv():
    buf = transport.LineBuffer()
    assert buf.feed(b'{"a"') == []
    assert buf.feed(b': 1}') == []
    assert buf.feed(b'\n') == [b'{"a": 1}']


def test_feed_splits_multiple_messages_from_a_single_recv():
    buf = transport.LineBuffer()
    chunk = b'{"a": 1}\n{"b": 2}\n{"c":'
    lines = buf.feed(chunk)
    assert lines == [b'{"a": 1}', b'{"b": 2}']
    # o fragmento final (sem \n) fica retido para a proxima leitura
    assert buf.feed(b' 3}\n') == [b'{"c": 3}']


def test_feed_raises_when_buffer_exceeds_limit_without_delimiter():
    buf = transport.LineBuffer(max_message_bytes=16)
    with pytest.raises(transport.MessageTooLargeError):
        buf.feed(b'x' * 17)  # sem \n: nao ha como saber onde a mensagem termina


def test_feed_does_not_raise_for_a_large_but_complete_line():
    buf = transport.LineBuffer(max_message_bytes=16)
    # a linha em si excede o limite, mas tem \n: enquadramento nao falha aqui.
    # o tamanho do CONTEUDO e responsabilidade de decode_json_line.
    lines = buf.feed(b'x' * 20 + b'\n')
    assert lines == [b'x' * 20]


# -- decode_json_line: validacao de conteudo, sem socket ---------------------


def test_decode_json_line_accepts_valid_envelope_shaped_object():
    raw = json.dumps({"type": "heartbeat", "payload": {"sequence": 1}}).encode("utf-8")
    assert transport.decode_json_line(raw) == {
        "type": "heartbeat",
        "payload": {"sequence": 1},
    }


def test_decode_json_line_rejects_malformed_json():
    with pytest.raises(transport.MessageDecodeError) as exc_info:
        transport.decode_json_line(b'{"type": "heartbeat",}')  # virgula sobrando
    assert exc_info.value.error_code == "invalid_json"


def test_decode_json_line_rejects_duplicate_keys():
    raw = b'{"type": "heartbeat", "type": "protocol_error"}'
    with pytest.raises(transport.MessageDecodeError) as exc_info:
        transport.decode_json_line(raw)
    assert exc_info.value.error_code == "duplicate_key"


def test_decode_json_line_rejects_duplicate_keys_in_nested_object():
    raw = b'{"type": "heartbeat", "source": {"node_id": "a", "node_id": "b"}}'
    with pytest.raises(transport.MessageDecodeError) as exc_info:
        transport.decode_json_line(raw)
    assert exc_info.value.error_code == "duplicate_key"


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_decode_json_line_rejects_non_finite_numbers(constant):
    raw = f'{{"sequence": {constant}}}'.encode("utf-8")
    with pytest.raises(transport.MessageDecodeError) as exc_info:
        transport.decode_json_line(raw)
    assert exc_info.value.error_code == "non_finite_number"


def test_decode_json_line_rejects_top_level_non_object():
    with pytest.raises(transport.MessageDecodeError) as exc_info:
        transport.decode_json_line(b'[1, 2, 3]')
    assert exc_info.value.error_code == "invalid_envelope"


def test_decode_json_line_rejects_oversized_message():
    raw = json.dumps({"payload": "x" * 100}).encode("utf-8")
    with pytest.raises(transport.MessageDecodeError) as exc_info:
        transport.decode_json_line(raw, max_message_bytes=16)
    assert exc_info.value.error_code == "message_too_large"


# -- NdjsonConnection: socket real via socketpair -----------------------------


def test_ndjson_connection_roundtrip_over_real_socket(socket_pair):
    left, right = socket_pair
    conn_left = transport.NdjsonConnection(left)
    conn_right = transport.NdjsonConnection(right)

    conn_left.send_message({"type": "heartbeat", "payload": {"sequence": 1}})

    lines: list[bytes] = []
    while not lines:
        lines = conn_right.recv_lines()
    assert len(lines) == 1
    assert transport.decode_json_line(lines[0]) == {
        "type": "heartbeat",
        "payload": {"sequence": 1},
    }


def test_ndjson_connection_detects_peer_closing(socket_pair):
    left, right = socket_pair
    conn_left = transport.NdjsonConnection(left)
    conn_right = transport.NdjsonConnection(right)

    conn_left.close()
    with pytest.raises(transport.ConnectionClosed):
        conn_right.recv_lines()
