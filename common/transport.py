"""Framing NDJSON sobre TCP: bytes crus -> linhas completas -> dict Python.

Modulo comum (usado por master, worker, client e collector). Nao conhece o
formato do envelope da aplicacao -- isso e responsabilidade de
common/envelope.py. Aqui a unica pergunta e "onde uma mensagem termina
dentro de um fluxo de bytes", nao "o que essa mensagem significa".

Dividido em duas partes independentes, cada uma testavel sem socket real:
  - LineBuffer: acumula bytes e extrai linhas completas delimitadas por LF.
  - decode_json_line: decodifica uma linha (bytes) em dict, validando as
    regras do protocolo (JSON valido, sem chaves duplicadas, sem NaN/
    Infinity, tamanho dentro do limite).

NdjsonConnection embrulha essas duas partes em torno de um socket real:
um leitor (recv_lines) e escrita serializada por lock (send_message), como
exige a regra "um socket = um unico leitor e escrita serializada".
"""

from __future__ import annotations

import json
import socket
import threading

from common.errors import ProtocolError

MAX_MESSAGE_BYTES = 65536
_RECV_CHUNK_BYTES = 4096


class FramingError(Exception):
    """Erro de enquadramento que exige fechar a conexao (nao e recuperavel)."""


class MessageTooLargeError(FramingError):
    """Buffer excedeu max_message_bytes sem que um delimitador \\n aparecesse."""


class ConnectionClosed(Exception):
    """O peer fechou a conexao (recv() retornou 0 bytes)."""


class MessageDecodeError(ProtocolError):
    """Uma linha completa (delimitada por \\n) nao e uma mensagem valida.

    Diferente de MessageTooLargeError: aqui ja sabemos onde a mensagem
    termina, entao o erro e de conteudo (poderia virar protocol_error, mas
    nesse ponto nao ha ainda source/request_id confiaveis para correlacionar
    -- o dispatcher so loga e descarta a linha), nao motivo para derrubar a
    conexao.
    """


class LineBuffer:
    """Acumula bytes recebidos e extrai linhas completas delimitadas por LF.

    TCP e um fluxo de bytes: um recv() pode trazer meia mensagem ou varias
    mensagens grudadas. feed() nunca assume "um chunk = uma mensagem": pode
    devolver zero, uma ou varias linhas para um unico chunk.
    """

    def __init__(self, max_message_bytes: int = MAX_MESSAGE_BYTES):
        self._max_message_bytes = max_message_bytes
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> list[bytes]:
        self._buffer.extend(chunk)

        lines: list[bytes] = []
        while True:
            newline_index = self._buffer.find(b"\n")
            if newline_index == -1:
                break
            lines.append(bytes(self._buffer[:newline_index]))
            del self._buffer[: newline_index + 1]

        if len(self._buffer) > self._max_message_bytes:
            raise MessageTooLargeError(
                f"buffer excedeu max_message_bytes={self._max_message_bytes} "
                "sem encontrar delimitador LF"
            )
        return lines


def _reject_duplicate_keys(pairs: list[tuple]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise MessageDecodeError(
                "duplicate_key", f"chave duplicada no objeto JSON: {key!r}"
            )
        result[key] = value
    return result


def _reject_non_finite_constant(constant: str) -> None:
    raise MessageDecodeError(
        "non_finite_number", f"constante numerica nao permitida pelo protocolo: {constant}"
    )


def decode_json_line(raw: bytes, *, max_message_bytes: int = MAX_MESSAGE_BYTES) -> dict:
    """Decodifica uma linha (sem o LF) em dict, ou levanta MessageDecodeError."""
    if len(raw) > max_message_bytes:
        raise MessageDecodeError(
            "message_too_large",
            f"mensagem com {len(raw)} bytes excede max_message_bytes={max_message_bytes}",
        )

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MessageDecodeError("invalid_utf8", str(exc)) from exc

    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite_constant,
        )
    except json.JSONDecodeError as exc:
        raise MessageDecodeError("invalid_json", str(exc)) from exc

    if not isinstance(parsed, dict):
        raise MessageDecodeError(
            "invalid_envelope", "mensagem NDJSON deve ser um objeto JSON no nivel superior"
        )
    return parsed


class NdjsonConnection:
    """Um socket TCP com framing NDJSON: um leitor, escrita serializada por lock."""

    def __init__(self, sock: socket.socket, *, max_message_bytes: int = MAX_MESSAGE_BYTES):
        self._sock = sock
        self._line_buffer = LineBuffer(max_message_bytes)
        self._write_lock = threading.Lock()

    def recv_lines(self) -> list[bytes]:
        """Um recv() + framing. Pode devolver [] (chunk sem linha completa ainda)."""
        chunk = self._sock.recv(_RECV_CHUNK_BYTES)
        if not chunk:
            raise ConnectionClosed("peer fechou a conexao (recv retornou 0 bytes)")
        return self._line_buffer.feed(chunk)

    def send_message(self, message: dict) -> None:
        payload = json.dumps(message, allow_nan=False, separators=(",", ":")) + "\n"
        data = payload.encode("utf-8")
        with self._write_lock:
            self._sock.sendall(data)

    def close(self) -> None:
        self._sock.close()
