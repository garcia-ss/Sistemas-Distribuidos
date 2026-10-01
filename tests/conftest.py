import socket

import pytest


@pytest.fixture
def socket_pair():
    left, right = socket.socketpair()
    try:
        yield left, right
    finally:
        left.close()
        right.close()
