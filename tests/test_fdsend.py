from socket import socketpair, AF_UNIX
from odr.fdsend import send_fds, recv_fds

def test_send_fd(tmp_path):
    fd = open(tmp_path / "secret_file", "w").fileno()

    in_sock, out_sock = socketpair(AF_UNIX)

    send_fds(in_sock, [b"test123"], [fd])
    x = recv_fds(out_sock, 1024, 10)
    print(x)
