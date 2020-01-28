"""
Backport of the send_fds and recv_fds functions from python 3.9
"""

import socket
import array


def send_fds(sock, buffers, fds, flags=0, address=None):
    """ send_fds(sock, buffers, fds[, flags[, address]]) -> integer

    Send the list of file descriptors fds over an AF_UNIX socket.
    """
    return sock.sendmsg(
        buffers, [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fds))]
    )


def recv_fds(sock, bufsize, maxfds, flags=0):
    """ recv_fds(sock, bufsize, maxfds[, flags]) -> (data, list of file
    descriptors, msg_flags, address)

    Receive up to maxfds file descriptors returning the message
    data and a list containing the descriptors.
    """
    # Array of ints
    fds = array.array("i")
    msg, ancdata, flags, addr = sock.recvmsg(
        bufsize, socket.CMSG_LEN(maxfds * fds.itemsize)
    )
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            fds.frombytes(cmsg_data[: len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])

    return msg, list(fds), flags, addr
