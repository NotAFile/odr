# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:

# cmdconnection.py -- Module for simple line-based client-server communication
#         via UNIX domain sockets.
#
# Copyright © 2010 Fabian Knittel <fabian.knittel@lettink.de>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
import socket
import os
import struct
import json
from . import fdsend


class CommandConnection:
    """Represents the connection to a single client.  The class should be used
    as a base-class.  Sub-classes will implement the stub methods to provide the
    actual functionality.

    The communication is message based.  One command per message.  Optionally,
    the command can transfer up to 8 writable file descriptors.
    """

    MAX_NUM_FDS = 8
    MAX_MSG_SIZE = 1024

    def __init__(self, sloop, sock, log, **kwargs):
        """\
        :ivar sloop: Instance of the socket loop.
        :ivar sock: Socket that will be used for communication.
        """
        self._sloop = sloop
        self._socket = sock
        self._log = log
        self._sloop.add_socket_handler(self)

    def __del__(self):
        self._log.debug('destructing CommandConnection')
        self._socket.close()

    @property
    def socket(self):
        """:returns: the underlying socket.
        """
        return self._socket

    def _parse_command(self, cmd_line, files):
        """Splits the command-line and hands off the parsed data to the
        child class' handle_cmd method.
        :ivar cmd_line: The command line string to parse.
        """
        self._log.debug('parsing command "%s"' % repr(cmd_line))
        try:
            data = json.loads(cmd_line.decode("utf-8"))
        except json.JSONDecodeError:
            self._log.warning('failed to parse command "%s"' % repr(cmd_line))
            return
        self.handle_cmd(data["cmd"], data, files)

    def handle_socket(self):
        """Part of the interface expected by the socket loop.  Should be called
        as soon as the socket has data waiting to be read.  The method will
        process any pending data and parse the commands of full received
        command lines.

        In case of EOF, the socket will be removed from the socket loop and this
        instance will get destroyed.
        """
        cmd_line, fds, _, _ = fdsend.recv_fds(
            self._socket, self.MAX_MSG_SIZE, maxfds=self.MAX_NUM_FDS
        )

        # By wrapping the files in objects, they will be implicitly closed
        # (assuming a reference counted Python).
        files = [os.fdopen(fd, 'w') for fd in fds]

        if cmd_line != b'':
            self._parse_command(cmd_line, files=files)
        else:
            self._log.debug('closing cmd socket due to EOF')
            self._sloop.del_socket_handler(self)

    def send_cmd(self, cmd, params={}):
        """Used to send responses back to the client.  Sends the specified
        command as a single message.

        :ivar cmd: Command to send. Should not contain a new-line or a zero
                character.
        :ivar params: Parameters to send. (Optional.)
        """
        result = {"cmd": cmd}
        result.update(params)
        self._socket.send(json.dumps(result).encode("utf-8"))

    def handle_command(self, cmd):
        """The handle_command function is called as soon as a command was
        received and parsed by CommandConnection.  A sub-class should implement
        the stub function and do the actual command processing.
        """

    def handle_cmd(self, cmd, params, files):
        """Called for each command received over the command socket.  Forwards
        the command processing according to the command's name.

        @param cmd: Name of the command.
        @param params: Dictionary of the command's parameters.
        @param files: Array of file pointers passed through with the command.
        """
        raise NotImplementedError('Method handle_cmd not implemented')


class CommandConnectionListener:
    """Listens on a POSIX Local IPC Socket (AKA Unix domain socket) and uses a
    factory function to create an instance that takes care of each new socket
    connection.
    """

    ACCEPT_QUEUE_LEN = 32

    def __init__(
        self,
        sloop,
        cmd_conn_factory,
        socket_path,
        socket_perm_mode=0o666,
        auth_check=None,
    ):
        """Opens the POSIX Local IPC Socket.  If the file already exists, it
        is deleted first.  The file permissions are set according to the
        socket_perm_mode parameter.

        :ivar sloop: Instance of the socket loop.
        :ivar cmd_conn_factory: Factory method that gets called with the
                socket loop instance and the new socket for each new connection.
        :ivar socket_path: Path to the POSIX Local IPC Socket.
        :ivar socket_perm_mode: File access permissions to be set for the
                file socket.
        """
        self._sloop = sloop
        self._socket_path = socket_path
        self._factory = cmd_conn_factory
        self._auth_check = auth_check

        self._log = logging.getLogger('cmdconnlistener')
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.setblocking(False)
        if os.path.exists(self._socket_path):
            os.remove(self._socket_path)
        self._log.debug('listening on socket %s' % self._socket_path)
        self._socket.bind(self._socket_path)
        os.chmod(self._socket_path, socket_perm_mode)
        self._socket.listen(self.ACCEPT_QUEUE_LEN)

    def __del__(self):
        self._socket.close()

    @property
    def socket(self):
        """:returns: the listening socket.
        """
        return self._socket

    def handle_socket(self):
        """Part of the interface expected by the socket loop.  Should be called
        as soon as the socket has a new connection waiting.  Uses the factory
        method passed in at creation time to create a new handler instance for
        each socket.
        """
        try:
            sock, _ = self._socket.accept()
        except OSError as e:
            print("Received exception %s while accepting new cmd conn" % repr(e))
            return
        self._log.debug('received a new connection')
        sock.setblocking(False)
        if self._auth_check is not None:
            pid, uid, gid = getsockpeercred(sock)
            if not self._auth_check(sock=sock, pid=pid, uid=uid, gid=gid):
                self._log.info(
                    'rejecting command connection to %s from '
                    'PID %d (UID %d, GID %d)',
                    self._socket_path,
                    pid,
                    uid,
                    gid,
                )
                sock.close()
                return
        conn = self._factory(sloop=self._sloop, sock=sock)


def getsockpeercred(sock):
    """Retrieves the credentials of a peer which is connected via a AF_UNIX
    socket.

    :ivar sock: The socket connection.
    :returns: a triple with the peer's PID, UID and GID.
    """
    # SO_PEERCRED returns a struct ucred.  The three struct members pid_t, uid_t
    # and gid_t are defined as "int" on Linux systems, so this should be
    # portable across Linux architectures.
    return struct.unpack(
        '3i',
        sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize('3i')),
    )
