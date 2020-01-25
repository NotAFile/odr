# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:

# listeningsocket.py -- Manage a listening socket.
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

import errno
import socket

# reimport to prevent shadowing
from socket import socket as SocketType


class SocketLocalAddressBindFailed(OSError):
    """For some reason, the requested local address / port combination could not
    be bound to.
    """

class SocketLocalAddressNotAvailable(SocketLocalAddressBindFailed):
    """The requested local address / port combination was not available.
    """


class ListeningSocket(object):
    """A ListeningSocket represents a UDP socket listening for packets
    on a specific IP address and port on a specific network device, if
    desired.
    """

    def __init__(self, listen_address: str, listen_port: int, listen_device: str = None) -> None:
        """\
        @param listen_address: IP address as string to listen on.
        @param listen_port: Local DHCP listening port. Defaults to 67.
        @param listen_device: Device name to bind to.
        """
        self.listen_address, self.listen_port = listen_address, listen_port
        self.listen_device = listen_device

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if self.listen_device is not None:
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE,
                    self.listen_device.encode("utf-8") + b'\0')

        try:
            self._socket.bind((self.listen_address, self.listen_port))
        except OSError as ex:
            if ex.errno == errno.EADDRNOTAVAIL:
                raise SocketLocalAddressNotAvailable(
                        self.listen_address, self.listen_port,
                        self.listen_device)
            else:
                raise SocketLocalAddressBindFailed(
                        self.listen_address, self.listen_port,
                        self.listen_device)

    @property
    def socket(self) -> SocketType:
        """@return: Returns the listening socket.
        """
        return self._socket

