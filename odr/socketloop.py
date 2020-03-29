# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:

# socketloop.py - Provides a socket/select-based event loop.
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

import asyncio
import select
import logging


class SocketLoop:
    """This used to be a custom event loop. I have replaced it with asyncio"""

    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self.log = logging.getLogger('socketloop')

    @property
    def aio_loop(self) -> asyncio.AbstractEventLoop:
        """get the asyncio event loop"""
        return self._loop

    def run(self):
        """Runs the socket select loop until the quit method is called.  Calls
        the idle handlers after each loop cycle.
        """
        try:
            self._loop.run_forever()
        finally:
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    def add_socket_handler(self, socket_handler):
        """Add an additional socket handler.
        @param socket_handler: The socket handler instance to add.
        """
        self.log.debug(
            'adding socket_handler for socket %d', socket_handler.socket.fileno()
        )
        self._loop.add_reader(socket_handler.socket, socket_handler.handle_socket)

    def del_socket_handler(self, socket_handler):
        """Remove a previously added socket handler.
        @param socket_handler: The socket handler instance to remove.
        """
        self.log.debug(
            'removing socket_handler for socket %d', socket_handler.socket.fileno()
        )
        self._loop.remove_reader(socket_handler.socket)

    def quit(self):
        """Request that the select loop be exited soon.  Sets a flag that will
        be checked for in the select loop.
        """
        self._loop.stop()
