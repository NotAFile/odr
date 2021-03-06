#!/usr/bin/env python
# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:

# odr-ovpn-disconnect -- Called by the OpenVPN client-disconnect hook.
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

import os
import socket
import json
import odr.ovpn as ovpn

SCRIPT_NAME = 'odr-ovpn-disconnect'
CMD_SOCKET = '/var/run/odr/cmd.sock'


def main():
    #
    # Gather configuration
    #

    full_username = os.environ['username']
    daemon_name = ovpn.determine_daemon_name(script_name=SCRIPT_NAME)

    #
    # Build and submit command
    #

    params = {'full_username': full_username}
    if daemon_name is not None:
        params['daemon_name'] = daemon_name

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(CMD_SOCKET)
    result = {"cmd": "disconnect"}
    result.update(params)
    sock.send(json.dumps(result).encode("utf-8"))
    ret = sock.recv(1024)
    status = json.loads(ret.decode("utf-8"))["cmd"]
    if status != 'OK':
        raise RuntimeError('sending disconnect notification failed (ret: "%s")' % ret)


if __name__ == '__main__':
    main()
