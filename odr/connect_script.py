#!/usr/bin/env python
# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:

# odr-ovpn-connect -- Called by the OpenVPN client-connect hook.
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
from odr import fdsend

SCRIPT_NAME = 'odr-ovpn-connect'
CMD_SOCKET = '/var/run/odr/cmd.sock'


def main():
    #
    # Gather configuration
    #

    cfg_f = open(os.environ['client_connect_config_file'], 'w')
    ret_f = open(os.environ['client_connect_deferred_file'], 'w')
    full_username = os.environ['username']
    daemon_name = ovpn.determine_daemon_name(script_name=SCRIPT_NAME)

    #
    # Build and submit command
    #

    params = {
        'full_username': full_username,
        'ret_file_idx': '0',
        'config_file_idx': '1',
    }
    if daemon_name is not None:
        params['daemon_name'] = daemon_name

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(CMD_SOCKET)

    ovpn.write_deferred_ret_file(ret_f, ovpn.CC_RET_DEFERRED)
    try:
        result = {"cmd": "request"}
        result.update(params)
        fdsend.send_fds(
            sock,
            [json.dumps(result).encode("utf-8")],
            fds=[ret_f.fileno(), cfg_f.fileno()],
        )

        data = sock.recv(1024)
        if not data:
            raise RuntimeError('no response from odr')

        status = json.loads(data.decode("utf-8"))["cmd"]
        if status != 'OK':
            raise RuntimeError('starting dhcp request failed (ret: "%s")' % data)
    except Exception:
        ovpn.write_deferred_ret_file(ret_f, ovpn.CC_RET_FAILED)
        raise


if __name__ == '__main__':
    main()
