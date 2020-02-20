#!/usr/bin/env python3

# odrd -- OpenVPN DHCP Requestor daemon
#
# Copyright © 2010 Fabian Knittel <fabian.knittel@avona.com>
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

import sys
import logging
import weakref
import re
import os
import pwd
import grp
import time
import signal
import prctl
import socket
import random
from optparse import OptionParser
from configparser import ConfigParser
from functools import partial
import datetime
import hashlib
from ipaddress import IPv6Network, IPv4Network

from typing import Dict, Optional, Any, List, Tuple, Iterator, TextIO

import odr.ovpn as ovpn
from odr.cmdconnection import CommandConnection, CommandConnectionListener
from odr.timeoutmgr import TimeoutManager, TimeoutObject
from odr.socketloop import SocketLoop
import odr.dhcprequestor
import odr.listeningsocket
from odr.weakmethod import WeakBoundMethod


CONFIG_FILE = '/etc/odr.conf'


class ParseUsername:
    """Provides parsing of full usernames into their components.
    """

    USERNAME_RE = re.compile(
        r'^(?P<username>[^/@]+)(/(?P<resource>[^/@]+))?'
        r'(@((?P<domain>[^/@]+)/)?(?P<realm>[^/@]+))?$'
    )

    def __init__(self, default_realm: str) -> None:
        self._default_realm = default_realm
        self._log = logging.getLogger('parseusername')

    def parse_username(self, full_username: str) -> Optional[Dict[str, str]]:
        """Parse a full username into its components and apply any defaulting
        rules for the components.

        @param full_username: The full username to parse.
        @return: Returns a dictionary of the username components, consisting of
            "username", "resource", "domain" and "realm".
        """
        match = self.USERNAME_RE.match(full_username)
        if match is None:
            self._log.warning('username in unexpected format: "%s"', full_username)
            return None
        realm = match.group('realm')

        if realm is None:
            if self._default_realm is None:
                self._log.warning('username contains no realm: "%s"', full_username)
                return None
            self._log.debug(
                'no realm specified, using default realm "%s"', self._default_realm
            )
            realm = self._default_realm

        return {
            'username': match.group('username'),
            'resource': match.group('resource'),
            'domain': match.group('domain'),
            'realm': realm,
        }


class RealmData:
    """A RealmData object contains all data relevant for a specific realm.
    The attributes are injected at configuration-load-time.
    """

    def __init__(self, name, parent=None) -> None:
        self.name = name
        if parent is not None:
            self.vid = parent.vid
            self.dhcp_local_port = parent.dhcp_local_port
            self.dhcp_listening_device = parent.dhcp_listening_device
            self.dhcp_listening_ip = parent.dhcp_listening_ip
            self.provide_default_route = parent.provide_default_route
            self.default_gateway_ipv4 = parent.default_gateway_ipv4
            self.subnet_ipv6 = parent.subnet_ipv6
            self.default_gateway_ipv6 = parent.default_gateway_ipv6
            self.static_routes_ipv4 = parent.static_routes_ipv4
            self.static_routes_ipv6 = parent.static_routes_ipv6
            self.dhcp_server_ips = parent.dhcp_server_ips
            self.expected_dhcp_lease_time = parent.expected_dhcp_lease_time
        else:
            self.vid = None
            self.dhcp_local_port = 67
            self.dhcp_listening_device = None
            self.dhcp_listening_ip = None
            self.provide_default_route = True
            self.default_gateway_ipv4 = None
            self.subnet_ipv6 = None
            self.default_gateway_ipv6 = None
            self.static_routes_ipv4 = None
            self.static_routes_ipv6 = None
            self.dhcp_server_ips = None
            self.expected_dhcp_lease_time = None


class OvpnClient:
    """Represents an OpenVPN client connected to a specific OpenVPN server
    instance.
    """

    def __init__(
        self,
        timeout_mgr: TimeoutManager,
        refresh_lease_clb,
        full_username: str,
        server: ovpn.OvpnServer,
        realm_data: RealmData,
        leased_ip_address=None,
        rebinding_timeout: int = None,
        lease_timeout: int = None,
    ) -> None:
        self._timeout_mgr = timeout_mgr
        self._refresh_lease = refresh_lease_clb
        self.full_username = full_username
        self.server = server
        self._realm_data = realm_data
        self._leased_ip_address = leased_ip_address
        self._rebinding_timeout = rebinding_timeout
        self._lease_timeout = lease_timeout

        self._timeout_obj = None  # type: Optional[TimeoutObject]
        self._log = logging.getLogger('ovpnclient')
        self._killed = False

    def __str__(self) -> str:
        return '{} on {}'.format(self.full_username, self.server)

    def __repr__(self) -> str:
        return "<OvpnClient(common_name={}, server={}, ...)>".format(
            self.full_username, self.server
        )

    def track_lease(self) -> None:
        """Start keeping track of the DHCP lease time and make sure the lease
        is refreshed early enough.
        """
        if self._leased_ip_address is None or self._rebinding_timeout is None:
            self._log.error(
                'attempted to track lease for client "%s", but no lease available.',
                self,
            )
            return
        self._timeout_obj = TimeoutObject(self._rebinding_timeout, self.handle_timeout)
        self._timeout_mgr.add_timeout_object(self._timeout_obj)

    def kill(self) -> None:
        """Disable the client.  Although any pending activities will continue,
        no new activities will be started.
        """
        self._killed = True

    @property
    def iszombie(self) -> bool:
        """Has this client instance been killed?
        @return: Returns True if the instance has been killed, otherwise False.
        """
        return self._killed

    def handle_timeout(self) -> None:
        """Called as soon as the rebinding timeout occurs.
        """
        if self.iszombie:
            return

        if self._lease_timeout is not None and self._lease_timeout <= time.time():
            self._log.warning(
                'Rebinding timeout for %s called too late - '
                'lease has already expired on %d.  Disconnecting client.',
                self,
                self._lease_timeout,
            )
            self.server.disconnect_client(self.full_username)
            return

        try:
            self._refresh_lease(
                success_handler_clb=self._handle_lease_refresh_succeeded,
                failure_handler_clb=self._handle_lease_refresh_failed,
                client_identifier=self.full_username,
                device=self._realm_data.dhcp_listening_device,
                local_ip=self._realm_data.dhcp_listening_ip,
                server_ips=self._realm_data.dhcp_server_ips,
                client_ip=self._leased_ip_address,
                lease_time=self._realm_data.expected_dhcp_lease_time,
            )
        except Exception:
            self._log.exception('Adding a new DHCP refresh request failed')
            self.server.disconnect_client(self.full_username)

    def _handle_lease_refresh_succeeded(self, res) -> None:
        """Called as soon as the DHCP refresh request has completed and
        succeeded.  Takes care of remembering the new lease and refreshing
        again in time.
        @param res: Dictionary containing all data returned by the DHCP request.
        """
        if self.iszombie:
            return

        self._log.debug('DHCP refresh request succeeded: %s', repr(res))
        self._leased_ip_address = res['ip_address']
        rebinding_timeout = res['rebinding_timeout']  # type: int
        self._rebinding_timeout = rebinding_timeout
        self._lease_timeout = res['lease_timeout']
        self._timeout_obj = TimeoutObject(self._rebinding_timeout, self.handle_timeout)
        self._timeout_mgr.add_timeout_object(self._timeout_obj)

    def _handle_lease_refresh_failed(self) -> None:
        """Called as soon as the DHCP refresh request has completed and
        failed or has timed out.  Takes care of disconnecting the client, as
        the lease has obviously no chance of remaining established.
        """
        if self.iszombie:
            return

        self.server.disconnect_client(self.full_username)


class OvpnClientManager:
    """Manages a list of all clients currently connected to all known OpenVPN
    servers.  Takes care of regularly refreshing the client's DHCP leases.

    Periodically polls the OpenVPN servers to sync the list of connected
    clients.

    Note: Some clients might still be tracked by the manager, but already marked
          as killed.  These zombies should be collected as soon as the client-
          disconnect hook gets processed by OpenVPN or as soon as the next
          client list poll completes.
    """

    def __init__(
        self,
        timeout_mgr,
        realms_data: Dict[str, RealmData],
        parse_username_clb,
        servers,
        refresh_lease_clb,
        sync_interval=60,
    ) -> None:
        """\
        @param timeout_mgr: Reference to a timeout manager.
        @param realms_data: Map of realm names to realm data structures.
        @param parse_username_clb: Call-back to parse the full_username into
            its components.
        @param servers: List of OpenVPN servers to query.
        @param refresh_lease_clb: Callback for refreshing a DHCP lease.
        @param sync_interval: Intervall in which to poll the servers.
        """
        self._timeout_mgr = timeout_mgr
        self._realms_data = realms_data
        self._parse_username = parse_username_clb
        self._servers = servers
        self._refresh_lease = refresh_lease_clb
        self._sync_interval = sync_interval

        self._log = logging.getLogger('ovpnclientmgr')
        self._clients_by_username = {}  # type: Dict[str, OvpnClient]

        self._clients_by_server = {}  # type: Dict[str, Dict[str, OvpnClient]]
        for server in self._servers.values():
            self._clients_by_server[server] = {}

        self._timeout_mgr.add_rel_timeout(0, WeakBoundMethod(self._on_sync_clients))

    def create_client(self, **kwargs) -> OvpnClient:
        """Create and keep track of an OpenVPN client connection.  All keyword
        arguments are passed on to OvpnClient's constructor.
        @return: Returns the newly created OvpnClient instance.
        """
        client = OvpnClient(
            timeout_mgr=self._timeout_mgr,
            refresh_lease_clb=self._refresh_lease,
            **kwargs
        )
        client.track_lease()
        self._add_client(client)
        return client

    def _add_client(self, client) -> None:
        """Add a client, based on a completed and successful DHCP request.
        """
        if client.full_username in self._clients_by_username:
            self._log.info(
                'replacing client connection in client list with freshly connected '
                ' client instance: %s', client,
            )
            self._del_client(self._clients_by_username[client.full_username])
        else:
            self._log.debug('adding new client instance: %s', client)

        self._clients_by_username[client.full_username] = client
        self._clients_by_server[client.server][client.full_username] = client

    def sync_clients(self) -> None:
        """Syncs the client list with the client lists of each OpenVPN server.

        Any client connected to the server but not listed by us needs to be
        added to our client list.  Those client's leases need to be refreshed
        soon, as their last refresh time is unknown to us.

        Any clients that are listed by us but no longer listed by the OpenVPN
        server are removed from our list.  They have been disconnected.
        """
        for server in self._servers.values():
            # Asynchronously retrieve the list of clients against which to sync.
            server.poll_client_list(partial(self._sync_clients_with, server=server))

    def _on_sync_clients(self) -> None:
        """Timeout event handler to regularly sync clients.  See sync_clients().
        """
        self.sync_clients()
        self._timeout_mgr.add_rel_timeout(
            self._sync_interval, WeakBoundMethod(self._on_sync_clients)
        )

    def _sync_clients_with(self, client_data_list, server) -> None:
        """Called per-server as soon as the server's client data list has
        been retrieved.  Performs the actual processing as documented for
        sync_clients().
        """
        if client_data_list is None:
            self._log.error('syncing the client list with server %s failed', server)
            return

        client_data_by_username = {}

        for client_data in client_data_list:
            self._log.debug(
                'client_data: "%s" with "%s"',
                client_data.common_name,
                client_data.virtual_address,
            )
            if client_data.virtual_address is None:
                # Connection hasn't been fully established yet.  Skip it.
                continue

            client_data_by_username[client_data.common_name] = client_data

            if client_data.common_name in self._clients_by_username:
                client = self._clients_by_username[client_data.common_name]
                if client.server != server:
                    # The client has jumped servers.  Remove it from the list.
                    self._log.debug(
                        'cleaning up: client %s has moved to server "%s"',
                        client,
                        server,
                    )
                    self._del_client(client)

            if client_data.common_name not in self._clients_by_username:
                # New client!  Assume pessimistic last lease update time.  We're
                # probably recovering from a daemon restart.
                self._create_detected_client(
                    client_data.common_name, server, client_data.virtual_address
                )

        for client in list(self._clients_by_server[server].values()):
            if client.full_username not in client_data_by_username:
                # The client has been disconnected.
                if not client.iszombie:
                    self._log.debug(
                        'cleaning up: client %s was disconnected in the mean-while',
                        client,
                    )
                else:
                    self._log.debug('cleaning up: removing zombie client %s', client)
                self._del_client(client)

    def _del_client(self, client) -> None:
        """Kills a client instance and removes it from the manager's knowledge.
        In case the client has some pending operations, it might live on for
        some time.
        @param client: The client instance to kill and forget.
        """
        client.kill()
        del self._clients_by_username[client.full_username]
        del self._clients_by_server[client.server][client.full_username]

    def client_disconnected(self, full_username, server) -> None:
        """Called when a client was disconnected.
        """
        if server not in self._clients_by_server:
            self._log.error(
                'attempted to disconnect user from unkown server "%s" (user "%s")',
                server,
                full_username,
            )
            return
        server_clients = self._clients_by_server[server]

        if full_username not in server_clients:
            self._log.error('attempting to disconnect user: "%s"', full_username)
            return
        client = server_clients[full_username]

        if not client.iszombie:
            self._log.debug('removing zombie client %s', client)
        else:
            self._log.debug('disconnected %s', client)
        self._del_client(client)

    def _create_detected_client(self, full_username, server, leased_ip_address) -> None:
        """Create a client instance without knowledge of the last DHCP lease
        refresh time.  Therefore, the client's next lease update time is set to
        "soon".

        @param full_username: The full username of the connected client.
        @param server: The server instance the client is connected to.
        """
        self._log.debug('detected client "%s"', full_username)
        ret = self._parse_username(full_username)
        if ret is None:
            self._log.warning('parsing username "%s" failed', full_username)
            server.disconnect_client(full_username)
            return
        realm = ret['realm']

        if realm not in self._realms_data:
            self._log.warning('unknown realm "%s" for user "%s"', realm, full_username)
            server.disconnect_client(full_username)
            return
        realm_data = self._realms_data[realm]

        # We have no idea when the last refresh occured for this client,
        # but it's unlikely to be needed immediately.  Spread out the
        # requests a bit.
        rebinding_timeout = time.time() + random.uniform(0, 10)

        self.create_client(
            server=server,
            full_username=full_username,
            realm_data=realm_data,
            leased_ip_address=leased_ip_address,
            rebinding_timeout=rebinding_timeout,
            lease_timeout=None,
        )


class OvpnCmdConn(CommandConnection):
    """Represents an incoming command connection from one of the OpenVPN
    hooks.
    """

    def __init__(
        self,
        sloop,
        sock,
        realms_data,
        servers,
        secret,
        add_request_clb,
        parse_username_clb,
        create_client_clb,
        remove_client_clb,
    ) -> None:
        """\
        @param sloop: Socket loop instance.  (See CommandConnection for
            details.)
        @param sock: Socket of the command connection.  (See CommandConnection
            for details.)
        @param realms_data: Dictionary of realms data objects.  Indexed by
            realm name.
        @param servers: Dictionary of servers.  Indexed by server name.
        @param add_request_clb: Call-back for starting an initial DHCP request.
        @param parse_username_clb: Call-back for parsing a full username into
            the components.
        @param create_client_clb: Call-back for creating and registering a new
            OpenVPN client instance.
        @param remove_client_clb: Call-back for removing an existing OpenVPN
            client instance.
        """
        CommandConnection.__init__(
            self, sloop=sloop, sock=sock, log=logging.getLogger('ovpncmdconn')
        )
        self._realms_data = realms_data
        self._servers = servers
        self._secret = secret
        self._add_request = add_request_clb
        self._parse_username = parse_username_clb
        self._create_client = create_client_clb
        self._remove_client = remove_client_clb
        self._ret_f = None
        self._wrote_ret = False
        self._config_f = None  # type: Optional[TextIO]
        self._full_username = None
        self._server = None
        self._realm_data = None  # type: Optional[RealmData]

    def __del__(self) -> None:
        self._log.debug('destructing OvpnCmdConn')
        if self._ret_f is not None:
            if not self._wrote_ret:
                self._write_ret(ovpn.CC_RET_FAILED)
            self._ret_f.close()
        if self._config_f is not None:
            self._config_f.close()
        CommandConnection.__del__(self)

    def _write_ret(self, val) -> None:
        """Write a specific return value to the deferred return value file.
        @param val: A CC_RET_* value.
        """
        self._log.debug('writing deferred return value %d', val)
        assert self._ret_f is not None
        ovpn.write_deferred_ret_file(self._ret_f, val)
        self._wrote_ret = True

    def _success_handler(self, res) -> None:
        """Called as soon as the DHCP address request has completed and
        succeeded.  Takes care of passing on the received parameters to the
        OpenVPN server and remembering the client for later lease refreshing.

        @param res: Dictionary containing all data returned by the DHCP request.
        """
        self._log.debug('DHCP request succeeded: %s', repr(res))

        if 'ip_address' not in res or 'subnet_mask' not in res:
            self._log.error(
                'DHCP request failed to provide a valid IP ' 'address: %r', res
            )
            self._write_ret(ovpn.CC_RET_FAILED)
            return

        if 'rebinding_timeout' not in res or 'lease_timeout' not in res:
            self._log.error('DHCP request without lease indication: %r', res)
            self._write_ret(ovpn.CC_RET_FAILED)
            return

        self._log.debug('writing OpenVPN client configuration')
        assert self._config_f is not None and self._realm_data is not None
        self._config_f.seek(0)

        self._config_f.write(
            'ifconfig-push {} {}\n'.format(res['ip_address'], res['subnet_mask'])
        )
        self._config_f.write('push "ip-win32 dynamic"\n')

        if self._realm_data.subnet_ipv6 is not None:
            prefix = self._realm_data.subnet_ipv6

            today = str(datetime.date.today())
            hasher = hashlib.sha256()
            hasher.update((self._full_username + today + self._secret).encode('utf-8'))
            hash_hex = hasher.hexdigest()[:16]
            ipv6_network = IPv6Network(prefix).network_address
            ip6_address = str(ipv6_network + int(hash_hex, 16))

            if self._realm_data.default_gateway_ipv6 is not None:
                ip6_gateway = self._realm_data.default_gateway_ipv6
            else:
                ip6_gateway = str(ipv6_network + 1)

            self._config_f.write(
                'ifconfig-ipv6-push {} {}\n'.format(ip6_address, ip6_gateway)
            )

        if self._realm_data.vid is not None:
            self._config_f.write('vlan-pvid %d\n' % self._realm_data.vid)

        if self._realm_data.default_gateway_ipv4 is not None:
            self._config_f.write(
                'push "route-gateway %s"\n' % (self._realm_data.default_gateway_ipv4)
            )
        elif 'gateway' in res:
            self._config_f.write('push "route-gateway %s"\n' % (res['gateway']))
        else:
            self._log.debug(
                'DHCP request provided no gateway information: %s' % (repr(res))
            )

        if self._realm_data.provide_default_route:
            if self._realm_data.default_gateway_ipv6 is not None:
                self._config_f.write('push "route-ipv6 2000::/3"\n')
                self._config_f.write('push "redirect-gateway def1"\n')
            elif 'gateway' in res or self._realm_data.default_gateway_ipv4 is not None:
                self._config_f.write('push "redirect-gateway def1"\n')
        else:
            static_routes_ipv4 = []  # type: List[str]
            if self._realm_data.static_routes_ipv4 is not None:
                static_routes_ipv4 += self._realm_data.static_routes_ipv4
            if 'static_routes' in res:
                static_routes_ipv4 += res['static_routes']
            if len(static_routes_ipv4) > 0:
                for network, netmask, gateway in static_routes_ipv4:
                    self._config_f.write(
                        'push "route {} {} {}"\n'.format(network, netmask, gateway)
                    )

            static_routes_ipv6 = []  # type: List[str]
            if (
                self._realm_data.subnet_ipv6 is not None
                and self._realm_data.static_routes_ipv6 is not None
            ):
                static_routes_ipv6 += self._realm_data.static_routes_ipv6
            for network, gateway in static_routes_ipv6:
                self._config_f.write(
                    'push "route-ipv6 {} {}"\n'.format(network, gateway)
                )

        self._config_f.write('push "redirect-private"\n')

        for dns_ip in res['dns']:
            self._config_f.write('push "dhcp-option DNS %s"\n' % dns_ip)
        if 'domain' in res:
            self._config_f.write('push "dhcp-option DOMAIN %s"\n' % res['domain'])

        self._config_f.flush()
        os.fsync(self._config_f.fileno())

        self._write_ret(ovpn.CC_RET_SUCCEEDED)

        self._create_client(
            full_username=self._full_username,
            server=self._server,
            realm_data=self._realm_data,
            leased_ip_address=res['ip_address'],
            rebinding_timeout=res['rebinding_timeout'],
            lease_timeout=res['lease_timeout'],
        )

    def _failure_handler(self) -> None:
        """Called as soon as the DHCP address request has failed or timed out.
        Takes care of notifying the OpenVPN server of the failure.
        """
        self._log.debug('DHCP request failed')
        self._write_ret(ovpn.CC_RET_FAILED)

    def handle_cmd(self, cmd, params, files) -> None:
        """Called for each command received over the command socket.  Forwards
        the command processing according to the command's name.

        @param cmd: Name of the command.
        @param params: Dictionary of the command's parameters.
        @param files: Array of file pointers passed through with the command.
        """
        if cmd == 'request':
            self._handle_request_cmd(cmd, params, files)
        elif cmd == 'disconnect':
            self._handle_disconnect_cmd(cmd, params, files)
        else:
            self.send_cmd('FAIL')
            self._log.warning('received unknown command "%s"', cmd)
            return

    def _handle_request_cmd(self, cmd, params, files) -> None:
        """Handles the command for sending an initial DHCP address request.
        """
        try:
            self._full_username = params['full_username']
            ret_fidx = params['ret_file_idx']
            config_fidx = params['config_file_idx']
            server_name = params['daemon_name']
        except KeyError as exc:
            self.send_cmd('FAIL')
            self._log.warning('command "%s" is missing a parameter: %s', cmd, exc.args)
            return

        try:
            ret_f = files[int(ret_fidx)]
            config_f = files[int(config_fidx)]
        except IndexError as exc:
            self.send_cmd('FAIL')
            self._log.warning('file descriptor index out of range: %s', exc.args)
            return
        except ValueError as exc:
            self.send_cmd('FAIL')
            self._log.warning('file descriptor index parsing failed: %s', exc.args)
            return

        ret = self._parse_username(self._full_username)
        if ret is None:
            self.send_cmd('FAIL')
            self._log.warning('parsing username failed: "%s"', self._full_username)
            return
        realm = ret['realm']

        if realm not in self._realms_data:
            self.send_cmd('FAIL')
            self._log.error('unknown realm "%s"', realm)
            return

        if server_name not in self._servers:
            self.send_cmd('FAIL')
            self._log.error('unknown server "%s"', server_name)
            return

        realm_data = self._realms_data[realm]
        self._realm_data = realm_data
        self._server = self._servers[server_name]

        self.send_cmd('OK')
        self._ret_f = ret_f
        self._config_f = config_f

        try:
            self._add_request(
                success_handler_clb=self._success_handler,
                failure_handler_clb=self._failure_handler,
                client_identifier=self._full_username,
                device=realm_data.dhcp_listening_device,
                local_ip=realm_data.dhcp_listening_ip,
                server_ips=realm_data.dhcp_server_ips,
                lease_time=realm_data.expected_dhcp_lease_time,
            )
        except Exception:
            self._log.exception('Adding a new DHCP request failed')

    def _handle_disconnect_cmd(self, cmd, params, files) -> None:
        """Handles the command for informing us of a client disconnect.
        """
        try:
            full_username = params['full_username']
            server_name = params['daemon_name']
        except KeyError as exc:
            self.send_cmd('FAIL')
            self._log.warning('command "%s" is missing a parameter: %s', cmd, exc.args)
            return

        if server_name not in self._servers:
            self.send_cmd('FAIL')
            self._log.error('unknown server "%s"', server_name)
            return
        server = self._servers[server_name]

        self.send_cmd('OK')
        self._remove_client(full_username, server)


def cfg_get_def(cfg, sect, opt, default=None) -> str:
    """Small helper to allow configuration options with program-defined
    defaults.
    """
    if cfg.has_option(sect, opt):
        return cfg.get(sect, opt)
    else:
        return default


def cfg_getint_def(cfg, sect, opt, default=None) -> int:
    """Small helper to allow configuration options with program-defined
    defaults.
    """
    if cfg.has_option(sect, opt):
        return cfg.getint(sect, opt)
    else:
        return default


def cfg_getboolean_def(cfg, sect, opt, default=None) -> bool:
    """Small helper to allow configuration options with program-defined
    defaults.
    """
    if cfg.has_option(sect, opt):
        return cfg.getboolean(sect, opt)
    else:
        return default


def split_cfg_list(val, split=',') -> List[str]:
    """Split a string along "split" characters - ignoring any spaces
    surrounding the split characters.
    @param val: The string to split at the split characters.
    @param split: Optional split characters to use for splitting.  Defaults to
        ",".
    @return: Returns a list of strings.
    """
    return [v.strip() for v in val.split(split) if (len(v.strip()) > 0)]


def cfg_iterate(cfg, section_type) -> Iterator[Tuple[str, str]]:
    """Iterate over all config sections who's section names are prefixed with
    the specified section type.
    @param cfg: Config object to read the sections from.
    @param section_type: Section type name that acts as the prefix for all
        relevant section names.
    @return: Returns a tuple of section name and element part of the name
        (without the section type prefix).
    """
    sec_start = '%s ' % section_type
    for sect_name in [s for s in cfg.sections() if s.startswith(sec_start)]:
        elem_name = sect_name[len(sec_start) :]
        yield (sect_name, elem_name)


def user_to_uid(user) -> int:
    """Transforms a user to a UID.  In case the user is already a UID, the
    UID is passed through unchanged.
    @param user: The user as string.  It may either be the username or the
        user's UID.
    @return: Returns the UID as integer.
    """
    try:
        uid = int(user)
    except ValueError:
        try:
            uid = pwd.getpwnam(user).pw_uid
        except KeyError:
            logging.critical('could not resolve user "%s", exiting', user)
            sys.exit(1)
    return uid


def group_to_gid(group) -> int:
    """Transforms a group to a GID.  In case the group is already a GID, the
    GID is passed through unchanged.
    @param group: The group as string.  It may either be the group name or the
        group's GID.
    @return: Returns the GID as integer.
    """
    try:
        gid = int(group)
    except ValueError:
        try:
            gid = grp.getgrnam(group).gr_gid
        except KeyError:
            logging.critical('could not resolve group "%s", exiting', group)
            sys.exit(1)
    return gid


def get_ip_for_iface(iface) -> str:
    """Look up an IPv4 address on the given interface.
    @param iface: Interface name
    @return: Returns the IPv4 address as string.
    """
    import netifaces

    addrs = netifaces.ifaddresses(iface)
    if (
        netifaces.AF_INET not in addrs
        or len(addrs[netifaces.AF_INET]) == 0
        or 'addr' not in addrs[netifaces.AF_INET][0]
    ):
        raise RuntimeError('Could not detect IPv4 address on interface "%s"' % iface)
    return addrs[netifaces.AF_INET][0]['addr']


def parse_static_routes_ipv4(data) -> List[Tuple[str, str, str]]:
    """Parses a string of the form
      0.0.0.0/0 via 10.0.98.120, 10.0.97.0/24 via 10.0.98.121
    @return: Returns a list of network, netmask and gateway tuples.
    """
    static_routes = []
    for static_route_str in split_cfg_list(data):
        network_str, via, gateway = split_cfg_list(static_route_str, split=' ')
        if via != 'via':
            raise RuntimeError('Invalid static route format: "%s' % static_route_str)
        network = IPv4Network(network_str)
        static_routes.append(
            (str(network.network_address), str(network.netmask), gateway)
        )
    return static_routes


def parse_static_routes_ipv6(data: str) -> List[Tuple[str, str]]:
    """Parses a string of the form
      ::/0 via fd00::1, fd01:1234::/64 via fd00::1
    @return: Returns a list of prefix and gateway tuples.
    """
    static_routes = []
    for static_route_str in split_cfg_list(data):
        network, via, gateway = split_cfg_list(static_route_str, split=' ')
        if via != 'via':
            raise RuntimeError('Invalid static route format: "%s' % static_route_str)
        static_routes.append((network, gateway))
    return static_routes


def process_realm(cfg, realm_name, realms, delayed_realms) -> None:
    sect = 'realm %s' % realm_name

    logging.debug('processing realm "%s"', realm_name)
    parent_realm_name = cfg_get_def(cfg, sect, 'include_realm')
    if parent_realm_name is not None:
        # Is the realm we depend on already loaded?
        if parent_realm_name not in realms:
            logging.debug(
                'processing of realm "%s" delayed, because of dependency on "%s"',
                realm_name,
                parent_realm_name,
            )
            # No, so delay loading this realm and try again later.
            delayed_realms.append(realm_name)
            return
        realm_data = RealmData(realm_name, parent=realms[parent_realm_name])
    else:
        realm_data = RealmData(realm_name)

    realms[realm_name] = realm_data

    realm_data.vid = cfg_getint_def(cfg, sect, 'vid', realm_data.vid)

    realm_data.dhcp_local_port = cfg_getint_def(
        cfg, sect, 'dhcp_local_port', realm_data.dhcp_local_port
    )

    if cfg.has_option(sect, 'dhcp_listening_device'):
        realm_data.dhcp_listening_device = cfg.get(sect, 'dhcp_listening_device')
        # If a device is explicitly set, the listening IP needs to be explicitly
        # set too (or the implicit detection needs to be performed again).
        realm_data.dhcp_listening_ip = None

    realm_data.dhcp_listening_ip = cfg_get_def(
        cfg, sect, 'dhcp_listening_ip', realm_data.dhcp_listening_ip
    )

    realm_data.provide_default_route = cfg_getboolean_def(
        cfg, sect, 'provide_default_route', realm_data.provide_default_route
    )

    realm_data.default_gateway_ipv4 = cfg_get_def(
        cfg, sect, 'default_gateway_ipv4', realm_data.default_gateway_ipv4
    )

    realm_data.subnet_ipv6 = cfg_get_def(
        cfg, sect, 'subnet_ipv6', realm_data.subnet_ipv6
    )

    realm_data.default_gateway_ipv6 = cfg_get_def(
        cfg, sect, 'default_gateway_ipv6', realm_data.default_gateway_ipv6
    )

    if cfg.has_option(sect, 'static_routes_ipv4'):
        realm_data.static_routes_ipv4 = parse_static_routes_ipv4(
            cfg.get(sect, 'static_routes_ipv4')
        )

    if cfg.has_option(sect, 'static_routes_ipv6'):
        realm_data.static_routes_ipv6 = parse_static_routes_ipv6(
            cfg.get(sect, 'static_routes_ipv6')
        )

    if (
        realm_data.dhcp_listening_device is not None
        and realm_data.dhcp_listening_ip is None
    ):
        # We need to determine an IPv4 address on the specified network
        # device.
        realm_data.dhcp_listening_ip = get_ip_for_iface(
            realm_data.dhcp_listening_device
        )

    if cfg.has_option(sect, 'dhcp_server_ips'):
        realm_data.dhcp_server_ips = [
            socket.gethostbyname(i.strip())
            for i in cfg.get(sect, 'dhcp_server_ips').split(',')
        ]

    realm_data.expected_dhcp_lease_time = cfg_getint_def(
        cfg, sect, 'expected_dhcp_lease_time', realm_data.expected_dhcp_lease_time
    )


def read_realms(cfg) -> Optional[Dict[str, RealmData]]:
    """Read all realms from the configuration file into a dictionary of
    RealmData objects.
    """
    realms = {}  # type: Dict[str, RealmData]

    delayed_realms = []  # type: List[str]
    for sect, realm_name in cfg_iterate(cfg, 'realm'):
        process_realm(cfg, realm_name, realms, delayed_realms)

    # Did we have any delays, due to dependencies between realms?  Process them
    # now.
    while len(delayed_realms) > 0:
        new_delayed_realms = []  # type: List[str]
        for delayed_realm in delayed_realms:
            process_realm(cfg, delayed_realm, realms, new_delayed_realms)

        # In case nothing changed, we have a dependency loop.
        if len(delayed_realms) == len(new_delayed_realms):
            logging.error(
                'realm error: apparently recursive include '
                'relationships between: %s',
                ', '.join(delayed_realms),
            )
            return None
        delayed_realms = new_delayed_realms
    return realms


def drop_caps(user=None, group=None, caps=None) -> None:
    """Switches aways from UID 0 and full capabilities to a different user
    and a limited set of capabilities.  Child processes get none of the
    capabilities.
    @param user: The target user
    @param group: The target group
    @param caps: List of capabilities to retain.
    """
    if caps is None:
        caps = []

    if group is not None:
        # Switch to new GID.
        logging.debug('Switching to group %s', str(group))
        gid = group_to_gid(group)
        os.setgid(gid)
        os.setgroups([gid])

    if user is not None:
        # Retain all capabilities over UID switch.
        prctl.set_keepcaps(True)

        # Switch to new UID.
        logging.debug('Switching to user %s', str(user))
        os.setuid(user_to_uid(user))

    if len(caps) > 0:
        logging.debug('Restricting to capabilities "%s"', ', '.join(caps))
        # Some capabilities might be permitted but not effective, so explicitly
        # set them to effective here.
        for cap in caps:
            setattr(prctl.cap_effective, cap, True)
    else:
        logging.debug('Dropping all capabilities.')
    # Drop all capabilities except those listed in "caps".
    prctl.cap_effective.limit(*caps)
    prctl.cap_permitted.limit(*caps)
    # Child processes may not use our capabilities.
    prctl.cap_inheritable.limit()


def read_servers(cfg, sloop) -> Dict[str, ovpn.OvpnServer]:
    """Read all servers from the configuration file and connect to each of
    them.
    """
    servers = {}
    for sect, server_name in cfg_iterate(cfg, 'ovpn-server'):
        server = ovpn.OvpnServer(
            sloop, name=server_name, socket_fn=cfg.get(sect, 'mgmt_socket')
        )
        servers[server_name] = server
    return servers


def load_requestors(sloop, requestor_mgr, realms_data) -> bool:
    """Load all requestors, based on the existing realms.

    @param sloop: Socket loop instance.
    @param requestor_mgr: Requestor manager instance.
    @param realms_data: Dictionary of all existing realms.
    @return: Returns False in case an error occured while loading the
        requestors.  Otherwise returns True.
    """
    try:
        for realm_data in realms_data.values():
            if requestor_mgr.has_requestor(
                realm_data.dhcp_listening_device, realm_data.dhcp_listening_ip
            ):
                # Skip creating requestor, a previous realm already did that.
                continue

            requestor = odr.dhcprequestor.DhcpAddressRequestor(
                listen_address=realm_data.dhcp_listening_ip,
                listen_port=realm_data.dhcp_local_port,
                listen_device=realm_data.dhcp_listening_device,
            )
            sloop.add_socket_handler(requestor)
            requestor_mgr.add_requestor(requestor)
    except odr.listeningsocket.SocketLocalAddressBindFailed as ex:
        logging.error(
            'Could not bind to DHCP listening address %s:%d@%s',
            ex.args[0],
            ex.args[1],
            ex.args[2],
        )
        return False
    return True


def setup_logging(loglevel, use_syslog=False) -> None:
    root = logging.getLogger()
    root.setLevel(loglevel)

    if use_syslog:
        from logging.handlers import SysLogHandler

        hdlr = SysLogHandler(address='/dev/log')  # type: logging.Handler
    else:
        hdlr = logging.StreamHandler()

    fmt = logging.Formatter(logging.BASIC_FORMAT)
    hdlr.setFormatter(fmt)
    root.addHandler(hdlr)


def main() -> None:
    prctl.set_name('odrd')
    prctl.set_proctitle(' '.join(sys.argv))

    parser = OptionParser()
    parser.add_option(
        "-c",
        "--config",
        dest="config_file",
        help="Configuration file",
        default=CONFIG_FILE,
    )
    parser.add_option(
        "--debug",
        dest="debug",
        action="store_true",
        help="Activate debug logging",
        default=False,
    )
    parser.add_option(
        "--keep-user",
        dest="keep_user",
        action="store_true",
        help="Do not switch to a different UID / GID; ignore capabilities",
        default=False,
    )
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    cfg = ConfigParser()
    cfg.read(options.config_file)

    loglevel = logging.INFO
    if options.debug:
        loglevel = logging.DEBUG
    setup_logging(loglevel, cfg_getboolean_def(cfg, 'daemon', 'syslog', False))

    if not options.keep_user:
        # Capability net_raw is needed for binding to network devices.
        # Capability net_bind_service is needed for binding to the DHCP port.
        drop_caps(
            user=cfg_get_def(cfg, 'daemon', 'user', None),
            group=cfg_get_def(cfg, 'daemon', 'group', None),
            caps=['net_raw', 'net_bind_service'],
        )

    realms_data = read_realms(cfg)
    if realms_data is None:
        sys.exit(1)

    sloop = SocketLoop()

    def exit_daemon(*args) -> None:
        """Signal handler performing a soft shutdown of the loop.
        """
        logging.info('exiting on signal')
        sloop.quit()

    signal.signal(signal.SIGTERM, exit_daemon)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    timeout_mgr = TimeoutManager()
    sloop.add_idle_handler(timeout_mgr.check_timeouts)

    requestor_mgr = odr.dhcprequestor.DhcpAddressRequestorManager()

    servers = read_servers(cfg, sloop)

    for server in servers.values():
        ovpn.OvpnServerSupervisor(
            timeout_mgr=weakref.proxy(timeout_mgr),
            server=weakref.proxy(server),
            timeout=30,
        )

    def start_dhcp_address_request(device, local_ip, **kwargs) -> None:
        requestor = requestor_mgr.get_requestor(device, local_ip)
        if requestor is None:
            return
        request = odr.dhcprequestor.DhcpAddressInitialRequest(
            timeout_mgr=weakref.proxy(timeout_mgr),
            requestor=weakref.proxy(requestor),
            local_ip=local_ip,
            **kwargs
        )
        requestor.add_request(request)

    def start_dhcp_refresh_request(device, local_ip, **kwargs) -> None:
        requestor = requestor_mgr.get_requestor(device, local_ip)
        if requestor is None:
            return
        request = odr.dhcprequestor.DhcpAddressRefreshRequest(
            timeout_mgr=weakref.proxy(timeout_mgr),
            requestor=weakref.proxy(requestor),
            local_ip=local_ip,
            **kwargs
        )
        requestor.add_request(request)

    parse_username = ParseUsername(
        default_realm=cfg_get_def(cfg, 'daemon', 'default_realm')
    )

    client_mgr = OvpnClientManager(
        timeout_mgr=timeout_mgr,
        servers=servers,
        refresh_lease_clb=start_dhcp_refresh_request,
        realms_data=realms_data,
        parse_username_clb=parse_username.parse_username,
    )

    def create_vpn_cmd_conn(sloop, sock) -> OvpnCmdConn:
        return OvpnCmdConn(
            sloop,
            sock,
            realms_data=realms_data,
            servers=servers,
            secret=cfg_get_def(cfg, 'daemon', 'secret', None),
            create_client_clb=client_mgr.create_client,
            remove_client_clb=client_mgr.client_disconnected,
            add_request_clb=start_dhcp_address_request,
            parse_username_clb=parse_username.parse_username,
        )

    cmd_socket_uids = [
        user_to_uid(user)
        for user in split_cfg_list(cfg_get_def(cfg, 'daemon', 'cmd_socket_uids', ''))
    ]
    cmd_socket_gids = [
        group_to_gid(group)
        for group in split_cfg_list(cfg_get_def(cfg, 'daemon', 'cmd_socket_gids', ''))
    ]

    def cmd_conn_auth_check(sock, pid, uid, gid) -> bool:
        if uid in cmd_socket_uids:
            return True
        if gid in cmd_socket_gids:
            return True
        return False

    cmd_socket_perms = int(cfg_get_def(cfg, 'daemon', 'cmd_socket_perms', '0666'), 8)
    for unix_socket_fn in split_cfg_list(cfg_get_def(cfg, 'daemon', 'cmd_sockets', '')):
        cmd_listener = CommandConnectionListener(
            sloop=weakref.proxy(sloop),
            socket_path=unix_socket_fn,
            cmd_conn_factory=create_vpn_cmd_conn,
            socket_perm_mode=cmd_socket_perms,
            auth_check=cmd_conn_auth_check,
        )
        sloop.add_socket_handler(cmd_listener)

    if not load_requestors(sloop, requestor_mgr, realms_data):
        sys.exit(1)

    if not options.keep_user:
        # Special capabilities no longer necessary.
        drop_caps()

    try:
        sloop.run()
    except Exception:
        logging.exception('Caught exception in main loop, exiting.')
        sys.exit(1)


if __name__ == '__main__':
    main()
