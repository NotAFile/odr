"""Realmdata stores the static configuration data of a realm"""

from configparser import ConfigParser, SectionProxy
import logging
import socket

from typing import Dict, Optional, List

from .config import (
    cfg_iterate,
    parse_static_routes_ipv4,
    parse_static_routes_ipv6,
    split_cfg_list,
)


class RealmData:
    """A RealmData object contains all data relevant for a specific realm.
    The attributes are injected at configuration-load-time.
    """

    def __init__(self, name, parent: RealmData = None) -> None:
        self.name = name
        if parent is not None:
            self.vid = parent.vid
            self.dhcp_local_port = parent.dhcp_local_port
            self.dhcp_listening_device = parent.dhcp_listening_device
            self.dhcp_listening_ip = parent.dhcp_listening_ip
            self.provide_default_route = parent.provide_default_route
            self.default_gateway_ipv4 = parent.default_gateway_ipv4
            self.subnet_ipv6 = parent.subnet_ipv6
            self.subnet_ipv4 = parent.subnet_ipv4
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
            self.subnet_ipv4 = None
            self.default_gateway_ipv6 = None
            self.static_routes_ipv4 = None
            self.static_routes_ipv6 = None
            self.dhcp_server_ips = None
            self.expected_dhcp_lease_time = None


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


class RealmDepenencyNotLoaded(Exception):
    """raised to inform that this realm depends on a parent which has not been
    loaded"""


def process_realm(
    cfg: SectionProxy,
    realm_name: str,
    realms: Dict[str, RealmData],
) -> RealmData:
    """take a realm config section and turn it into RealmData, resolving
    interfaces and addresses as needed"""
    logging.debug('processing realm "%s"', realm_name)
    parent_realm_name = cfg.get('include_realm', fallback=None)
    if parent_realm_name is not None:
        # Is the realm we depend on already loaded?
        if parent_realm_name not in realms:
            logging.debug(
                'processing of realm "%s" delayed, because of dependency on "%s"',
                realm_name,
                parent_realm_name,
            )
            # No, so delay loading this realm and try again later.
            raise RealmDepenencyNotLoaded()
        realm_data = RealmData(realm_name, parent=realms[parent_realm_name])
    else:
        realm_data = RealmData(realm_name)

    realm_data.vid = cfg.getint('vid', fallback=realm_data.vid)

    realm_data.dhcp_local_port = cfg.getint(
        'dhcp_local_port', fallback=realm_data.dhcp_local_port
    )

    if cfg.has_option('dhcp_listening_device'):
        realm_data.dhcp_listening_device = cfg.get('dhcp_listening_device')
        # If a device is explicitly set, the listening IP needs to be explicitly
        # set too (or the implicit detection needs to be performed again).
        realm_data.dhcp_listening_ip = None

    realm_data.dhcp_listening_ip = cfg.get(
        'dhcp_listening_ip', fallback=realm_data.dhcp_listening_ip
    )

    realm_data.provide_default_route = cfg.getboolean(
        'provide_default_route', fallback=realm_data.provide_default_route
    )

    realm_data.default_gateway_ipv4 = cfg.get(
        'default_gateway_ipv4', fallback=realm_data.default_gateway_ipv4
    )

    realm_data.subnet_ipv6 = cfg.get('subnet_ipv6', fallback=realm_data.subnet_ipv6)

    realm_data.subnet_ipv4 = cfg.get('subnet_ipv4', fallback=realm_data.subnet_ipv4)

    realm_data.default_gateway_ipv6 = cfg.get(
        'default_gateway_ipv6', fallback=realm_data.default_gateway_ipv6
    )

    if cfg.has_option('static_routes_ipv4'):
        realm_data.static_routes_ipv4 = parse_static_routes_ipv4(
            cfg.get('static_routes_ipv4')
        )

    if cfg.has_option('static_routes_ipv6'):
        realm_data.static_routes_ipv6 = parse_static_routes_ipv6(
            cfg.get('static_routes_ipv6')
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

    if cfg.has_option('dhcp_server_ips'):
        realm_data.dhcp_server_ips = [
            socket.gethostbyname(i.strip())
            for i in cfg.get('dhcp_server_ips').split(',')
        ]

    realm_data.expected_dhcp_lease_time = cfg.getint(
        'expected_dhcp_lease_time', fallback=realm_data.expected_dhcp_lease_time
    )

    return realm_data


def read_realms(cfg: ConfigParser, default_dhcp_device = None) -> Optional[Dict[str, RealmData]]:
    """Read all realms from the configuration file into a dictionary of
    RealmData objects.
    """
    realms = {}  # type: Dict[str, RealmData]

    delayed_realms = []  # type: List[str]
    for _, realm_name in cfg_iterate(cfg, 'realm'):
        sect = 'realm ' + realm_name

        # this is the least ugly way to do this, sadly
        if default_dhcp_device and not cfg[sect].has_option("dhcp_listening_device"):
            cfg[sect]["dhcp_listening_device"] = default_dhcp_device

        try:
            realm = process_realm(cfg[sect], realm_name, realms)
        except RealmDepenencyNotLoaded:
            delayed_realms.append(realm_name)
        realms[realm_name] = realm

    # Did we have any delays, due to dependencies between realms?  Process them
    # now.
    while len(delayed_realms) > 0:
        new_delayed_realms = []  # type: List[str]
        for delayed_realm in delayed_realms:
            try:
                process_realm(cfg[sect], delayed_realm, realms)
            except RealmDepenencyNotLoaded:
                new_delayed_realms.append(delayed_realm)

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
