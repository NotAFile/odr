"""helpers for reading the odr config"""
from typing import Dict, Optional, Any, List, Tuple, Iterator, TextIO


def split_cfg_list(val: str, split=",") -> List[str]:
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
    sec_start = "%s " % section_type
    for sect_name in [s for s in cfg.sections() if s.startswith(sec_start)]:
        elem_name = sect_name[len(sec_start) :]
        yield (sect_name, elem_name)


def parse_static_routes_ipv4(data) -> List[Tuple[str, str, str]]:
    """Parses a string of the form
      0.0.0.0/0 via 10.0.98.120, 10.0.97.0/24 via 10.0.98.121
    @return: Returns a list of network, netmask and gateway tuples.
    """
    static_routes = []
    for static_route_str in split_cfg_list(data):
        network_str, via, gateway = split_cfg_list(static_route_str, split=" ")
        if via != "via":
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
        network, via, gateway = split_cfg_list(static_route_str, split=" ")
        if via != "via":
            raise RuntimeError('Invalid static route format: "%s' % static_route_str)
        static_routes.append((network, gateway))
    return static_routes
