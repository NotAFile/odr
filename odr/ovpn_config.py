"""
utility for easily creating and inspecting openvpn client configs
"""

from itertools import chain

from typing import Iterable


def config_escape(option: str) -> str:
    """escape a single value as required by openvpn"""
    # escape backslashes
    option = option.replace("\\", "\\\\")

    # quote if necessary
    if any(c.isspace() for c in option):
        return '"{}"'.format(option.replace('"', '\\"'))

    return option


def make_config_line(values: Iterable[str]) -> str:
    return " ".join(config_escape(val) for val in values)


class OvpnConf:
    """construct an openvpn config"""

    def __init__(self):
        self.lines = []

    def add(self, *values) -> None:
        """add one config line"""
        self.lines.append(make_config_line(values))

    def push(self, *values) -> None:
        """add one one config line to push to the client"""
        self.add("push", make_config_line(values))

    def push_dhcp_option(self, option, *values) -> None:
        """add one one dhcp option to push to the client"""
        self.push("dhcp-option", option.upper(), *values)

    def to_text(self) -> str:
        """get the current config as a string"""
        return "\n".join(self.lines)
