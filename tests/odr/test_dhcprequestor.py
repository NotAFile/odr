from unittest.mock import Mock
import logging

import pytest

from odr.dhcprequestor import (
    DhcpAddressRequest,
    parse_classless_static_routes,
    DhcpAddressInitialRequest,
    DhcpAddressRefreshRequest,
)
from pydhcplib.dhcp_packet import DhcpPacket


@pytest.fixture()
def dhcprequest(timeout_mgr):
    def _make_dhcprequest(
        *,
        cls=DhcpAddressRequest,
        requestor=None,
        success_clb=None,
        failure_clb=None,
        **kwargs
    ):
        req = cls(
            requestor=requestor,
            timeout_mgr=timeout_mgr,
            success_handler_clb=success_clb,
            failure_handler_clb=failure_clb,
            local_ip="127.1.2.3",
            client_identifier="test123",
            server_ips=["123.123.123.123"],
            **kwargs,
        )
        req._start_time = 1580000000

        return req

    return _make_dhcprequest


def test_parse_classless_static_routes():
    data = [24, 192, 168, 10, 192, 168, 10, 1]
    data.extend([32, 8, 8, 8, 8, 1, 1, 1, 1])
    data.extend([0, 172, 16, 17, 1])
    data.extend([12, 10, 160, 10, 160, 10, 1])

    expected = [
        ("192.168.10.0", "255.255.255.0", "192.168.10.1"),
        ("8.8.8.8", "255.255.255.255", "1.1.1.1"),
        ("0.0.0.0", "0.0.0.0", "172.16.17.1"),
        ("10.160.0.0", "255.240.0.0", "10.160.10.1"),
    ]
    assert parse_classless_static_routes(data) == expected


def test_parse_inv_classless_static_routes():
    # invalid mask
    data = [33, 192, 168, 10, 192, 168, 10, 1]
    assert parse_classless_static_routes(data) is None
    # truncated gateway
    data = [24, 192, 168, 10, 192, 168, 10]
    assert parse_classless_static_routes(data) is None
    # extra data
    data = [24, 192, 168, 10, 192, 168, 10, 1, 1]
    assert parse_classless_static_routes(data) is None


def test_handle_dhcp_handshake(dhcprequest) -> None:
    success_mock = Mock()
    failure_mock = Mock()
    req = dhcprequest(
        cls=DhcpAddressInitialRequest,
        success_clb=success_mock,
        failure_clb=failure_mock,
        requestor=Mock(),
    )

    offer = DhcpPacket()
    offer.source_address = ("123.123.123.123", 67)
    req.handle_dhcp_offer(offer)

    packet = DhcpPacket()
    packet.AddLine("op: 2")
    packet.AddLine("domain_name: scc.kit.edu")
    packet.AddLine("yiaddr: 1.2.3.4")
    packet.AddLine("router: 2.3.4.5")
    packet.AddLine("subnet_mask: 255.255.255.0")
    packet.AddLine("domain_name_server: 1.0.0.0,2.0.0.0,3.0.0.0")
    packet.SetOption(
        "classless_static_route", bytes([0, 4, 0, 0, 0, 16, 10, 12, 5, 0, 0, 0])
    )
    packet.AddLine("ip_address_lease_time: 9000")
    packet.AddLine("renewal_time_value: 300")
    packet.AddLine("ip_address_lease_time: 9000")
    packet.AddLine("rebinding_time_value: 7000")
    packet.source_address = ("123.123.123.123", 67)

    print(packet.str())

    expected_res = {
        "dns": ["1.0.0.0", "2.0.0.0", "3.0.0.0"],
        "domain": "scc.kit.edu",
        "ip_address": "1.2.3.4",
        "gateway": "4.0.0.0",
        "static_routes": [("10.12.0.0", "255.255.0.0", "5.0.0.0")],
        "subnet_mask": "255.255.255.0",
        "lease_timeout": 1580009000,
        "renewal_timeout": 1580000300,
        "rebinding_timeout": 1580007000,
    }

    req.handle_dhcp_ack(packet)
    success_mock.assert_called_once_with(expected_res)


def test_dhcp_refresh(dhcprequest) -> None:
    req = dhcprequest(
        cls=DhcpAddressRefreshRequest, requestor=Mock(), client_ip="1.2.3.4"
    )
