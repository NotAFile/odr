# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:

# dhcprequestor.py -- Requests an IP address on behalf of a given MAC address.
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

import random
import time
import logging

from typing import Dict, Optional, Tuple, List, Callable, Iterable, Any
from ipaddress import IPv4Address, IPv4Network

from odr.listeningsocket import ListeningSocket
from odr.timeoutmgr import TimeoutManager, TimeoutObject

from pydhcplib.dhcp_packet import DhcpPacket


DHCP_SUBOPTION_LINKSEL = 5
DHCP_SUBOPTION_LINKSEL_LEN = 4


class DhcpAddressRequest:
    """Represents the request for an IP address (and additional settings
    relevant for the target network) based on a MAC address.

    To perform the above task, DHCP packets are sent and received.  For each
    packet, the class pretends to be a DHCP relay so that all answers can be
    received and responded to, although the requested IP address is completely
    different from the one that the packets are received on.

    The DHCP requests are targeted at specific DHCP server IP addresses.

    As soon as the request has completed, has failed or has timed out, the
    apropriate call back handler is called.
    """

    AR_DISCOVER = 1
    AR_REQUEST = 2

    def __init__(
        self,
        *,
        log: logging.Logger,
        requestor: "DhcpAddressRequestor",
        timeout_mgr: TimeoutManager,
        success_handler_clb: Callable[[Dict], None],
        failure_handler_clb: Callable[[], None],
        local_ip: str,
        client_identifier: str,
        server_ips: Iterable[str],
        target_addr: str = None,
        max_retries: int = 3,
        timeout: int = 4,
        lease_time: int = None
    ) -> None:
        """Sets up the address request.

        Creates a new XID.  Each address request has such a unique (randomly
        chosen) identifier.

        :ivar log: Instance of the logger for this class.
        :ivar requestor: Instance of the requestor, where the request is
                tracked and where the listening socket is maintained.
        :ivar timeout_mgr: Instance of the timeout manager.
        :ivar success_handler_clb: Call-back that is called as soon as the
                request has succeeded.
        :ivar failure_handler_clb: Call-back that is called as soon as the
                request has failed or timed out.
        :ivar local_ip: IP address from which all DHCP requests originate and
                on which the responses are received.  Is used within the DHCP
                packets.
        :ivar client_identifier: The client identifier which will represent the
                client for which an IP address is requested.
        :ivar server_ips: A list of IP addresses to which the DHCP requests
                should be sent.
        :ivar target_addr: An address specifying the subnet to send the reqest
                for, which will be sent to the DHCP server using the link
                selection option descriped in RFC 3527
        :ivar max_retries: The maximum number of retries after timeouts.
                Defaults to 2 retries.
        :ivar timeout: Number of seconds to wait for a DHCP response before
                timing out and/or retrying the request.  Defaults to 5 seconds.
        :ivar lease_time: DHCP lease time we would like to have. Defaults to
                None, meaning no specific lease time is requested.
        """
        self._log = log
        self._requestor = requestor
        self._timeout_mgr = timeout_mgr
        self._success_handler = success_handler_clb
        self._failure_handler = failure_handler_clb
        self._local_ip = IPv4Address(local_ip)
        self._client_identifier = client_identifier.encode("utf-8")
        self._server_ips = [IPv4Address(ip) for ip in server_ips]
        self._target_addr = IPv4Address(target_addr) if target_addr else None
        self._max_retries = max_retries
        self._initial_timeout = timeout
        self._lease_time = lease_time

        self._start_time = int(time.time())

        self._xid = random.randint(0, (2 ** 32) - 1)

        # Current packet state.
        self._state = None  # type: Optional[int]

        # What's the current timeout?  (Will be increased after each timeout
        # event.)
        self._timeout = self._initial_timeout
        # When will the packet time out?
        self._timeout_obj = None  # type: Optional[TimeoutObject]
        # What was the contents of the last packet?  (Used for retry.)
        self._last_packet = None  # type: Optional[DhcpPacket]
        # Number of packet retries
        self._packet_retries = 0

    def __del__(self) -> None:
        self._log.debug('xid %d destroyed', self.xid)

    @property
    def xid(self) -> int:
        """:returns: the unique identifier of the DHCP request.
        """
        return self._xid

    def _generate_base_packet(self) -> DhcpPacket:
        packet = DhcpPacket()
        packet.AddLine("op: BOOTREQUEST")
        packet.AddLine("htype: 1")
        packet.SetOption("xid", self._xid.to_bytes(4, "big"))

        # We're the gateway.
        packet.SetOption("giaddr", self._local_ip.packed)

        if self._target_addr:
            packet.SetOption(
                "relay_agent",
                bytes((DHCP_SUBOPTION_LINKSEL, DHCP_SUBOPTION_LINKSEL_LEN))
                + self._target_addr.packed,
            )

        # Request IP address, etc. for the following client identifier.
        packet.SetOption("client_identifier", self._client_identifier)

        # We pretend to be a gateway, so the packet hop count is > 0 here.
        packet.AddLine("hops: 1")

        return packet

    def _add_option_list(self, packet: DhcpPacket) -> None:
        # 'classless_static_route' must be requested before 'router'.
        packet.AddLine(
            "parameter_request_list: subnet_mask,"
            "classless_static_route,router,"
            "domain_name_server,domain_name,renewal_time_value,"
            "rebinding_time_value"
        )

    def _set_lease_time(self, packet: DhcpPacket) -> None:
        if self._lease_time is None:
            return
        packet.SetOption('ip_address_lease_time', self._lease_time.to_bytes(4, "big"))

    def _retrieve_server_ip(self, packet: DhcpPacket) -> None:
        """In case we're sending the requests to more than one DHCP server,
        attempt to determine which DHCP server answered, so that we can restrict
        our future requests to only one server.
        """
        if len(self._server_ips) > 1:
            self._log.debug("Attempting to find server ip")
            try:
                self._server_ips = [IPv4Address(packet.GetOption('server_identifier'))]
            except Exception:
                self._log.exception("invalid server ip response")
            else:
                # We were able to determine a single DHCP server with which we
                # will communicate from now on.
                self._log.debug("Found server ip %s", self._server_ips[0])

    def _send_packet(self, packet: DhcpPacket) -> None:
        """Method to initially send a packet.
        """
        self._last_packet = packet
        self._packet_retries = 0
        self._timeout = self._initial_timeout
        self._send_to_server(packet)

    def _resend_packet(self) -> None:
        """Method to re-send the packet that was sent last.
        """
        assert self._last_packet is not None
        self._packet_retries += 1
        self._timeout *= 2
        self._send_to_server(self._last_packet)

    def _send_to_server(self, packet: DhcpPacket) -> None:
        """Method that does the actual packet sending.  The packet is sent once
        for each DHCP server destination.
        """
        randomised_timeout = self._timeout + random.uniform(-1, 1)
        self._log.debug('timeout for xid %d is %ds', self.xid, randomised_timeout)
        timeout_time = time.time() + randomised_timeout
        self._timeout_obj = TimeoutObject(timeout_time, self.handle_timeout)
        self._timeout_mgr.add_timeout_object(self._timeout_obj)
        for server_ip in self._server_ips:
            self._log.debug(
                "Sending packet in state %s to %s [%d/%d]",
                self._state,
                server_ip,
                self._packet_retries + 1,
                self._max_retries + 1,
            )
            self._requestor.send_packet(packet, str(server_ip), 67)

    def _valid_source_address(self, packet: DhcpPacket) -> bool:
        if not packet.source_address:
            return False
        ip_address_str, port = packet.source_address  # type: (str, int)
        ip_address = IPv4Address(ip_address_str)
        if port != 67:
            self._log.debug(
                "dropping packet from wrong port: %s:%d", packet.source_address, port
            )
            return False
        if ip_address not in self._server_ips:
            self._log.debug(
                "dropping packet from wrong IP address: %s:%d",
                packet.source_address,
                port,
            )
            return False
        return True

    def handle_dhcp_offer(self, offer_packet: DhcpPacket) -> None:
        """Called by the requestor as soon as a DHCP OFFER packet is received
        for our XID.

        In case the packet matches what we currently expect, the packet is
        parsed and a matching DHCP REQUEST packet is generated.
        """
        if self._state != self.AR_DISCOVER:
            self._log.debug("received unsolicited offer")
            return
        self._log.debug("Received offer")
        if not self._valid_source_address(offer_packet):
            return
        if self._timeout_obj:
            self._timeout_mgr.del_timeout_object(self._timeout_obj)
        req_packet = self._generate_request(offer_packet)
        self._retrieve_server_ip(req_packet)
        self._state = self.AR_REQUEST
        self._send_packet(req_packet)

    def handle_dhcp_ack(self, packet: DhcpPacket) -> None:
        """Called by the requestor as soon as a DHCP ACK packet is received for
        our XID.

        In case the packet matches what we currently expect, the packet is
        parsed and the success handler called.

        The request instance (self) is removed from the requestor and will
        therefore be destroyed soon.
        """
        if self._state != self.AR_REQUEST:
            return
        self._log.debug("Received ACK")
        if not self._valid_source_address(packet):
            return
        if self._timeout_obj:
            self._timeout_mgr.del_timeout_object(self._timeout_obj)
        self._requestor.del_request(self)
        result = {}  # type: Dict[str, Any]
        result['domain'] = packet.GetOption('domain_name').decode("ascii")

        translate_ips = {
            'yiaddr': 'ip_address',
            'subnet_mask': 'subnet_mask',
            'router': 'gateway',
        }
        for opt_name in translate_ips:
            if not packet.IsOption(opt_name):
                continue
            val = packet.GetOption(opt_name)
            if len(val) == 4:
                result[translate_ips[opt_name]] = str(IPv4Address(val))

        dns = []  # type: List[str]
        result['dns'] = dns
        dns_list = packet.GetOption('domain_name_server')
        while len(dns_list) >= 4:
            dns.append(str(IPv4Address(dns_list[:4])))
            dns_list = dns_list[4:]

        if packet.IsOption('classless_static_route'):
            static_routes = parse_classless_static_routes(
                list(packet.GetOption('classless_static_route'))
            )
            if static_routes is not None:
                if 'gateway' in result:
                    # We MUST ignore a regular default route if static routes
                    # are sent.
                    del result['gateway']
                # Find and filter out default route (if any).  And set it as
                # the new gateway parameter.
                result['static_routes'] = []
                for network, netmask, gateway in static_routes:
                    if network == '0.0.0.0' and netmask == '0.0.0.0':
                        result['gateway'] = gateway
                    else:
                        result['static_routes'].append((network, netmask, gateway))
            del static_routes

        # Calculate lease timeouts (with RFC T1/T2 if not found in packet)
        lease_delta = int.from_bytes(packet.GetOption('ip_address_lease_time'), "big")
        result['lease_timeout'] = self._start_time + lease_delta
        if packet.IsOption('renewal_time_value'):
            renewal_delta = int.from_bytes(
                packet.GetOption('renewal_time_value'), "big"
            )
        else:
            renewal_delta = int(lease_delta * 0.5) + random.randint(-5, 5)
        result['renewal_timeout'] = self._start_time + renewal_delta
        if packet.IsOption('rebinding_time_value'):
            rebinding_delta = int.from_bytes(
                packet.GetOption('rebinding_time_value'), "big"
            )
        else:
            rebinding_delta = int(lease_delta * 0.875) + random.randint(-5, 5)
        result['rebinding_timeout'] = self._start_time + rebinding_delta

        self._success_handler(result)

    def handle_dhcp_nack(self, packet: DhcpPacket) -> None:
        """Called by the requestor as soon as a DHCP NACK packet is received for
        our XID.

        In case the packet matches what we currently expect, the failure handler
        is called.

        The request instance (self) is removed from the requestor and will
        therefore be destroyed soon.
        """
        if self._state != self.AR_REQUEST:
            return
        self._log.debug("Received NACK")
        if not self._valid_source_address(packet):
            return
        if self._timeout_obj:
            self._timeout_mgr.del_timeout_object(self._timeout_obj)
        self._requestor.del_request(self)
        self._failure_handler()

    def handle_timeout(self) -> None:
        """Called in case the timeout_time has passed without a proper DHCP
        response.  Handles resend attempts up to a certain maximum number of
        retries.

        In case the maximum number of retries have been attempted, the failure
        handler is called.  Additionally, the request instance (self) is removed
        from the requestor and will therefore be destroyed soon.
        """
        self._log.debug("handling timeout for %d", self.xid)
        if self._packet_retries >= self._max_retries:
            self._requestor.del_request(self)
            self._log.debug("Timeout for reply to packet in state %s", self._state)
            self._failure_handler()
        elif self._last_packet is not None:
            self._resend_packet()

    def _generate_request(self, offer_packet: DhcpPacket) -> DhcpPacket:
        """Generates a DHCP REQUEST packet.
        """
        raise NotImplementedError('Method _generate_request not implemented')


class DhcpAddressInitialRequest(DhcpAddressRequest):
    """
    """

    def __init__(self, **kwargs) -> None:
        """Sets up the initial address request.

        See :meth:`DhcpAddressRequest.__init__` for further parameters.
        """
        DhcpAddressRequest.__init__(
            self, log=logging.getLogger('dhcpaddrinitreq'), **kwargs
        )

        self._log.debug('initial request with xid %d created', self.xid)
        self._state = self.AR_DISCOVER

        self._requestor.add_request(self)
        self._send_packet(self._generate_discover())

    def _generate_discover(self) -> DhcpPacket:
        """Generates a DHCP DISCOVER packet.
        """
        packet = self._generate_base_packet()
        packet.AddLine("dhcp_message_type: DHCP_DISCOVER")
        self._add_option_list(packet)
        self._set_lease_time(packet)
        return packet

    def _generate_request(self, offer_packet: DhcpPacket) -> DhcpPacket:
        """Generates a DHCP REQUEST packet.
        """
        packet = self._generate_base_packet()
        packet.AddLine("dhcp_message_type: DHCP_REQUEST")
        self._add_option_list(packet)
        self._set_lease_time(packet)
        for opt in ["server_identifier"]:
            packet.SetOption(opt, offer_packet.GetOption(opt))
        packet.SetOption("request_ip_address", offer_packet.GetOption("yiaddr"))
        return packet


class DhcpAddressRefreshRequest(DhcpAddressRequest):
    """
    """

    def __init__(self, client_ip: str, **kwargs) -> None:
        """Sets up the address request.

        See :meth:`DhcpAddressRequest.__init__` for further parameters.
        """
        super().__init__(log=logging.getLogger('dhcpaddrrefreshreq'), **kwargs)

        self._client_ip = IPv4Address(client_ip)
        self._log.debug('refresh request with xid %d created', self.xid)

        self._state = self.AR_REQUEST

        self._requestor.add_request(self)
        self._send_packet(self._generate_refresh_request())

    def _generate_refresh_request(self) -> DhcpPacket:
        """Generates a DHCP REQUEST packet.
        """
        packet = self._generate_base_packet()
        packet.AddLine("dhcp_message_type: DHCP_REQUEST")
        self._add_option_list(packet)
        self._set_lease_time(packet)
        packet.SetOption("request_ip_address", self._client_ip.packed)
        return packet


class DhcpAddressRequestor(ListeningSocket):
    """A DhcpAddressRequestor instance represents a UDP socket listening for
    DHCP responses on a specific IP address and port on a specific network
    device.

    Specific requests are added to a requestor instance and use the requestor
    to send DHCP requests.  The requestor maps DHCP responses back to a specific
    request via the request's XID.

    Provides attribute listen_device, listen_address (through its super-class
    ListeningSocket) and add_request method for use by the requestor manager.

    Provides socket (through its super-class ListeningSocket) and handle_socket
    for use by the socket loop.
    """

    # Maps dhcp_message_type to a request's message type handler.
    _DHCP_TYPE_HANDLERS = {
        2: 'handle_dhcp_offer',
        5: 'handle_dhcp_ack',
        6: 'handle_dhcp_nack',
    }

    def __init__(
        self, listen_address: str = '', listen_port: int = 67, listen_device: str = None
    ) -> None:
        """\
        :ivar listen_address: IP address as string to listen on.
        :ivar listen_port: Local DHCP listening port. Defaults to 67.
        :ivar listen_device: Device name to bind to.
        """
        self._log = logging.getLogger('dhcpaddrrequestor')
        self._requests = {}  # type: Dict[int, DhcpAddressRequest]

        super().__init__(listen_address, listen_port, listen_device)

        self._log.debug(
            'listening on %s:%d@%s for DHCP responses',
            self.listen_address,
            self.listen_port,
            self.listen_device,
        )

    def add_request(self, request: DhcpAddressRequest) -> None:
        """Adds a new DHCP address request to this requestor.

        :ivar request: The request that should be added.
        """
        self._log.debug("adding xid %d", request.xid)
        self._requests[request.xid] = request

    def del_request(self, request: DhcpAddressRequest) -> None:
        """Removes a DHCP address request that was previously added.

        :ivar request: The request that should be removed.
        """
        self._log.debug("deleting xid %d", request.xid)
        del self._requests[request.xid]

    def handle_socket(self) -> None:
        """Retrieves the next, waiting DHCP packet, parses it and calls the
        handler of the associated request.
        """
        try:
            data, source_address = self._socket.recvfrom(2048)
            if len(data) == 0:
                self._log.warning("unexpectedly received EOF!")
                return
            packet = DhcpPacket()
            packet.source_address = source_address
            packet.DecodePacket(data)

            if (not packet.IsDhcpPacket()) or (
                not packet.IsOption("dhcp_message_type")
            ):
                self._log.debug("Ignoring invalid packet")
                return

            dhcp_type = packet.GetOption("dhcp_message_type")[0]
            if dhcp_type not in self._DHCP_TYPE_HANDLERS:
                self._log.debug("Ignoring packet of unexpected DHCP type %d", dhcp_type)
                return

            xid = int.from_bytes(packet.GetOption('xid'), "big")
            if xid not in self._requests:
                self._log.debug("Ignoring answer with xid %r", xid)
                return

            request = self._requests[xid]
            clb_name = self._DHCP_TYPE_HANDLERS[dhcp_type]
            if not hasattr(request, clb_name):
                self._log.error("request has no callback '%s'", clb_name)
                return

            clb = getattr(request, clb_name)
            clb(packet)
        except Exception:
            self._log.exception('handling DHCP packet failed')

    def send_packet(self, packet: DhcpPacket, dest_ip: str, dest_port: int) -> None:
        data = packet.EncodePacket()
        self.socket.sendto(data, (dest_ip, dest_port))


class DhcpAddressRequestorManager:
    """Holds a list of all available requestors.  Not much more than a
    dictionary with added error detection.
    """

    def __init__(self) -> None:
        self._requestors_by_device_and_ip = (
            {}
        )  # type: Dict[Tuple[Optional[str], str], DhcpAddressRequestor]
        self._log = logging.getLogger('dhcpaddrrequestormgr')

    def add_requestor(self, requestor: DhcpAddressRequestor) -> None:
        """
        :ivar requestor: Instance of a requestor that should be added to
                the list of known requestors.
        """
        listen_pair = (requestor.listen_device, requestor.listen_address)
        if listen_pair in self._requestors_by_device_and_ip:
            self._log.error(
                'attempt to listen on IP %s@%s multiple times',
                requestor.listen_address,
                requestor.listen_device,
            )
            return
        self._requestors_by_device_and_ip[listen_pair] = requestor

    def has_requestor(self, device: str, local_ip: str) -> bool:
        """:returns: True if the device and local_ip already has a requestor.
        """
        return (device, local_ip) in self._requestors_by_device_and_ip

    def get_requestor(
        self, device: str, local_ip: str
    ) -> Optional[DhcpAddressRequestor]:
        """
        :returns: the requestor matching the device and local_ip, or
                  None in case there is none.
        """
        listen_pair = (device, local_ip)
        if listen_pair not in self._requestors_by_device_and_ip:
            self._log.error('request for unsupported local IP %s@%s', local_ip, device)
            return None
        return self._requestors_by_device_and_ip[listen_pair]


def parse_classless_static_routes(
    data: List[int],
) -> Optional[List[Tuple[str, str, str]]]:
    """Parses an array of ints, representing classless static routes according
    to RFC 3442, into a list of tuples with full IP addresses.

    :returns: a tuple consisting of network, netmask and router.
    """
    routes = []
    remaining = data[:]
    while len(remaining) >= 5:
        mask_width = remaining.pop(0)

        significant_octets = (mask_width - 1) // 8 + 1
        if significant_octets > 4:
            # Invalid number of octets.
            return None

        network_addr = bytes(remaining[:significant_octets]).ljust(4, b"\x00")
        remaining = remaining[significant_octets:]

        network = IPv4Network((network_addr, mask_width))

        gateway = bytes(remaining[:4])
        remaining = remaining[4:]

        if len(gateway) != 4:
            # List too short, malformed gateway.
            return None
        routes.append(
            (
                str(network.network_address),
                str(network.netmask),
                str(IPv4Address(gateway)),
            )
        )

    if len(remaining) > 0:
        # Failed to properly parse the option.
        return None
    return routes
