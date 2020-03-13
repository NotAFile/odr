"""various parsers"""
import re
import logging

from typing import Dict, Optional


class ParseUsername:
    """Provides parsing of full username@realm usernames into their components.
    """

    USERNAME_RE = re.compile(
        r"^(?P<username>[^/@]+)(/(?P<resource>[^/@]+))?"
        r"(@((?P<domain>[^/@]+)/)?(?P<realm>[^/@]+))?$"
    )

    def __init__(self, default_realm: str) -> None:
        self._default_realm = default_realm
        self._log = logging.getLogger("parseusername")

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
        realm = match.group("realm")

        if realm is None:
            if self._default_realm is None:
                self._log.warning('username contains no realm: "%s"', full_username)
                return None
            self._log.debug(
                'no realm specified, using default realm "%s"', self._default_realm
            )
            realm = self._default_realm

        return {
            "username": match.group("username"),
            "resource": match.group("resource"),
            "domain": match.group("domain"),
            "realm": realm,
        }
