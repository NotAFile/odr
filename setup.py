#!/usr/bin/env python3

from setuptools import setup

setup(
    name = 'odr',
    py_modules = ['netdb_webapi'],
    version = '0.0.1',
    description = 'OpenVPN DHCP requestor',
    author='Klaras Armee',
    author_email='pony@kit.fail',
    url='https://git.scc.kit.edu/scc-net/odr',
    classifiers = [
        # Verhindert versehentliches hochladen auf öffentlichen Repos, z.B.
        # PyPi
        'Private :: Do not upload',
    ],
)
