# coding: utf8
"""
syspad_monitor main module

:author: julien

"""

import logging
from os import path

import pkg_resources
from setuptools.config import read_configuration


def _extract_version(package_name):
    try:
        return pkg_resources.get_distribution(package_name).version
    except pkg_resources.DistributionNotFound:
        _conf = read_configuration(path.join(path.dirname(path.dirname(__file__)), "setup.cfg"))
        return _conf["metadata"]["version"]


__version__ = _extract_version("syspad_monitor")

logging.getLogger(__name__).addHandler(logging.NullHandler())

if __name__ == "__main__":
    print(__version__)
