import json
from typing import Dict, NamedTuple
from collections import namedtuple
from pathlib import Path

from eo_engine.models import Credentials

credentials = NamedTuple('credentials', [('username', str), ('password', str)])


def get_credentials(domain: str) -> credentials:
    """ return stored credentials for the domain.
    Raises Credentials.DoesNotExist if credentials are missing"""
    obj = Credentials.objects.get(domain=domain)

    return credentials(obj.username, obj.password)
