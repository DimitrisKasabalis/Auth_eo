import json
from typing import Dict
from collections import namedtuple
from pathlib import Path

from eo_engine.models import Credentials


def get_auth(domain: str) -> (str, str):
    credentials = Credentials.objects.get(domain=domain)

    return credentials.username, credentials.password
