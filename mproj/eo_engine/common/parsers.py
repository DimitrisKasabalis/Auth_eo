from datetime import datetime
from pathlib import Path
import string
import re

from dateutil.parser import parse

punctuation_chars = re.escape(string.punctuation)


def parse_dt_from_generic_string(timestr: str) -> datetime:
    timestr = timestr.lower()  # its a copy

    # The final path component, without its suffix:
    timestr = Path(timestr).stem

    # remove toxic sub-strings
    TOXIC_WORDS = [
        'hdf5',
    ]
    timestr = re.sub(r'[' + punctuation_chars + ']', ' ', timestr)
    for word in TOXIC_WORDS:
        timestr = timestr.replace(word, '')
    return parse(timestr, fuzzy=True)
