import re
from typing import Optional

from django.utils.timezone import now

from eo_engine.common.contrib.waporv2 import (variables, well_known_bboxes,
                                              default_bbox, WAPORRemoteJob,
                                              WAPORRemoteVariable)
from eo_engine.common.time import runningdekad2date
from eo_engine.errors import AfriCultuReSMisconfiguration
from eo_engine.models import Credentials, EOSourceGroup
from eo_engine.models.eo_source import EOSource, EOSourceGroupChoices, EOSourceStateChoices


def wapor_from_filename(string, uuid: Optional[str] = None) -> WAPORRemoteVariable:
    m = re.compile(r'(?P<var_name>L[123]_[A-Z]+(_([A-Z]+))?_[DAME])_(?P<time_element>[0-9]+)(_(?P<area>[A-Z]+))?')
    match = m.match(string)
    if match is None:
        raise Exception(f'invalid name: {string}')
    groupdict = match.groupdict()

    variable_name: str = variables.get(match['var_name'])
    area: Optional[str] = groupdict.get('area')
    time_element: str = groupdict.get('time_element')
    if area:
        area = area.lower()
        bbox = well_known_bboxes[area]
    else:
        area = 'africa'
        bbox = default_bbox
    c = WAPORRemoteVariable(variable_name, bbox=bbox)
    c._area = area
    if uuid:
        c.ticket = uuid
    year = int(time_element[:2]) + 2000  # 1904 -> 2019, 04 DEKAD
    runningdekad = int(time_element[2:])
    start_date, end_date = runningdekad2date(year, runningdekad)
    c.start_date = start_date
    c.end_date = end_date
    return c


def from_eosource_url(url):
    match = re.match(r'wapor://(?P<uuid>[A-Za-z0-9-]+)$', url)
    if match:
        uuid = match.groupdict()['uuid']
        return WAPORRemoteJob.from_uuid(uuid)
    raise AfriCultuReSMisconfiguration('EOSource url was not recognised! :O')


def create_or_get_wapor_object_from_filename(filename: str) -> (EOSource, bool):
    """ Returns EOSource,Bool """

    wapor_variable = wapor_from_filename(filename)
    group = EOSourceGroup.objects.get(name__endswith=f'{wapor_variable.product_id}_{wapor_variable.area.upper()}')
    obj, created = EOSource.objects.get_or_create(
        filename=filename,
        defaults={
            'state': EOSourceStateChoices.AVAILABLE_REMOTELY,
            'domain': 'wapor',
            'datetime_seen': now(),
            'filesize_reported': 0,
            'reference_date': wapor_variable.start_date,
            'url': 'wapor://',
            'credentials': Credentials.objects.filter(domain='WAPOR').first()
        }
    )
    if created:
        obj.group.add(group)

    return obj, bool
