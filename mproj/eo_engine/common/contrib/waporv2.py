import re
from datetime import datetime
from functools import wraps
from typing import Optional, Dict, Type, TypeVar, Iterator, Union
from uuid import UUID

import requests
from dateutil.relativedelta import relativedelta
from requests import HTTPError

from eo_engine.errors import AfriCultuReSMisconfiguration
from .wapor2_wapor_data import cubes, WAPORVersion, Variable, BBox, default_wapor_version

_D = TypeVar("_D")


def requires_client(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._default_client is None:
            raise AfriCultuReSMisconfiguration('This method requires to setup WAPOR Client')
        return method(self, *args, **kwargs)

    return wrapper


def requires_login(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if isinstance(self, WAPORv2Client):
            self.login()
        elif isinstance(self, WAPORRemoteVariable):
            self._default_client.login()

        return method(self, *args, **kwargs)

    return wrapper


_products = [Variable(cube=cube) for cube in cubes]


def _build_index() -> Dict[str, Variable]:
    return dict((r.cube.code.upper(), r) for r in _products)


_by_name = _build_index()

variable_by_name = _by_name


class NotFound:
    pass


class _VariableLookup:
    @staticmethod
    def get(key: str, default=Type[NotFound]):
        key = key.upper()
        r = _by_name.get(key, default)

        if r == NotFound:
            raise KeyError(key)

        return r

    __getitem__ = get

    def __len__(self) -> int:
        return len(_products)

    def __iter__(self) -> Iterator[Variable]:
        return iter(_products)

    def __contains__(self, item: str) -> bool:
        try:
            self.get(item)
            return True
        except KeyError:
            return False


variables = _VariableLookup()

well_known_bboxes = {
    'africa': BBox(min_x=-30, min_y=-40, max_x=60, max_y=40, projection=4326),
    'ken': BBox(min_x=33.89, min_y=-4.625, max_x=41.917, max_y=4.625, projection=4326),
    'tun': BBox(min_x=7.497, min_y=30.219, max_x=11.583, max_y=37.345, projection=4326),
    'moz': BBox(min_x=30.208, min_y=-26.865, max_x=40.849, max_y=-10.467, projection=4326),
    'rwa': BBox(min_x=28.845, max_y=-1.052, max_x=30.894, min_y=-2.827, projection=4326),
    'eth': BBox(max_x=48.0, min_y=3.37, min_x=32.98, max_y=14.93, projection=4326),
    'gha': BBox(min_x=-3.25, max_y=12.16, max_x=2.205, min_y=4.7, projection=4326)

}
default_bbox = well_known_bboxes['africa']


class WAPORRemoteJob(object):

    @classmethod
    def from_uuid(cls, uuid, workspace_version: WAPORVersion = default_wapor_version):
        url = f'https://io.apps.fao.org/gismgr/api/v1/catalog/workspaces/{workspace_version.label}/jobs/{uuid}'
        response = requests.get(url)
        return cls(response.json())

    def __init__(self, wapor_raw_data):
        self._wapor_raw_data: dict = wapor_raw_data
        self._error_message: Optional[str] = None
        self._error: Optional[str] = None

    def __str__(self):
        return f"<{self.__class__.__name__}/{self.job_id}/{self.job_status}>"

    def refresh(self):
        url = f'https://io.apps.fao.org/gismgr/api/v1/catalog/workspaces/{self.workspace}/jobs/{self.job_id}'
        response = requests.get(url)
        self._wapor_raw_data = response.json()
        try:
            response.raise_for_status()
        except HTTPError:
            self._error = self._wapor_raw_data['error']
            self._error_message = self._wapor_raw_data['message']

        return self.job_status

    @property
    def _job_details(self):
        return self._wapor_raw_data.get('response', None)

    @property
    def response_status(self):
        return self._wapor_raw_data['status']

    def job_exists(self):
        return True if self.response_status == 200 else False

    @property
    def job_status(self) -> Optional[dict]:
        if self.job_exists():
            return self._job_details.get('status', 'UNKNOWN')
        return None

    def job_url(self):
        if self.job_exists():
            return self._job_details['links'][0]['href']  # self

    @property
    def workspace(self) -> str:
        if self.job_exists():
            return self._job_details['workspaceCode']

    def process_log(self):
        if self.job_exists():
            return self._job_details['log']

    # code
    @property
    def job_id(self) -> str:
        if self.job_exists():
            return self._job_details['code']
        elif self.response_status == 404:
            match = re.match(r'(?<=code=)(?P<job_id>[A-Za-z-].+),', self._error_message)
            return match.groupdict()['job_id']

    def download_url(self) -> Optional[str]:
        if self.job_exists() and self.job_status == 'COMPLETED':
            return self._job_details['output']['downloadUrl']


class WAPORv2Client(object):

    def __init__(self, api_key: Optional[str] = None):
        self._session = requests.Session()

        # auth details
        self._token_timestamp: Optional[int] = None  # unused for now
        self._token_expiration_timestamp: Optional[int] = None  # unsued for now
        self._api_key = api_key
        self._token: Optional[str] = None

        # update default headers
        self._session.headers.update({
            "Content-type": "application/json;charset=UTF-8",
            "Accept": "application/json"
        })

    @requires_login
    def submit_job(self, data: dict):
        query_endpoint = 'https://io.apps.fao.org/gismgr/api/v1/query'
        rs = self._session.post(url=query_endpoint, json=data)
        return rs

    def get(self, url, **kwargs):
        res = self._session.get(url, **kwargs)
        res.raise_for_status()
        return res

    @property
    def headers(self):
        return self._session.headers

    def login(self):
        # Login into WAPOR
        if self._api_key is None:
            raise AfriCultuReSMisconfiguration('Requires API KEY')

        sign_in_url = 'https://io.apps.fao.org/gismgr/api/v1/iam/sign-in'
        resp_vp = self._session.post(
            sign_in_url,
            headers={'X-GISMGR-API-KEY': self._api_key})
        # guard against erroneous return codes
        resp_vp.raise_for_status()
        data = resp_vp.json()
        self._token_timestamp = data['timestamp']
        self._token_expiration_timestamp = self._token_timestamp + data['response']['expiresIn']
        self._token = data['response']['accessToken']

        self.headers.update({
            "Authorization": "Bearer " + self._token,
        })

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def set_api_key(self, api_key: str):
        self._api_key = api_key


class WAPORRemoteVariable(object):
    _job_id: Optional[UUID] = None
    _default_client: Optional[WAPORv2Client]
    _latest_update: WAPORRemoteJob
    _time_element: str
    _start_date: datetime.date
    _end_date: datetime.date
    _bbox: BBox
    _area: str

    _measure: None

    def __init__(self, product: Union[Variable, str], bbox: BBox = default_bbox, api_key: Optional[str] = None):
        if isinstance(product, str):
            product = variables.get(product)
        self._product: Variable = product
        self._bbox = bbox
        if api_key:
            self._default_client = WAPORv2Client(api_key=api_key)
        else:
            self._default_client = WAPORv2Client()

    @property
    def dimension(self) -> str:
        return self._product.cube.dimensions[0].code

    @property
    def area(self) -> str:
        return self._area

    @property
    def product_id(self) -> str:
        return self._product.cube.code

    @property
    def bbox(self):
        return self._bbox

    @bbox.setter
    def bbox(self, value: BBox):
        self._bbox = value

    def set_api_key(self, api_key=None):
        self._default_client.set_api_key(api_key)

    @property
    def time_element(self):
        return self._time_element

    @time_element.setter
    def time_element(self, value):
        self._time_element = value

    @property
    def start_date(self):
        return self._start_date

    @start_date.setter
    def start_date(self, value: datetime.date):
        self._start_date = value

    @property
    def end_date(self):
        return self._end_date

    @end_date.setter
    def end_date(self, value: datetime.date):
        self._end_date = value

    @property
    def ticket(self):
        return self._job_id

    @ticket.setter
    def ticket(self, value):
        self._job_id = value

    @requires_client
    def update_remote_status(self) -> bool:
        url = f'https://io.apps.fao.org/gismgr/api/v1/catalog/workspaces/{self._product.workspace.label}/jobs/{self.ticket}'
        res = self._default_client.get(url)
        self._latest_update = WAPORRemoteJob(res.json())
        return True

    @requires_login
    @requires_client
    def submit(self) -> WAPORRemoteJob:
        payload = self.payload_factory()
        print(payload)
        rv = self._default_client.submit_job(payload)
        rv.raise_for_status()

        return WAPORRemoteJob(rv.json())

    def payload_factory(self) -> Dict:
        self._require_start_end_dates()
        start_date = self.start_date
        end_date = self.end_date + relativedelta(days=1)
        return {
            "type": "CropRaster",
            "params": {
                "properties": {
                    "outputFileName": "%s_clipped.tif" % self._product.cube.code,
                    "cutline": True,
                    "tiled": True,
                    "compressed": True,
                    "overviews": True
                },
                "cube": {
                    "code": self._product.cube.code,
                    "workspaceCode": self._product.workspace.label,
                    "language": "en"
                },
                "dimensions": [
                    {
                        "code": self._product.cube.dimensions[0].code,
                        "values": [
                            f"[{start_date},{end_date})"
                        ]
                    }
                ],
                "measures": [
                    self._product.cube.measure[0].code
                ],
                "shape": self.bbox.as_shape()
            }
        }

    def __str__(self):
        return f'<{self.__class__.__name__}/{self._product.cube}>'

    def _require_start_end_dates(self):
        if self._start_date is None and self._end_date is None:
            raise AfriCultuReSMisconfiguration('start/end date needs to be set')
