import re
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Optional, NamedTuple, Literal, Dict, Type, TypeVar, Iterator, Union
from uuid import UUID

import requests
from requests import HTTPError

from eo_engine.common.time import yearly_dekad_to_wapor_range
from eo_engine.errors import AfriCultuReSMisconfiguration

_D = TypeVar("_D")
WAPORVersion = NamedTuple('WAPORVersion', [('version', int), ('label', str)])
wapor_v1 = WAPORVersion(1, 'WAPOR')
wapor_v2 = WAPORVersion(2, 'WAPOR_2')
default_wapor_version = wapor_v2


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


@dataclass
class Variable(object):
    id: str
    description: str
    measure: str
    dimension: Literal['YEAR', 'MONTH', 'DAY', 'DEKAD']

    @property
    def level(self) -> int:
        return int(self.id.split('_')[0][-1])

    @property
    def workspace(self) -> WAPORVersion:
        if self.level == 'L3':
            return wapor_v1
        return wapor_v2


_products = [
    Variable('L1_GBWP_A', 'Gross Biomass Water Productivity', 'WPR', 'YEAR'),
    Variable('L1_NBWP_A', 'Net Biomass Water Productivity', 'WPR', 'YEAR'),
    Variable('L1_AETI_A', 'Actual EvapoTranspiration and Interception (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L1_AETI_M', 'Actual EvapoTranspiration and Interception (Monthly)', 'WATER_MM', 'MONTH'),
    Variable('L1_AETI_D', 'Actual EvapoTranspiration and Interception (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L1_T_A', 'Transpiration (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L1_E_A', 'Evaporation (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L1_I_A', 'Interception (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L1_T_D', 'Transpiration (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L1_E_D', 'Evaporation (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L1_I_D', 'Interception (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L1_NPP_D', 'Net Primary Production', 'NPP', 'DEKAD'),
    Variable('L1_TBP_A', 'Total Biomass Production (Annual)', 'LPR', 'YEAR'),
    Variable('L1_LCC_A', 'Land Cover Classification', 'LCC', 'YEAR'),
    Variable('L1_RET_A', 'Reference EvapoTranspiration (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L1_PCP_A', 'Precipitation (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L1_RET_M', 'Reference EvapoTranspiration (Monthly)', 'WATER_MM', 'MONTH'),
    Variable('L1_PCP_M', 'Precipitation (Monthly)', 'WATER_MM', 'MONTH'),
    Variable('L1_RET_D', 'Reference EvapoTranspiration (Dekadal)', 'WATER_MM', 'MONTH'),
    Variable('L1_PCP_D', 'Precipitation (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L1_RET_E', 'Reference EvapoTranspiration (Daily)', 'WATER_MM', 'DAY'),
    Variable('L1_PCP_E', 'Precipitation (Daily)', 'WATER_MM', 'DAY'),
    Variable('L1_QUAL_NDVI_D', 'Quality of Normalized Difference Vegetation Index (Dekadal)', 'N_DEKADS', 'DEKAD'),
    Variable('L1_QUAL_LST_D', 'Quality Land Surface Temperature (Dekadal)', 'N_DAYS', 'DEKAD'),
    # 'L2_GBWP_S': 'Gross Biomass Water Productivity (Seasonal)',
    Variable('L2_AETI_A', 'Actual EvapoTranspiration and Interception (Annual)', 'WATER_MM', 'YEAR', ),
    Variable('L2_AETI_M', 'Actual EvapoTranspiration and Interception (Monthly)', 'WATER_MM', 'MONTH'),
    Variable('L2_AETI_D', 'Actual EvapoTranspiration and Interception (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L2_T_A', 'Transpiration (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L2_E_A', 'Evaporation (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L2_I_A', 'Interception (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L2_T_D', 'Transpiration (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L2_E_D', 'Evaporation (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L2_I_D', 'Interception (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L2_NPP_D', 'Net Primary Production', 'WATER_MM', 'DEKAD'),
    # 'L2_TBP_S': 'Total Biomass Production (Seasonal)',
    Variable('L2_LCC_A', 'Land Cover Classification', 'LCC', 'YEAR'),
    # 'L2_PHE_S': 'Phenology (Seasonal)',
    Variable('L2_QUAL_NDVI_D', 'Quality of Normalized Difference Vegetation Index (Dekadal)', 'N_DEKADS',
             'DEKAD'),
    Variable('L2_QUAL_LST_D', 'Quality Land Surface Temperature (Dekadal)', 'N_DAYS', 'DEKAD'),
    Variable('L3_AETI_A', 'Actual EvapoTranspiration and Interception (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L3_AETI_M', 'Actual EvapoTranspiration and Interception (Monthly)', 'WATER_MM', 'MONTH'),
    Variable('L3_AETI_D', 'Actual EvapoTranspiration and Interception (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L3_T_A', 'Transpiration (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L3_E_A', 'Evaporation (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L3_I_A', 'Interception (Annual)', 'WATER_MM', 'YEAR'),
    Variable('L3_T_D', 'Transpiration (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L3_E_D', 'Evaporation (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L3_I_D', 'Interception (Dekadal)', 'WATER_MM', 'DEKAD'),
    Variable('L3_NPP_D', 'Net Primary Production', 'NPP', 'DEKAD'),
    Variable('L3_QUAL_NDVI_D', 'Quality of Normalized Difference Vegetation Index (Dekadal)', 'N_DEKADS',
             'DEKAD'),
    Variable('L3_QUAL_LST_D', 'Quality Land Surface Temperature (Dekadal)', 'N_DAYS', 'DEKAD'),
    Variable('L3_LCC_A', 'Land Cover Classification', 'LCC', 'YEAR')
]


def _build_index(idx: int) -> Dict[str, Variable]:
    return dict((r.id.upper(), r) for r in _products)


_by_name = _build_index(0)

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


@dataclass
class BBox:
    min_x: float
    min_y: float

    max_x: float
    max_y: float

    projection: int = 4326

    def as_shape(self):
        return {
            "crs": "EPSG:%d" % self.projection,
            "type": "Polygon",
            "coordinates": [
                [
                    [
                        self.min_x,
                        self.max_y
                    ],
                    [
                        self.max_x,
                        self.max_y
                    ],
                    [
                        self.max_x,
                        self.min_y
                    ],
                    [
                        self.min_x,
                        self.min_y
                    ],
                    [
                        self.min_x,
                        self.max_y
                    ]
                ]
            ]
        }


well_known_bbox = {
    'africa': BBox(min_x=-30, min_y=-40, max_x=60, max_y=40, projection=4326),
    'ken': BBox(min_x=33.89, min_y=-4.625, max_x=41.917, max_y=4.625, projection=4326),
    'tun': BBox(min_x=7.497, min_y=30.219, max_x=11.583, max_y=37.345,projection=4326),
    'moz': BBox(min_x=30.208, min_y=-26.865, max_x=40.849, max_y=-10.467,projection=4326),
    'rwa': BBox(min_x=28.845, max_y=-1.052, max_x=30.894, min_y=-2.827,projection=4326),
    'eth': BBox(max_x=48.0, min_y=3.37, min_x=32.98, max_y=14.93,projection=4326),
    'gaf': BBox(min_x=-3.25, max_y=12.16, max_x=2.205, min_y=4.7,projection=4326)

}
default_bbox = well_known_bbox['africa']


class WAPORRemoteJob(object):

    @classmethod
    def from_eosource_url(cls, url):
        match = re.match(r'wapor://(?P<uuid>[A-Za-z0-9-]+)$', url)
        if match:
            uuid = match.groupdict()['uuid']
            return cls.from_uuid(uuid)
        raise AfriCultuReSMisconfiguration('EOSource url was not recognised! :O')

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

    @classmethod
    def from_filename(cls, string, uuid: Optional[str] = None):
        m = re.compile(r'(?P<var_name>L[123]_[A-Z]+(_([A-Z]+))?_[DAME])_(?P<time_element>[0-9]+)(_(?P<area>[A-Z]+))?')
        match = m.match(string)
        if match is None:
            raise Exception(f'invalid name: {string}')
        groupdict = match.groupdict()
        var = variables.get(match['var_name'])
        area = groupdict.get('area')
        time_element = groupdict.get('time_element')
        if area:
            area = area.lower()
            bbox = well_known_bbox[area]
        else:
            area = 'africa'
            bbox = default_bbox
        c = cls(var, bbox=bbox)
        c._area = area
        if uuid:
            c.ticket = uuid
        if time_element:
            c.time_element = time_element
            c._set_start_end_date()
        return c

    _job_id: Optional[UUID] = None
    _default_client: Optional[WAPORv2Client]
    _latest_update: WAPORRemoteJob
    _time_element: str
    _start_date: Optional[datetime] = None
    _end_date: Optional[datetime] = None
    _bbox: BBox
    _area: str

    def __init__(self, product: Union[Variable, str], bbox: BBox = default_bbox, api_key: Optional[str] = None):
        if isinstance(product, str):
            product = variables.get(product)
        self._product = product
        self._bbox = bbox
        if api_key:
            self._default_client = WAPORv2Client(api_key=api_key)
        else:
            self._default_client = WAPORv2Client()

    @property
    def dimension(self) -> str:
        return self._product.dimension

    @property
    def area(self) -> str:
        return self._area

    @property
    def product_id(self) -> str:
        return self._product.id

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

    def _set_start_end_date(self):

        frequency = self._product.dimension
        if self.time_element is None:
            return

        # not sure how this works
        if frequency == 'DEKAD':
            start_date, end_date = yearly_dekad_to_wapor_range(self.time_element)
            self._start_date = start_date
            self._end_date = end_date
        else:
            raise NotImplemented('not implemented start/end for this frequency')

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
        return {
            "type": "CropRaster",
            "params": {
                "properties": {
                    "outputFileName": "%s_clipped.tif" % self._product.id,
                    "cutline": True,
                    "tiled": True,
                    "compressed": True,
                    "overviews": True
                },
                "cube": {
                    "code": self._product.id,
                    "workspaceCode": self._product.workspace.label,
                    "language": "en"
                },
                "dimensions": [
                    {
                        "code": self._product.dimension,
                        "values": [
                            "[{start_date},{end_date})".format(
                                start_date=self._start_date.date().isoformat(),
                                end_date=self._end_date.date().isoformat()
                            )
                        ]
                    }
                ],
                "measures": [
                    self._product.measure
                ],
                "shape": self.bbox.as_shape()
            }
        }

    def __str__(self):
        return f'{self.__class__.__name__} - {self._product.id}'

    def _require_start_end_dates(self):
        if self._start_date is None and self._end_date is None:
            raise AfriCultuReSMisconfiguration('start/end date needs to be set')
