from pathlib import Path

import responses
from dateutil.relativedelta import relativedelta
from django.test import override_settings
from django.utils.timezone import now

from eo_engine.models import (EOSource, Credentials, EOSourceGroupChoices,
                              EOSourceStateChoices, EOProduct)
from . import BaseTest

TEST_MEDIA_ROOT = Path(r'D:\src\geoAuthPipe\test_MEDIA_ROOT')


# noinspection DuplicatedCode
@override_settings(MEDIA_ROOT=TEST_MEDIA_ROOT)
class TestUnit(BaseTest):

    def setUp(self) -> None:
        self.NOW = now()
        self.responses = responses.RequestsMock()
        self.responses.start()

        self.target_url = "https://land.copernicus.vgt.vito.be/" \
                          "PDF/datapool/Vegetation/Indicators/" \
                          "NDVI_300m_V2/" \
                          "2021/03/21/NDVI300_202103210000_GLOBE_OLCI_V2.0.1/" \
                          "c_gls_NDVI300_202103210000_GLOBE_OLCI_V2.0.1.nc"

        self.responses.add(responses.GET,
                           self.target_url,
                           body=Path(
                               r"D:\src\geoAuthPipe\test_RESPONSES\land.copernicus.vgt.vito.be\PDF\datapool\Vegetation\Indicators\NDVI_300m_V2\2021\03\21\NDVI300_202103210000_GLOBE_OLCI_V2.0.1\c_gls_NDVI300_202103210000_GLOBE_OLCI_V2.0.1.nc").read_bytes()
                           )

        self.eo_sourse = EOSource.objects.create(
            domain='land.copernicus.vgt.vito.be',
            filename='c_gls_NDVI300_202103210000_GLOBE_OLCI_V2.0.1.nc',
            status=EOSourceStateChoices.AvailableRemotely,
            url=self.target_url,
            credentials=Credentials.objects.first(),
            product=EOSourceGroupChoices.ndvi_300m_v2,
            datetime_uploaded=self.NOW - relativedelta(days=2),
            datetime_seen=self.NOW,
        )

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(TEST_MEDIA_ROOT / 'land.copernicus.vgt.vito.be')

    def test_download_file_stand_alone(self):
        from eo_engine.common import download_asset
        self.assertEqual(self.eo_sourse.state, EOSourceStateChoices.AvailableRemotely)
        r = download_asset(self.eo_sourse)
        self.assertEqual(self.eo_sourse.state, EOSourceStateChoices.AvailableLocally)

    def test_download_file_fro_object(self):
        self.assertEqual(self.eo_sourse.state, EOSourceStateChoices.AvailableRemotely)
        self.eo_sourse.download()
        self.assertEqual(self.eo_sourse.state, EOSourceStateChoices.AvailableLocally)
        self.assertEqual(EOProduct.objects.all().count(), 1)
        p = EOProduct.objects.first()
        self.assertEqual(p.inputs.count(), 1)
        self.assertGreater(self.eo_sourse.file.size, 0)

    def test_bake_product(self):
        self.eo_sourse.download()
        assert self.eo_sourse.file.size > 0
        p: EOProduct = EOProduct.objects.first()
        p.make_product(as_task=False)
        print('hello')
