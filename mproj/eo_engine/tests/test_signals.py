from django.utils.timezone import now
from dateutil.relativedelta import relativedelta
from . import BaseTest

from eo_engine.models import EOSource, Credentials, EOSourceProductChoices, \
    EOSourceProductGroupChoices, EOSourceStatusChoices

from eo_engine.models import EOProduct, EOProductStatusChoices


class TestSignals(BaseTest):

    def setUp(self) -> None:
        self.NOW = now()

        c = Credentials.objects.first()

        self.eo_sourse = EOSource(
            domain='www.mock.site.gr',
            filename='c_gls_NDVI300_202103110000_GLOBE_OLCI_V2.0.1.nc',
            status=EOSourceStatusChoices.availableRemotely,
            url="https://www.mock.site.gr/assets/sat1/ndvi1/africa/c_gls_NDVI300_202103110000_GLOBE_OLCI_V2.0.1.nc",
            credentials=c,
            product=EOSourceProductChoices.ndvi_300m_v2,
            datetime_uploaded=self.NOW - relativedelta(days=2),
            datetime_seen=self.NOW,
            product_group=EOSourceProductGroupChoices.NDVI
        )

    def test_eo_source_post_save(self):
        # a remote source is found
        self.eo_sourse.save()
        self.assertEqual(len(EOProduct.objects.all()), 0)
        self.assertEqual(len(EOProduct.objects.filter(status=EOProductStatusChoices.AVAILABLE)), 0)

        #  and then becames available locally
        self.eo_sourse.status = EOSourceStatusChoices.availableLocally
        self.eo_sourse.save()
        self.assertEqual(len(EOProduct.objects.filter(status=EOProductStatusChoices.AVAILABLE)), 1)
