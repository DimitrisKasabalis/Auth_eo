from django.utils.timezone import now

from eo_engine.models import EOProduct, EOProductStatusChoices
from eo_engine.models import EOSource, Credentials, EOSourceStatusChoices
from . import BaseTest


class TestSignals(BaseTest):
    @classmethod
    def setUpClass(cls):
        c = Credentials.objects.create(domain='asdf', username='test', password='test')
        cls.eo_sourse = EOSource.objects.create(
            domain='www.mock.site.gr',
            filename='c_gls_WB100_202105010000_GLOBE_S2_V1.0.1.nc',
            status=EOSourceStatusChoices.availableRemotely,
            url="test/testity.nc",
            credentials=c,
            product="c_gls_WB100-V1-GLOB",
            filesize_reported=10,
            # datetime_uploaded=self.NOW - relativedelta(days=2),
            datetime_seen=now(),
            # product_group=EOSourceProductGroupChoices.NDVI
        )

    def test_eo_source_post_save(self):
        # a remote source is found
        self.eo_sourse.save()
        self.assertEqual(len(EOProduct.objects.all()), 0)

        #  source becames
        self.eo_sourse.status = EOSourceStatusChoices.availableLocally
        self.eo_sourse.save()

        self.assertGreaterEqual(len(EOProduct.objects.filter(status=EOProductStatusChoices.Available)), 1)
