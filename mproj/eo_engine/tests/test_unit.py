from pathlib import Path

import responses
from dateutil.relativedelta import relativedelta
from django.test import override_settings, TransactionTestCase, TestCase
from django.utils.timezone import (
    now,
    utc as utc_tz
)

from datetime import datetime

from eo_engine.models import (
    EOSource,
    Credentials,
    EOSourceGroupChoices,
    EOSourceStateChoices,
    EOProductGroupChoices,
    EOProduct,
    EOProductGroup,
    EOSourceGroup,
    Pipeline,
)

from . import BaseTest


# A TransactionTestCase resets the
#   database after the test runs by truncating all tables.
#   A TransactionTestCase may call commit and rollback and
#   observe the effects of these calls on the database.
#
# A TestCase, on the other hand,
#   does not truncate tables after a test.
#   Instead, it encloses the test code in a
#   database transaction that is rolled back
#   at the end of the test. This guarantees that
#   the rollback at the end of the test
#   restores the database to its initial state.

def create_test_pipeline():
    pass


def create_test_group():
    pass


def create_test_eo_source():
    pass


def create_test_eo_product():
    pass


# noinspection DuplicatedCode
# @override_settings(MEDIA_ROOT=TEST_MEDIA_ROOT)
class JustTests(TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self) -> None:
        pass

    @classmethod
    def setUpTestData(cls):
        cls.input_group_1 = input_group_1 = EOSourceGroup.objects.create(
            name=EOSourceGroupChoices.S02P02_LAI_300M_V1_AFR,
            date_regex=r'.+-(?P<YYYYMMDD>\d+)\..+',
            crawler_type=EOSourceGroup.CrawlerTypeChoices.NONE
        )
        cls.input_group_2 = input_group_2 = EOSourceGroup.objects.create(
            name=EOSourceGroupChoices.S02P02_NDVI_1KM_V3_AFR,
            date_regex=r'.+-(?P<YYYYMMDD>\d+)\..+',
            crawler_type=EOSourceGroup.CrawlerTypeChoices.NONE
        )
        cls.eo_source_1 = eo_source_1 = EOSource.objects.create(
            state=EOSourceStateChoices.AVAILABLE_LOCALLY,
            filename='file-type-A-20210101.xxyy',
            domain='https://example-A.com/',
            filesize_reported=0,
            reference_date=datetime(year=2021, month=1, day=1, tzinfo=utc_tz),
            datetime_seen=now(),
            url='https://www.example-A.com/file-typeA-20210101.xxyy',
            credentials=None
        )
        eo_source_1.group.add(input_group_1)

        cls.eo_source_2 = eo_source_2 = EOSource.objects.create(
            state=EOSourceStateChoices.AVAILABLE_LOCALLY,
            filename='file-type-B-20210101.xxyy',
            domain='https://example-B.com/',
            filesize_reported=0,
            reference_date=datetime(year=2021, month=1, day=1, tzinfo=utc_tz),
            datetime_seen=now(),
            url='https://www.example-B.com/file-typeA-20210101.xxyy',
            credentials=None
        )
        eo_source_2.group.add(input_group_2)

        cls.output_group_1 = output_group_1 = EOProductGroup.objects.create(
            name=EOProductGroupChoices.S02P02_NDVI_300M_V3_AFR,
        )

        cls.outpout_file_1 = output_file_1 = EOProduct.objects.create(

        )

        cls.pipeline = pipeline = Pipeline.objects.create(
            name='test_pipeline',

        )

    def test_delete_eo_source(self):
        pass
