from django.test import SimpleTestCase
from django.urls import reverse
from django.http import QueryDict


class TestViewETC(SimpleTestCase):

    def test_submit_task_view(self):
        query_dictionary = QueryDict('', mutable=True)
        query_dictionary.update(
            task_name='task_sftp_parse_remote_dir',
            remote_dir='sftp://safmil.ipma.pt/home/safpt/OperationalChain/LSASAF_Products/DMET'
        )
        url = '{base_url}?{querystring}'.format(
            base_url=reverse("eo_engine:submit-task"),
            querystring=query_dictionary.urlencode()
        )
        self.client.get(url, follow=True)
