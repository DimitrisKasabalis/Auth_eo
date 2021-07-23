from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


# @after_task_publish.connect
# def handles_task_after_publish(headers=None, body=None, sender: str = None, **kwargs):
#     pass
