import inspect

from celery.signals import before_task_publish

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


# @after_task_publish.connect
# def handles_task_after_publish(headers=None, body=None, sender: str = None, **kwargs):
#     pass


@before_task_publish.connect
def handles_task_publish(sender: str = None, headers=None, body=None, **kwargs):
    """ Essential procedures after a task was published on the broker.
    This function is connected to the  after_task_publish signal """
    # set the default Django settings module for the 'celery' program.
    # print('Before connect signal firing up')
    from eo_engine.models import GeopGroupTask, GeopTask

    if not (sender.startswith('eo_engine') or
            sender.startswith('app')):
        return
    info = headers

    task_id = info['id']
    group_task_id = info.get('group', None)
    root_id = None if task_id == info.get('root_id') else info.get('root_id', task_id)
    if group_task_id:
        group_task_obj, created = GeopGroupTask.objects.get_or_create(group_task_id=group_task_id, root_id=root_id)

    else:
        group_task_obj = None
    # in case of Retry, celery will try to
    # republish the task with the same task_id
    # _kwargs_repr = json.dumps(body[1])

    task_obj, created = GeopTask.objects.get_or_create(task_id=task_id)

    if created:
        _data = dict()
        _data.update(task_name=info['task'],
                     task_args=info['argsrepr'],
                     task_kwargs=body[1],
                     parent_id=info['parent_id'],
                     root_id=info['root_id'],
                     group_task=group_task_obj
                     )
        for k, v in _data.items():
            setattr(task_obj, k, v)
        task_obj.save()
        logger.info(f'{inspect.stack()[0][3]}: Created task: {info["id"]}')
    else:
        logger.info(f'{inspect.stack()[0][3]}: Task {info["id"]} found.')
    return
