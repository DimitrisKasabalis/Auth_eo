from typing import List, Union
import re
from more_itertools import flatten, collapse

RE_PROCESS_TASK = re.compile(r'eo_engine\.tasks\.task_s[0-9]{1,2}p[0-9]{1,2}.+$')


def is_process_task(task_name: str) -> bool:
    if RE_PROCESS_TASK.search(task_name) is not None:
        return True
    return False


def get_task_ref_from_name(token: Union[str, List[str]]):
    """ Return a ref to function/task by name. Raises AttributeError exception if not found """
    from eo_engine import tasks
    if isinstance(token, List):
        token = next(collapse(token))
    # remove namespaces: a.b.task_name -> task_name
    token = token.split('.')[-1]
    return getattr(tasks, token)


def revoke_task(task_id, terminate: bool = False):
    from mproj import celery_app as app
    from eo_engine.models import GeopTask
    from eo_engine.models import EOSourceStateChoices, EOProductStateChoices

    task = GeopTask.objects.get(task_id=task_id)

    if task.eo_product.exists():
        db_entries = task.eo_product.all()
        db_entries.update(status=EOProductStateChoices.Ignore)

    elif task.eo_source.exists():
        db_entries = task.eo_source.all()
        db_entries.update(status=EOSourceStateChoices.Ignore)
    task.state = GeopTask.TaskTypeChoices.REVOKED
    task.save()
    app.control.revoke(task_id=task_id, terminate=terminate)
