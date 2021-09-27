def get_task_ref_from_name(token: str):
    """ get ref to runction/task by name. raises AttributeError exception if not found """
    from eo_engine import tasks

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
