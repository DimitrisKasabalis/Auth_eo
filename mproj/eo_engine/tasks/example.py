from logging import Logger
from typing import Optional

from celery import shared_task
from celery.utils.log import get_task_logger

logger: Logger = get_task_logger(__name__)


@shared_task
def task_debug_add(x: int, y: int) -> int:
    return x + y


@shared_task
def task_debug_append_char(token: str) -> str:
    import string
    from random import choice
    new_char = choice(string.ascii_letters)
    print(f'Appending {new_char} to {token}.')
    return token + new_char


@shared_task
def task_debug_failing(wait_time: Optional[int] = None):
    if wait_time is None:
        wait_time = 2
    import time
    time.sleep(wait_time)

    logger.info('About to throw exception!')
    raise Exception('Expected-Error')


__all__ = ['task_debug_add',
           'task_debug_append_char',
           'task_debug_failing']
