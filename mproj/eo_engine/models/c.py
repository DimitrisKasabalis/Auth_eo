from celery import states as c_states
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.db import models


class UBDCGroupTask(models.Model):
    # Convenience model to retrieve tasks

    group_task_id = models.UUIDField(editable=False, db_index=True, unique=True)
    root_id = models.UUIDField(editable=False, db_index=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    datetime_started = models.DateTimeField(null=True)
    datetime_finished = models.DateTimeField(null=True)  # TODO: find a good way to find when a group-task finishes

    op_name = models.TextField(blank=True, null=True)
    op_args = ArrayField(base_field=models.CharField(max_length=255), blank=True, null=True)
    op_kwargs = models.JSONField(default=dict)
    op_initiator = models.TextField(max_length=255, default='')

    class Meta:
        ordering = ["timestamp", ]
        unique_together = ['group_task_id', 'root_id']


class UBDCTask(models.Model):
    class TaskTypeChoices(models.TextChoices):
        SUBMITTED = 'SUBMITTED'
        STARTED = c_states.STARTED
        SUCCESS = c_states.SUCCESS
        FAILURE = c_states.FAILURE
        REVOKED = c_states.REVOKED
        RETRY = c_states.RETRY
        UNKNOWN = 'UNKNOWN'

    task_id = models.UUIDField(editable=False, unique=True, db_index=True)
    task_name = models.TextField(blank=False)
    task_args = models.TextField(default='[]')
    parent_id = models.UUIDField(editable=False, db_index=True, null=True)

    status = models.TextField(choices=TaskTypeChoices.choices, default=TaskTypeChoices.SUBMITTED, db_index=True)

    datetime_submitted = models.DateTimeField(auto_now_add=True, db_index=True)
    datetime_started = models.DateTimeField(null=True, blank=True)
    datetime_finished = models.DateTimeField(null=True, blank=True)
    time_to_complete = models.TextField(blank=True, default='')
    retries = models.IntegerField(default=0)
    task_kwargs = models.JSONField(default=dict)

    group_task = models.ForeignKey('eo_engine.UBDCGroupTask', on_delete=models.DO_NOTHING, null=True,
                                   related_query_name='ubdc_task', related_name='ubdc_tasks',
                                   to_field='group_task_id')

    root_id = models.UUIDField(editable=False, db_index=True, null=True)

    class Meta:
        ordering = ["datetime_submitted", ]
        indexes = [
            GinIndex(fields=['task_kwargs'])
        ]


__all__ = [
    "UBDCGroupTask",
    "UBDCTask"
]
