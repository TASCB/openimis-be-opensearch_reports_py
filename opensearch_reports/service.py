import logging
import os
from itertools import islice

from django_opensearch_dsl import Document

from core.services import BaseService
from core.signals import register_service_signal
from opensearch_reports.models import OpenSearchDashboard
from opensearch_reports.validations import OpenSearchDashboardValidation
from opensearch_reports.tasks import index_opensearch_bulk

logger = logging.getLogger(__name__)


class OpenSearchDashboardService(BaseService):
    @register_service_signal("opensearch_dashboard_service.update")
    def update(self, obj_data):
        return super().update(obj_data)

    OBJECT_TYPE = OpenSearchDashboard

    def __init__(self, user, validation_class=OpenSearchDashboardValidation):
        super().__init__(user, validation_class)


def _iter_chunks(iterable, chunk_size: int):
    """
    Yield lists of up to chunk_size items from iterable, without loading everything at once.
    """
    it = iter(iterable)
    while True:
        chunk = list(islice(it, chunk_size))
        if not chunk:
            break
        yield chunk


class BaseSyncDocument(Document):
    """
    Base document class controlling synchronization with OpenSearchDashboard.synch_disabled.

    Rules:
    - If dashboard.synch_disabled=True -> skip and return (0, [])
    - If OPENSEARCH_FORCE_SYNC=1 -> run super().bulk() synchronously (NO celery)
    - If from_celery=True -> run super().bulk() synchronously (worker execution)
    - Otherwise -> queue Celery tasks in SMALL CHUNKS and return (0, [])
    """

    DASHBOARD_NAME = None

    # IMPORTANT:
    # Keep this low enough to avoid RabbitMQ max message size.
    # If you still see RabbitMQ "message size larger than configured max", reduce further.
    ASYNC_BULK_CHUNK_SIZE = int(os.getenv("OPENSEARCH_ASYNC_CHUNK_SIZE", "200"))

    def is_sync_disabled(self):
        try:
            dashboard = OpenSearchDashboard.objects.get(name=self.DASHBOARD_NAME)
            return dashboard.synch_disabled
        except OpenSearchDashboard.DoesNotExist:
            return False

    def bulk(self, actions, using=None, from_celery=False, **kwargs):
        if self.is_sync_disabled():
            logger.info(
                "Skipping OpenSearch sync; dashboard '%s' is disabled",
                self.DASHBOARD_NAME,
            )
            return (0, [])

        force_sync = os.getenv("OPENSEARCH_FORCE_SYNC", "").lower() in ("1", "true", "yes")

        # SYNC path (backfills / rebuilds OR worker execution)
        if force_sync or from_celery:
            return super().bulk(actions, using=using, **kwargs)

        # ASYNC path (normal): queue SMALL chunks, never list(actions) for whole queryset
        model = self.Django.model
        app_label = model._meta.app_label

        queued = 0
        for chunk in _iter_chunks(actions, self.ASYNC_BULK_CHUNK_SIZE):
            index_opensearch_bulk.delay(
                app_label, model.__name__, chunk, using=using, **kwargs
            )
            queued += len(chunk)

        logger.info(
            "Queued %s OpenSearch actions for %s.%s in chunks of %s",
            queued, app_label, model.__name__, self.ASYNC_BULK_CHUNK_SIZE
        )

        # management command expects (success_count, errors)
        return (0, [])
