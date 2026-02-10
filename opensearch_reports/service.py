import logging
import os

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


class BaseSyncDocument(Document):
    """
    Base document class that controls synchronization based on the 'synch_disabled' flag.
    All OpenSearch document classes should inherit from this class.

    Behavior:
    - If dashboard.synch_disabled=True => do nothing
    - If OPENSEARCH_FORCE_SYNC=1 OR from_celery=True => run super().bulk() synchronously
    - Else => queue celery task index_opensearch_bulk.delay(...)
    """
    DASHBOARD_NAME = None

    def is_sync_disabled(self):
        try:
            dashboard = OpenSearchDashboard.objects.get(name=self.DASHBOARD_NAME)
            return dashboard.synch_disabled
        except OpenSearchDashboard.DoesNotExist:
            # If no dashboard entry, assume sync is enabled
            return False

def bulk(self, actions, using=None, from_celery=False, **kwargs):
    force_sync = os.getenv("OPENSEARCH_FORCE_SYNC") in ("1", "true", "True", "YES", "yes")

    if self.is_sync_disabled():
        logger.info(
            "Skipping bulk sync because sync is disabled for dashboard '%s'",
            self.DASHBOARD_NAME,
        )
        return (0, [])

    # FORCE SYNC PATH (backfills / rebuilds) OR celery executing the task
    if force_sync or from_celery:
        res = super().bulk(actions, using=using, **kwargs)
        # Normalize return to tuple expected by management command
        if res is None:
            return (0, [])
        return res

    # Normal async behavior (queue to celery)
    model = self.Django.model
    app_label = model._meta.app_label
    index_opensearch_bulk.delay(app_label, model.__name__, list(actions), using=using, **kwargs)
    return (0, [])
