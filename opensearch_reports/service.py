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
    Base document class controlling synchronization with OpenSearchDashboard.synch_disabled.

    Rules:
    - If dashboard.synch_disabled=True -> skip and return (0, [])
    - If OPENSEARCH_FORCE_SYNC=1 -> run super().bulk() synchronously
    - If from_celery=True -> run super().bulk() synchronously (worker execution)
    - Otherwise -> queue Celery task and return (0, [])
    """
    DASHBOARD_NAME = None

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

        # ASYNC path (normal)
        model = self.Django.model
        app_label = model._meta.app_label
        index_opensearch_bulk.delay(app_label, model.__name__, list(actions), using=using, **kwargs)
        return (0, [])
