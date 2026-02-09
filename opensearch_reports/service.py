import logging

from django_opensearch_dsl import Document

from core.services import BaseService
from core.signals import register_service_signal
from opensearch_reports.models import OpenSearchDashboard
from opensearch_reports.validations import OpenSearchDashboardValidation
from opensearch_reports.tasks import index_opensearch_bulk

logger = logging.getLogger(__name__)


class OpenSearchDashboardService(BaseService):

    @register_service_signal('opensearch_dashboard_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    OBJECT_TYPE = OpenSearchDashboard

    def __init__(self, user, validation_class=OpenSearchDashboardValidation):
        super().__init__(user, validation_class)


class BaseSyncDocument(Document):
    """
    Base document class that controls synchronization based on the 'synch_disabled' flag.
    All OpenSearch document classes should inherit from this class.
    DASHBOARD_NAME - connecting document with dashboard
    """
    DASHBOARD_NAME = None
    
    def update(self, *args, **kwargs):
        res = super().update(*args, **kwargs)
        if res is None:
            return (0, [])
        return res

    def is_sync_disabled(self):
        try:
            dashboard = OpenSearchDashboard.objects.get(name=self.DASHBOARD_NAME)
            return dashboard.synch_disabled
        except OpenSearchDashboard.DoesNotExist:
            # If no dashboard entry, assume sync is enabled
            return False

    def bulk(self, actions, using=None, from_celery=False, **kwargs):
        """
        Override the bulk method to control batch synchronization dynamically.
        Document.update() uses bulk()
        """
        if not self.is_sync_disabled():
            if from_celery:
                return super().bulk(actions, using=using, **kwargs)
            else:
                model = self.Django.model
                app_label = model._meta.app_label
                index_opensearch_bulk.delay(
                    app_label, model.__name__, list(actions), using=using, **kwargs
                )
            return (0, [])
        else:
            # Log and skip bulk syncing if disabled
            logger.info(f"Skipping bulk sync because sync is disabled for dashboard '{self.DASHBOARD_NAME}'")
            return (0, [])
