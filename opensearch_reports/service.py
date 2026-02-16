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

        When from_celery=False (from signals/management commands):
          - Extracts PKs from actions to keep Celery message size small
          - Queues async task to avoid 128MB RabbitMQ message limit
          - Returns (count, errors) tuple for command reporting

        When from_celery=True (from Celery worker):
          - Executes actual bulk indexing to OpenSearch
          - Uses reconstructed queryset from PKs
        """
        if not self.is_sync_disabled():
            if from_celery:
                return super().bulk(actions, using=using, **kwargs)
            else:
                # Extract PKs from actions to keep Celery message payload small
                # Actions are dicts with structure: {'_op_type': 'index'|'update', '_id': pk, ...}
                actions_list = list(actions)
                pk_list = [
                    action.get('_id')
                    for action in actions_list
                    if action.get('_op_type') in ('index', 'update')
                ]

                if pk_list:
                    model = self.Django.model
                    app_label = model._meta.app_label
                    logger.debug(f"Queueing {len(pk_list)} PKs for {model.__name__} to OpenSearch via Celery")
                    index_opensearch_bulk.delay(
                        app_label, model.__name__, pk_list, using=using, **kwargs
                    )
                    # Return (count, []) per elasticsearch-py bulk API contract
                    return len(pk_list), []
                else:
                    return 0, []
        else:
            # Log and skip bulk syncing if disabled
            logger.info(f"Skipping bulk sync because sync is disabled for dashboard '{self.DASHBOARD_NAME}'")
            return 0, []