import logging

from celery import shared_task
from django.apps import apps
from django_opensearch_dsl import Document
from django_opensearch_dsl.registries import registry

logger = logging.getLogger(__name__)

# Version marker for tracking patched deployments
OPENSEARCH_BULK_VERSION = "2.0.0-backcompat"  # Supports both PKs and full actions


@shared_task
def index_opensearch_bulk(app_label, model_name, pk_list, actions_list=None, using=None, **kwargs):
    """
    Celery task for asynchronous bulk indexing to OpenSearch.

    Backwards-compatible version supporting both:
    1. NEW: pk_list (small message, reconstructs from DB)
       - Addresses RabbitMQ max frame size limit (128MB)
       - Supports large datasets (191k+ records)
       - Used by patched BaseSyncDocument.bulk()

    2. OLD: actions_list (full action dicts)
       - Used during upgrade transition period
       - Allows old queued tasks to drain
       - Provides backwards compatibility

    Args:
        app_label: Django app label (e.g., 'individual')
        model_name: Model class name (e.g., 'Individual')
        pk_list: List of primary keys to index
        actions_list: Legacy parameter - full action dicts (optional, backwards-compat only)
        using: OpenSearch connection alias
        **kwargs: Additional kwargs passed to bulk()
    """
    Model = apps.get_model(app_label, model_name)
    document_classes = list(_get_documents(app_label, model_name))

    if not document_classes:
        logger.warning(f"No OpenSearch document found for model: {model_name} in app: {app_label}")
        return

    # Handle legacy format: if actions_list is provided and non-empty, use it
    if actions_list is not None and len(actions_list) > 0:
        logger.info(
            f"[v{OPENSEARCH_BULK_VERSION}] Using legacy actions format for {model_name} "
            f"({len(actions_list)} actions). This indicates an old queued task during upgrade."
        )
        for doc_class in document_classes:
            try:
                doc_class().bulk(iter(actions_list), using=using, from_celery=True, **kwargs)
            except Exception as e:
                logger.error(f"Error indexing {model_name} with legacy actions: {e}", exc_info=True)
        return

    # Handle new format: reconstruct queryset from PKs
    if pk_list and len(pk_list) > 0:
        logger.info(
            f"[v{OPENSEARCH_BULK_VERSION}] Reconstructing {model_name} from {len(pk_list)} PKs "
            f"for OpenSearch indexing"
        )
        try:
            # Filter queryset to PKs we need to index
            queryset = Model.objects.filter(pk__in=pk_list)
            indexed_count = 0

            for doc_class in document_classes:
                # Use Document.update() directly (parent class) to avoid BaseSyncDocument.bulk() recursion
                # This executes the actual OpenSearch bulk indexing
                response = Document.update(doc_class(), queryset, using=using, **kwargs)
                # response is typically (num_indexed, errors_list)
                if response and isinstance(response, tuple):
                    indexed_count += response[0]

            logger.info(f"Successfully indexed {indexed_count} {model_name} documents to OpenSearch")
        except Exception as e:
            logger.error(f"Error indexing {model_name} from PKs: {e}", exc_info=True)
    else:
        logger.warning(f"No PKs or actions provided for indexing {model_name}")


def _get_documents(app_label, model_name):
    """
    Yield document classes registered for the given model.

    Args:
        app_label: Django app label
        model_name: Model class name

    Yields:
        Document class instances
    """
    try:
        Model = apps.get_model(app_label, model_name)
        document_classes = registry._models.get(Model, [])

        if not document_classes:
            logger.warning(f"No OpenSearch document found for model: {model_name} in app: {app_label}")
            return

        for doc_class in document_classes:
            yield doc_class()
    except LookupError as e:
        logger.error(f"Failed to load model {app_label}.{model_name}: {e}")

