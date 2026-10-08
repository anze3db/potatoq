"""A task defined next to the views instead of in tasks.py (only the URLconf imports it)."""

from potatoq import shared_task


@shared_task
def refresh_preview(order_id):
    return order_id
