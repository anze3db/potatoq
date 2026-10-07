from potatoq import shared_task


@shared_task
def send_receipt(order_id):
    from django.db import connection

    with connection.cursor() as cur:
        cur.execute("SELECT total FROM shop_order WHERE id = %s", [order_id])
        row = cur.fetchone()
    return {"order": order_id, "total": row[0] if row else None}


@shared_task(enqueue_on_commit=False)
def audit(event):
    return event
