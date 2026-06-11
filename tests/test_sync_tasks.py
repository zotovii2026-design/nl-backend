from tasks.stocks_sync import sync_stocks_task
from tasks.wb_sync import sync_products_task, sync_sales_task


def test_manual_sync_task_names_are_stable():
    assert sync_products_task.name == "wb.sync_products"
    assert sync_sales_task.name == "wb.sync_sales"
    assert sync_stocks_task.name == "wb.sync_stocks"
