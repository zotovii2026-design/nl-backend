from typing import Optional

from pydantic import BaseModel


class SalesPlanItem(BaseModel):
    nm_id: int
    vendor_code: Optional[str] = None
    size_name: Optional[str] = None
    period: str
    plan_type: str = "quantity"
    plan_value: float = 0
    actual_value: float = 0
    sales_temp: Optional[float] = None
    seasonality: str = "medium"
    entity_id: Optional[str] = None
