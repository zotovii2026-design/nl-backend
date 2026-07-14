"""Pydantic-модели для Справочника"""
from pydantic import BaseModel
from typing import Optional


class RefItem(BaseModel):
    nm_id: int
    vendor_code: Optional[str] = None
    product_name: Optional[str] = None
    target_date: Optional[str] = None  # YYYY-MM-DD
    cost_price: Optional[float] = None
    purchase_price: Optional[float] = None
    packaging_cost: Optional[float] = None
    logistics_cost: Optional[float] = None
    other_costs: Optional[float] = None
    notes: Optional[str] = None
    product_class: Optional[str] = None
    brand: Optional[str] = None
    transport_pack_qty: Optional[int] = None
    tax_system: Optional[str] = None  # usn / osn / usn_dr
    tax_rate: Optional[float] = None
    vat_rate: Optional[float] = None
