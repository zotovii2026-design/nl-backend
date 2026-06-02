"""add wb_price_fact and wb_discount to reference_book

Revision ID: n7o8p9q0r1s2
Revises: m2n3o4p5q6r7
Create Date: 2026-06-02

Добавляем колонки для хранения актуальных цен из WB Prices API:
- wb_price_fact: discountedPrice (цена со скидкой, реально на витрине)
- wb_price_retail: price (цена до скидки)
- wb_discount_pct: discount (скидка %)
- wb_prices_updated_at: когда последний раз обновили из API
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TIMESTAMP

revision = "n7o8p9q0r1s2"
down_revision = "323b0e18fa1c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("reference_book", sa.Column("wb_price_fact", sa.Numeric(12, 2), nullable=True, comment="Цена со скидкой из WB API (discountedPrice)"))
    op.add_column("reference_book", sa.Column("wb_price_retail", sa.Numeric(12, 2), nullable=True, comment="Цена до скидки из WB API (price)"))
    op.add_column("reference_book", sa.Column("wb_discount_pct", sa.Integer, nullable=True, comment="Скидка WB % из API"))
    op.add_column("reference_book", sa.Column("wb_prices_updated_at", TIMESTAMP(timezone=True), nullable=True, comment="Время последнего обновления цен из WB API"))


def downgrade():
    op.drop_column("reference_book", "wb_prices_updated_at")
    op.drop_column("reference_book", "wb_discount_pct")
    op.drop_column("reference_book", "wb_price_retail")
    op.drop_column("reference_book", "wb_price_fact")
