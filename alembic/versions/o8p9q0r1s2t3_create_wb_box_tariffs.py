"""create wb_box_tariffs table

Revision ID: o8p9q0r1s2t3
Revises: n7o8p9q0r1s2
Create Date: 2026-06-03

Таблица тарифов коробной логистики WB по складам.
Источник: GET https://common-api.wildberries.ru/api/v1/tariffs/box
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP

revision = "o8p9q0r1s2t3"
down_revision = "n7o8p9q0r1s2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "wb_box_tariffs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("organization_id", UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True),
        # Склад
        sa.Column("warehouse_name", sa.String(255), nullable=False, index=True),
        sa.Column("geo_name", sa.String(255), nullable=True),
        # ФБО-тарифы
        sa.Column("box_delivery_base", sa.Numeric(10, 2), nullable=True, comment="ФБО: логистика первый литр, ₽"),
        sa.Column("box_delivery_liter", sa.Numeric(10, 2), nullable=True, comment="ФБО: логистика каждый следующий литр, ₽"),
        sa.Column("box_delivery_coef", sa.Numeric(10, 2), nullable=True, comment="ФБО: коэффициент логистики, %"),
        # ФБС-тарифы
        sa.Column("box_delivery_marketplace_base", sa.Numeric(10, 2), nullable=True, comment="ФБС: логистика первый литр, ₽"),
        sa.Column("box_delivery_marketplace_liter", sa.Numeric(10, 2), nullable=True, comment="ФБС: логистика каждый следующий литр, ₽"),
        sa.Column("box_delivery_marketplace_coef", sa.Numeric(10, 2), nullable=True, comment="ФБС: коэффициент логистики, %"),
        # Хранение
        sa.Column("box_storage_base", sa.Numeric(10, 2), nullable=True, comment="Хранение первый литр/день, ₽"),
        sa.Column("box_storage_liter", sa.Numeric(10, 2), nullable=True, comment="Хранение каждый следующий литр/день, ₽"),
        sa.Column("box_storage_coef", sa.Numeric(10, 2), nullable=True, comment="Коэффициент хранения, %"),
        # Даты
        sa.Column("snapshot_date", sa.Date, nullable=False, index=True),
        sa.Column("created_at", TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("organization_id", "warehouse_name", "snapshot_date", name="wb_box_tariffs_org_wh_date_key"),
    )


def downgrade():
    op.drop_table("wb_box_tariffs")
