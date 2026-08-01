"""paid storage barcode key

Revision ID: a2b3c4d5e6f7
Revises: z1a2b3c4d5e6
Create Date: 2026-07-31 13:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a2b3c4d5e6f7"
down_revision: Union[str, None] = "z1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "wb_paid_storage_rows",
        sa.Column("barcode", sa.String(length=100), nullable=False, server_default=""),
    )
    op.execute(
        """
        UPDATE wb_paid_storage_rows
        SET barcode = COALESCE(
            NULLIF(raw_data->>'sku', ''),
            NULLIF(raw_data->>'barcode', ''),
            ''
        )
        WHERE barcode = ''
          AND jsonb_typeof(raw_data) = 'object'
          AND raw_data ?| ARRAY['sku', 'barcode']
        """
    )
    op.drop_constraint(
        "wb_paid_storage_rows_org_date_nm_key",
        "wb_paid_storage_rows",
        type_="unique",
    )
    op.create_unique_constraint(
        "wb_paid_storage_rows_org_date_nm_barcode_key",
        "wb_paid_storage_rows",
        ["organization_id", "storage_date", "nm_id", "barcode"],
    )
    op.create_index(
        "ix_wb_paid_storage_rows_org_entity",
        "wb_paid_storage_rows",
        ["organization_id", "entity_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_wb_paid_storage_rows_org_entity",
        table_name="wb_paid_storage_rows",
    )
    op.drop_constraint(
        "wb_paid_storage_rows_org_date_nm_barcode_key",
        "wb_paid_storage_rows",
        type_="unique",
    )
    op.create_unique_constraint(
        "wb_paid_storage_rows_org_date_nm_key",
        "wb_paid_storage_rows",
        ["organization_id", "storage_date", "nm_id"],
    )
    op.drop_column("wb_paid_storage_rows", "barcode")
