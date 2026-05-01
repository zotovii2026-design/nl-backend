"""unify_reference_book

Расширяем cost_prices → reference_book (единый справочник),
переносим данные из unit_economics_user, добавляем новые поля.

Revision ID: h1i2j3k4l5m6
Revises: g2b3c4d5e6f7
Create Date: 2026-05-01

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "h1i2j3k4l5m6"
down_revision = "g2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Добавляем новые колонки в cost_prices (size_name уже есть!)
    op.add_column("cost_prices", sa.Column("mp_base_pct", sa.Numeric(5, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("mp_correction_pct", sa.Numeric(5, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("buyout_niche_pct", sa.Numeric(5, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("ad_plan_rub", sa.Numeric(10, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("price_before_spp_plan", sa.Numeric(10, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("price_before_spp_change", sa.Numeric(10, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("change_date", sa.Date, nullable=True))
    op.add_column("cost_prices", sa.Column("fulfillment_model", sa.String(20), nullable=True, server_default="fbo"))
    op.add_column("cost_prices", sa.Column("wb_club_discount_pct", sa.Numeric(5, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("storage_pct", sa.Numeric(5, 2), nullable=True))
    op.add_column("cost_prices", sa.Column("product_status", sa.String(50), nullable=True))

    # 2. Переносим данные из unit_economics_user → cost_prices
    op.execute("""
        UPDATE cost_prices cp
        SET 
            mp_correction_pct = ue.mp_correction_pct,
            buyout_niche_pct = ue.buyout_niche_pct,
            ad_plan_rub = ue.ad_plan_rub,
            price_before_spp_plan = ue.price_before_spp_plan,
            price_before_spp_change = ue.price_before_spp_change,
            change_date = ue.change_date,
            fulfillment_model = COALESCE(ue.tariff_type, 'fbo'),
            wb_club_discount_pct = ue.wb_club_discount_pct
        FROM unit_economics_user ue
        WHERE cp.organization_id = ue.organization_id
          AND (
              (cp.entity_id IS NOT NULL AND cp.entity_id = ue.entity_id)
              OR (cp.nm_id = ue.nm_id)
          )
    """)

    # 3. Переименовываем таблицу cost_prices → reference_book
    op.rename_table("cost_prices", "reference_book")

    # 4. Переименовываем индексы
    op.execute("ALTER INDEX IF EXISTS cost_prices_pkey RENAME TO reference_book_pkey")
    op.execute("ALTER INDEX IF EXISTS cost_prices_org_entity_vf_key RENAME TO reference_book_org_entity_vf_key")
    op.execute("ALTER INDEX IF EXISTS idx_cost_prices_nm RENAME TO idx_reference_book_nm")
    op.execute("ALTER INDEX IF EXISTS idx_cost_prices_org RENAME TO idx_reference_book_org")
    op.execute("ALTER INDEX IF EXISTS ix_cost_prices_entity_id RENAME TO ix_reference_book_entity_id")

    # 5. Удаляем unit_economics_user (данные перенесены)
    op.drop_table("unit_economics_user")

    # 6. Удаляем reference_sheet (пустая, не используется)
    op.drop_table("reference_sheet")


def downgrade() -> None:
    # ВНИМАНИЕ: downgrade — упрощённый, для полноценного отката нужен бэкап
    # Переименовываем обратно
    op.rename_table("reference_book", "cost_prices")
    
    op.execute("ALTER INDEX IF EXISTS reference_book_pkey RENAME TO cost_prices_pkey")
    op.execute("ALTER INDEX IF EXISTS reference_book_org_entity_vf_key RENAME TO cost_prices_org_entity_vf_key")
    op.execute("ALTER INDEX IF EXISTS idx_reference_book_nm RENAME TO idx_cost_prices_nm")
    op.execute("ALTER INDEX IF EXISTS idx_reference_book_org RENAME TO idx_cost_prices_org")
    op.execute("ALTER INDEX IF EXISTS ix_reference_book_entity_id RENAME TO ix_cost_prices_entity_id")

    # Удаляем новые колонки
    with op.batch_alter_table("cost_prices") as batch_op:
        batch_op.drop_column("product_status")
        batch_op.drop_column("storage_pct")
        batch_op.drop_column("wb_club_discount_pct")
        batch_op.drop_column("fulfillment_model")
        batch_op.drop_column("change_date")
        batch_op.drop_column("price_before_spp_change")
        batch_op.drop_column("price_before_spp_plan")
        batch_op.drop_column("ad_plan_rub")
        batch_op.drop_column("buyout_niche_pct")
        batch_op.drop_column("mp_correction_pct")
        batch_op.drop_column("mp_base_pct")
