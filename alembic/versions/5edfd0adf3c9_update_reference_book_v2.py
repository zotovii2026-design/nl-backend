"""update reference_book v2 — seasonality, dims, delivery, queries

Revision ID: 5edfd0adf3c9
Revises: b4a32c41d930
Create Date: 2026-05-07

"""
from alembic import op
import sqlalchemy as sa

revision = '5edfd0adf3c9'
down_revision = 'b4a32c41d930'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- Удалить ---
    op.drop_column('reference_book', 'tax_rate')
    op.drop_column('reference_book', 'vat_rate')

    # --- Сезонность (коэффициенты по месяцам) ---
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    for m in months:
        op.add_column('reference_book', sa.Column(f'season_{m}', sa.Numeric(5,2), nullable=True))

    # --- Габариты ПЛАН (ручной ввод) ---
    op.add_column('reference_book', sa.Column('plan_length', sa.Numeric(8,2), nullable=True))
    op.add_column('reference_book', sa.Column('plan_width', sa.Numeric(8,2), nullable=True))
    op.add_column('reference_book', sa.Column('plan_height', sa.Numeric(8,2), nullable=True))
    op.add_column('reference_book', sa.Column('plan_volume', sa.Numeric(8,2), nullable=True))
    op.add_column('reference_book', sa.Column('plan_weight', sa.Numeric(8,2), nullable=True))

    # --- Скорость доставаемости ---
    op.add_column('reference_book', sa.Column('delivery_days_to_seller', sa.Integer, nullable=True))
    op.add_column('reference_book', sa.Column('delivery_days_to_mp', sa.Integer, nullable=True))

    # --- ТОП запросы планируемые (3 шт) ---
    op.add_column('reference_book', sa.Column('top_query_1', sa.String(200), nullable=True))
    op.add_column('reference_book', sa.Column('top_query_2', sa.String(200), nullable=True))
    op.add_column('reference_book', sa.Column('top_query_3', sa.String(200), nullable=True))

    # --- Приоритетный способ отгрузки ---
    op.add_column('reference_book', sa.Column('shipment_method', sa.String(50), nullable=True))

    # --- Склад отгрузки FBS ---
    op.add_column('reference_book', sa.Column('fbs_warehouse', sa.String(200), nullable=True))


def downgrade() -> None:
    # Добавить обратно удалённые
    op.add_column('reference_book', sa.Column('tax_rate', sa.Numeric(5,2), nullable=True))
    op.add_column('reference_book', sa.Column('vat_rate', sa.Numeric(5,2), nullable=True))

    # Удалить новые
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    for m in months:
        op.drop_column('reference_book', f'season_{m}')

    for col in ['plan_length','plan_width','plan_height','plan_volume','plan_weight',
                'delivery_days_to_seller','delivery_days_to_mp',
                'top_query_1','top_query_2','top_query_3',
                'shipment_method','fbs_warehouse']:
        op.drop_column('reference_book', col)
