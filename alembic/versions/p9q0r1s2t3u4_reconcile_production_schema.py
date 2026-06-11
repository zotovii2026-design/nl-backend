"""reconcile production schema

Revision ID: p9q0r1s2t3u4
Revises: o8p9q0r1s2t3
Create Date: 2026-06-11
"""

from alembic import op


revision = "p9q0r1s2t3u4"
down_revision = "o8p9q0r1s2t3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Capture schema changes that were previously applied directly to
    # production, making a clean database equivalent to the live schema.
    op.execute("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS wb_seller_id INTEGER")

    op.execute("ALTER TABLE reference_book ALTER COLUMN id SET DEFAULT gen_random_uuid()")
    op.execute("ALTER TABLE reference_book ADD COLUMN IF NOT EXISTS min_price NUMERIC(12, 2)")
    op.execute("ALTER TABLE reference_book ADD COLUMN IF NOT EXISTS rrc_price NUMERIC(12, 2)")
    op.execute("ALTER TABLE reference_book ADD COLUMN IF NOT EXISTS vat_rate NUMERIC(5, 2)")
    op.execute("ALTER TABLE reference_book ADD COLUMN IF NOT EXISTS tax_rate NUMERIC(5, 2) DEFAULT 0")

    op.execute(
        "ALTER TABLE wb_tariff_snapshot "
        "ADD COLUMN IF NOT EXISTS commission_fbs_pct NUMERIC(5, 2)"
    )

    for column in ("weight", "width", "height", "length"):
        op.execute(
            f"ALTER TABLE product_entities ALTER COLUMN {column} "
            f"TYPE DOUBLE PRECISION USING {column}::DOUBLE PRECISION"
        )

    op.execute("""
        CREATE TABLE IF NOT EXISTS raw_api_data (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            organization_id UUID NOT NULL
                REFERENCES organizations(id) ON DELETE CASCADE,
            api_method VARCHAR(100) NOT NULL,
            target_date DATE NOT NULL,
            raw_response JSONB,
            status VARCHAR(20) NOT NULL DEFAULT 'ok',
            error_message TEXT,
            records_count INTEGER,
            fetched_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            is_final VARCHAR(5) DEFAULT 'no',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            CONSTRAINT raw_api_data_organization_id_api_method_target_date_key
                UNIQUE (organization_id, api_method, target_date)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_api_data_org ON raw_api_data(organization_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_api_data_method ON raw_api_data(api_method)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_api_data_date ON raw_api_data(target_date)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS warehouse_refs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            organization_id UUID NOT NULL
                REFERENCES organizations(id) ON DELETE CASCADE,
            wb_warehouse_id INTEGER NOT NULL UNIQUE,
            name VARCHAR(200) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_warehouse_refs_org "
        "ON warehouse_refs(organization_id)"
    )

    op.execute("""
        CREATE TABLE IF NOT EXISTS ad_campaigns (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            organization_id UUID NOT NULL
                REFERENCES organizations(id) ON DELETE CASCADE,
            wb_campaign_id INTEGER NOT NULL,
            name VARCHAR(500) NOT NULL,
            type VARCHAR(50),
            status VARCHAR(50),
            budget NUMERIC(12, 2),
            nm_ids JSONB DEFAULT '[]'::JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE,
            payment_type VARCHAR(20),
            bid_type VARCHAR(20),
            daily_budget NUMERIC(12, 2) DEFAULT 0,
            wb_change_time TIMESTAMP WITH TIME ZONE,
            CONSTRAINT ad_campaigns_organization_id_wb_campaign_id_key
                UNIQUE (organization_id, wb_campaign_id)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_ad_campaigns_org ON ad_campaigns(organization_id)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS ad_stats (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            organization_id UUID NOT NULL
                REFERENCES organizations(id) ON DELETE CASCADE,
            wb_campaign_id INTEGER NOT NULL,
            stat_date DATE NOT NULL,
            views INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0,
            spent NUMERIC(12, 2) DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            ctr NUMERIC(8, 4) DEFAULT 0,
            cpc NUMERIC(10, 2) DEFAULT 0,
            orders INTEGER DEFAULT 0,
            atbs INTEGER DEFAULT 0,
            cr NUMERIC(8, 4) DEFAULT 0,
            canceled INTEGER DEFAULT 0,
            shks INTEGER DEFAULT 0,
            sum_price NUMERIC(12, 2) DEFAULT 0,
            currency VARCHAR(10) DEFAULT 'RUB',
            CONSTRAINT ad_stats_organization_id_wb_campaign_id_stat_date_key
                UNIQUE (organization_id, wb_campaign_id, stat_date)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_ad_stats_org ON ad_stats(organization_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ad_stats_date ON ad_stats(stat_date)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS ad_stats_nm (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            organization_id UUID NOT NULL,
            wb_campaign_id INTEGER NOT NULL,
            nm_id INTEGER NOT NULL,
            stat_date DATE NOT NULL,
            app_type INTEGER DEFAULT 1,
            views INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0,
            spent NUMERIC DEFAULT 0,
            ctr NUMERIC DEFAULT 0,
            cpc NUMERIC DEFAULT 0,
            orders INTEGER DEFAULT 0,
            atbs INTEGER DEFAULT 0,
            cr NUMERIC DEFAULT 0,
            canceled INTEGER DEFAULT 0,
            shks INTEGER DEFAULT 0,
            sum_price NUMERIC DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            CONSTRAINT ad_stats_nm_organization_id_wb_campaign_id_nm_id_stat_date__key
                UNIQUE (organization_id, wb_campaign_id, nm_id, stat_date, app_type)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ad_stats_nm_campaign "
        "ON ad_stats_nm(organization_id, wb_campaign_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ad_stats_nm_org_nm "
        "ON ad_stats_nm(organization_id, nm_id)"
    )


def downgrade() -> None:
    # This migration records pre-existing production state. Dropping these
    # objects automatically would risk deleting live data.
    pass
