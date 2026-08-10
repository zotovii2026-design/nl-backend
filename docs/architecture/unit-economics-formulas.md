# Unit economics formula registry

The executable formulas live in `domain/unit_economics.py`. API handlers must
not duplicate or modify them.

## Logistics

- Product volume: `length * width * height / 1000`, dimensions in centimeters,
  result in liters.
- Forward delivery up to 1 liter: WB tier rate multiplied by the warehouse
  coefficient.
- Forward FBO delivery: average tariff for the organization's selected FBO
  warehouses, 1 to 5 warehouses.
- If the organization has no selected FBO warehouses, the default average uses
  Kolедино, Краснодар and Казань.
- If a saved FBO warehouse selection cannot be matched in fresh tariffs, the
  calculation falls back to the default three warehouses.
- Forward FBS delivery: tariff of the selected FBS warehouse, with Kolедино as
  the legacy fallback.
- Reverse delivery: WB base volume tariff without a warehouse coefficient.
- Logistics with buyout: forward delivery plus reverse delivery multiplied by
  the non-buyout share.

Sources: `product_entities`, `wb_box_tariffs`, `wb_tariff_snapshot`.

## Marketplace and taxes

- Marketplace percent: base commission plus manual correction.
- Marketplace commission: customer price multiplied by marketplace percent.
- Acquiring: 1.5 percent of customer price.
- USN tax: customer price multiplied by the organization tax rate.
- USN income-minus-expenses tax: positive income after marketplace commission
  and cost price, multiplied by the organization tax rate.
- OSN tax: output VAT minus input VAT calculated from purchase cost.

Sources: `reference_book`, `wb_tariff_snapshot`, organization tax settings.

## Scenarios

The API returns three calculations:

- `fact`: current customer price and factual advertising inputs.
- `plan`: planned price and planned advertising percent.
- `change`: price after proposed changes.

Each scenario reports expenses, profit, margin, ROI and transfer-to-account
values where those fields exist in the legacy API contract.

## Known placeholders

The current API intentionally keeps these values at zero until a verified data
source is connected:

- `spp_pct`
- `price_with_spp`
- `ad_fact_pct`
- `ad_fact_rub`
- `logistics_actual`
- `storage_actual`
- `acceptance_avg`

Changing a placeholder into a factual metric requires a source, period,
measurement unit and characterization test.
