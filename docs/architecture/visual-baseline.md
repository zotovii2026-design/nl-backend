# NL Table: visual baseline

Status: captured and verified before the SaaS refactor.

Capture date: 2026-06-11.

Source checkpoint: `pre-saas-refactor-20260611-1418`.

Source Git tag: `pre-saas-refactor-2026-06-11`.

Viewport: `780x493`, headless Chrome.

## Screens

| File | Screen | SHA-256 |
| --- | --- | --- |
| `00-login.png` | Login | `ed723715a8d1d03f34456c97b41fec57a34639704c08f2a0650b02b9396323b0` |
| `01-dashboard.png` | Main dashboard | `206007ea6c920377cc664b55f0a28e7e207421e809dcd032d7b187ab839d9de3` |
| `02-reference-book.png` | Reference book | `875e65263e9b170c37ba8323ca84887b678a07c77a8caa8203a8d161c8b7f4ac` |
| `03-unit-economics.png` | Unit economics | `a3809c04b90a2d664ab66f26780061a58883b7aefaaccd1272d63e629a7a4a67` |
| `04-ads.png` | Advertising | `4b09500142add8ab12be04ee540b6ebfdc34b20b097248f34ac82830480ccf4a` |
| `05-promotions.png` | Promotions | `9aa36085102f0939a881af1da2d75d6ced283e53a3eb3a84f4259e6972ebcc7c` |

## Private storage

The screenshots contain account and live marketplace data. They must not be
committed to this public repository.

- Production checkpoint:
  `/root/nl-checkpoints/pre-saas-refactor-20260611-1418/screenshots/`
- Independent backup:
  `/home/clawd/.openclaw/private-backups/nl-table/pre-saas-refactor-20260611-1418/screenshots/`

Both copies were compared by SHA-256 on 2026-06-11.

## Comparison rules

Use this baseline when a refactor changes HTML, CSS, JavaScript loading, page
navigation, table rendering, or frontend API contracts.

1. Use the same viewport and browser family.
2. Select the same organization and wait for the page data to finish loading.
3. Capture the six named screens in the same order.
4. Compare layout, navigation, visible controls, table headers, grouping,
   loading states, and error states.
5. Store replacement screenshots privately and commit only their manifest and
   hashes.

Dynamic values such as dates, metrics, product rows, and campaign totals are
not pixel-stable. Structural differences must be reviewed separately from data
differences.
