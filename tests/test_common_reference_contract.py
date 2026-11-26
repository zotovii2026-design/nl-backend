from pathlib import Path


TEMPLATE = Path("templates/nl_v2.html").read_text(encoding="utf-8")
SCRIPT = Path("static/js/common-reference.js").read_text(encoding="utf-8")


def test_common_reference_is_a_separate_lazy_section():
    assert "navTo('commonref',this)" in TEMPLATE
    assert 'id="page-commonref"' in TEMPLATE
    assert "commonref:  { title:'Справочник общий' }" in TEMPLATE
    assert "_sectionHTML.commonref" in TEMPLATE
    assert "_sectionHTML.costprice" in TEMPLATE
    assert "case 'commonref':" in TEMPLATE


def test_common_reference_reuses_existing_reference_workflow():
    enter_block = TEMPLATE.split("case 'commonref':", 1)[1].split("break;", 1)[0]
    assert "loadCommonReference()" in enter_block
    assert "loadTaxSettings()" in enter_block
    assert "loadFboAverageWarehouses()" in enter_block
    assert "loadCostPrices()" in enter_block


def test_common_reference_has_three_modes_and_matching():
    assert "/api/v1/commerce/products?" in SCRIPT
    assert "switchCommonReferenceMode('wb')" in TEMPLATE
    assert "switchCommonReferenceMode('ozon')" in TEMPLATE
    assert "switchCommonReferenceMode('common')" in TEMPLATE
    assert "attach-listing" in SCRIPT
    assert "Добавить к сопоставлению" in SCRIPT
    assert "WB + Ozon" in SCRIPT
    assert "WB only" in SCRIPT
    assert "Ozon only" in SCRIPT
    assert "unmatched" in SCRIPT
    assert "common-reference-wb-panel" in TEMPLATE
    assert "common-reference-ozon-panel" in TEMPLATE
    assert "common-reference-common-panel" in TEMPLATE


def test_common_reference_is_reloaded_for_org_switches():
    assert TEMPLATE.count("tabName === 'commonref'") == 2
    assert TEMPLATE.count("loadCommonReference(); loadTaxSettings(); loadFboAverageWarehouses(); loadCostPrices();") == 2


def test_common_reference_asset_is_loaded():
    assert "/static/js/common-reference.js?v=20261126-1" in TEMPLATE
