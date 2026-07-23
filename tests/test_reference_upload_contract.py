from pathlib import Path


REFERENCE_SOURCE = Path("api/v1/routers/reference.py").read_text(encoding="utf-8")
REQUIREMENTS = Path("requirements.txt").read_text(encoding="utf-8")


def test_reference_upload_supports_csv_and_xlsx_only():
    assert 'filename_lower.endswith(".csv")' in REFERENCE_SOURCE
    assert 'filename_lower.endswith(".xlsx")' in REFERENCE_SOURCE
    assert "Поддерживаются только CSV и XLSX" in REFERENCE_SOURCE
    assert "вне допустимого диапазона" in REFERENCE_SOURCE


def test_reference_xlsx_reader_is_packaged_and_returns_400_on_bad_file():
    assert "openpyxl==" in REQUIREMENTS
    assert "Не удалось прочитать XLSX-файл" in REFERENCE_SOURCE
    assert "raise HTTPException(400" in REFERENCE_SOURCE


def test_reference_template_endpoint_returns_xlsx_with_dropdowns():
    assert '@router.get("/api/v1/nl/cost-prices/template")' in REFERENCE_SOURCE
    assert "REFERENCE_TEMPLATE_HEADERS" in REFERENCE_SOURCE
    assert "DataValidation(type=\"list\"" in REFERENCE_SOURCE
    assert "spravochnik_template.xlsx" in REFERENCE_SOURCE


def test_reference_status_options_are_not_hardcoded_only():
    dashboard = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    cost_grid = Path("static/js/cost-grid.js").read_text(encoding="utf-8")
    assert '@router.get("/api/v1/nl/cost-prices/statuses")' in REFERENCE_SOURCE
    assert "addReferenceProductStatus" in dashboard
    assert "getProductStatusEditorValues" in cost_grid
    assert "cost-file-input" in dashboard
    assert "excelBtn.disabled" not in dashboard
