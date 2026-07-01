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
