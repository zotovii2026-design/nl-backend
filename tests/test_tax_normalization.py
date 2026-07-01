"""Тесты нормализации системы налогообложения и fallback на организацию."""
import pytest
from domain.unit_economics import normalize_tax_system, calculate_tax


class TestNormalizeTaxSystem:
    """Проверка маппинга человекочитаемых названий в коды."""

    def test_usn_dohody(self):
        assert normalize_tax_system("УСН Доходы") == "usn"

    def test_usn_dohody_rashody(self):
        assert normalize_tax_system("УСН Доходы-Расходы") == "usn_dr"

    def test_osno(self):
        assert normalize_tax_system("ОСНО") == "osn"

    def test_osn_short(self):
        assert normalize_tax_system("ОСН") == "osn"

    def test_already_code_usn(self):
        assert normalize_tax_system("usn") == "usn"

    def test_already_code_usn_dr(self):
        assert normalize_tax_system("usn_dr") == "usn_dr"

    def test_already_code_osn(self):
        assert normalize_tax_system("osn") == "osn"

    def test_cyrillic_usn(self):
        assert normalize_tax_system("УСН") == "usn"

    def test_empty_string(self):
        assert normalize_tax_system("") is None

    def test_none(self):
        assert normalize_tax_system(None) is None

    def test_ausn(self):
        assert normalize_tax_system("АУСН Доходы") == "usn"

    def test_lowercase_usn(self):
        assert normalize_tax_system("усн доходы") == "usn"


class TestCalculateTaxWithNormalization:
    """Проверка что calculate_tax корректно работает с кириллицей."""

    def test_usn_cyrillic(self):
        item = {"tax_system": "УСН Доходы", "tax_rate": 7, "cost_price": 100, "vat_rate": 0, "purchase_cost": 0}
        assert calculate_tax(item, 1000, 150) == 70.0

    def test_usn_code(self):
        item = {"tax_system": "usn", "tax_rate": 6, "cost_price": 100, "vat_rate": 0, "purchase_cost": 0}
        assert calculate_tax(item, 1000, 150) == 60.0

    def test_empty_tax_system_returns_zero(self):
        item = {"tax_system": "", "tax_rate": 7, "cost_price": 100, "vat_rate": 0, "purchase_cost": 0}
        assert calculate_tax(item, 1000, 150) == 0

    def test_none_tax_system_returns_zero(self):
        item = {"tax_system": None, "tax_rate": 7, "cost_price": 100, "vat_rate": 0, "purchase_cost": 0}
        assert calculate_tax(item, 1000, 150) == 0

    def test_usn_dr_cyrillic(self):
        item = {"tax_system": "УСН Доходы-Расходы", "tax_rate": 15, "cost_price": 500, "vat_rate": 0, "purchase_cost": 0}
        # income = 1000 - 150 - 500 = 350, tax = 350 * 15% = 52.5
        assert calculate_tax(item, 1000, 150) == 52.5
