from pathlib import Path


DASHBOARD_SOURCE = Path("api/v1/nl.py").read_text(encoding="utf-8")


def test_marketer_loader_is_defined():
    assert "async function loadMarketer()" in DASHBOARD_SOURCE
    assert "/api/v1/nl/marketer/products?" in DASHBOARD_SOURCE


def test_async_sections_guard_removed_dom_nodes():
    required_guards = (
        "if (!document.getElementById('ad-views')) return;",
        "if (!thead || !tbody) return;",
        "if (!el || !el.options.length) return;",
        "if (!count || !body) return;",
        "if (!cards || !summary || !count || !header) return;",
        "const el = document.getElementById('wb-keys-list');\n    if (!el) return;",
    )

    for guard in required_guards:
        assert guard in DASHBOARD_SOURCE
