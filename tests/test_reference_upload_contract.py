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


def test_reference_template_deduplicates_barcodes():
    repository = Path("repositories/reference.py").read_text(encoding="utf-8")
    dashboard = Path("api/v1/routers/dashboard.py").read_text(encoding="utf-8")
    assert "string_agg(DISTINCT eb.barcode" in repository
    assert "_dedupe_barcode_string" in REFERENCE_SOURCE
    assert "r[1] not in barcodes_map[eid]" in dashboard


def test_reference_top_query_save_queues_off_schedule_seasonality():
    seasonality_task = Path("tasks/seasonality_sync.py").read_text(encoding="utf-8")
    calculator = Path("scripts/calculate_product_seasonality.py").read_text(encoding="utf-8")
    collector = Path("scripts/collect_evirma_seasonality.py").read_text(encoding="utf-8")
    api_status = Path("api/v1/routers/api_status.py").read_text(encoding="utf-8")
    seasonality_router = Path("api/v1/routers/seasonality.py").read_text(encoding="utf-8")
    dashboard = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    cost_grid = Path("static/js/cost-grid.js").read_text(encoding="utf-8")

    assert "TOP_QUERY_FIELDS" in REFERENCE_SOURCE
    assert "_top_queries_changed" in REFERENCE_SOURCE
    assert "_propagate_top_queries_to_nm_id" in REFERENCE_SOURCE
    assert 'data.get("_top_query_edited")' in REFERENCE_SOURCE
    assert 'if "_top_query_edited" in data' in REFERENCE_SOURCE
    assert "_cost_price_save_payload" in REFERENCE_SOURCE
    assert "SAVEABLE_COST_PRICE_FIELDS" in REFERENCE_SOURCE
    assert "_active_reference_valid_from" in REFERENCE_SOURCE
    assert "Активная строка справочника не найдена для partial-save" in REFERENCE_SOURCE
    assert "save_fields is not None and not save_fields" in REFERENCE_SOURCE
    assert "save_data[field] = None" in REFERENCE_SOURCE
    assert "WHERE organization_id = :org_id" in REFERENCE_SOURCE
    assert "AND nm_id = :nm_id" in REFERENCE_SOURCE
    assert "top_query_1 = :top_query_1" in REFERENCE_SOURCE
    assert "_queue_reference_seasonality_collect" in REFERENCE_SOURCE
    assert '"source": "reference_top_query_save"' in REFERENCE_SOURCE
    assert '"nm_ids": filtered_nm_ids' in REFERENCE_SOURCE
    assert "seasonality_nm_ids.add" in REFERENCE_SOURCE
    assert "nm_ids: Optional[list[int]] = None" in seasonality_task
    assert "await collect_keywords(org_id, test_mode=False, dry_run=False, nm_ids=nm_ids)" in seasonality_task
    assert "_reset_product_seasonality" in seasonality_task
    assert "season_jan = 8.33" in seasonality_task
    assert "await calculate_products(org_id, dry_run=False, nm_ids=nm_ids)" in seasonality_task
    assert "_nm_ids_in_sql" in seasonality_task
    assert "AND rb.nm_id IN" in seasonality_task
    assert "jsonb_typeof(ps.seasonality_coefficients) = 'object'" in seasonality_task
    assert "ps.seasonality_coefficients <> '{}'::jsonb" in seasonality_task
    assert "result.rowcount" in seasonality_task
    assert "async def calculate_product_seasonality(" in calculator
    assert "nm_ids: Optional[List[int]] = None" in calculator
    assert "jsonb_typeof(seasonality_coefficients) = 'object'" in calculator
    assert "seasonality_coefficients <> '{}'::jsonb" in calculator
    assert "len(freq_history_monthly) >= 13 else freq_history_monthly[-12:]" in collector
    assert "jsonb_typeof(seasonality_coefficients) = 'object'" in api_status
    assert "jsonb_typeof(seasonality_coefficients) = 'object'" in seasonality_router
    assert "syncCostTopQueriesForNmId" in cost_grid
    assert "_costTopQueryEditedIds" in cost_grid
    assert "_costEditedFieldsById" in cost_grid
    assert "_fields: editedFields(data)" in cost_grid
    assert "rows.filter(edited).map" in cost_grid
    assert "function resetCostEditTracking()" in cost_grid
    assert "_top_query_edited: topQueryEdited(data)" in cost_grid
    assert "field === 'top_query_1' || field === 'top_query_2' || field === 'top_query_3'" in cost_grid
    assert "cost-grid.js?v=20260805-partial-save" in dashboard
    assert 'id="cost-save-btn"' in dashboard
    assert "_costSaving" in dashboard
    assert "СОХРАНЯЮ..." in dashboard


def test_reference_tags_and_collapsed_article_tree_are_wired():
    dashboard = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    cost_grid = Path("static/js/cost-grid.js").read_text(encoding="utf-8")
    repository = Path("repositories/reference.py").read_text(encoding="utf-8")

    assert '"Теги"' in REFERENCE_SOURCE
    assert "cp.tags" in repository
    assert '"tags": r[66] or ""' in repository
    assert '"tags": data.get("tags")' in REFERENCE_SOURCE
    assert "dataTree: true" in cost_grid
    assert "dataTreeStartExpanded: false" in cost_grid
    assert "function setCostTreeOpen(open)" in cost_grid
    assert 'id="flt-tags"' in dashboard
    assert "Без статуса" in dashboard
    assert "costTagsEditor" in cost_grid
    assert "splitReferenceTags" in dashboard
    assert "getMultiSelectValues" in dashboard
    assert 'id="flt-product-status" multiple' in dashboard
