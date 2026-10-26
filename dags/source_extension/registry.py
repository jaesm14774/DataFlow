"""Source registry, scoring, schema bootstrap, and idempotent persistence."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

import pandas as pd

from common.config import sql_configure_path
from lib.get_sql import get_sql
from source_extension.adapters import (
    MAX_MAPPED_SOURCES,
    SOURCE_CONTRACTS,
    TWSE_DAILY_INDEX_FALLBACK_URL,
    TWSE_DAILY_QUOTES_FALLBACK_URL,
    TWSE_TOTAL_RETURN_FALLBACK_URL,
    _bounded_text,
    fetch_and_normalize,
    parse_number,
    parse_roc_date,
    parse_roc_month,
    validate_source_contract,
)
from source_extension.schema import create_schema


DATABASE_NAME = "crawler"
AUTO_ENABLE_THRESHOLD = 75.0
SUPPORTED_SCHEDULE_HINTS = frozenset(
    {"intraday", "daily", "daily_after_close", "monthly"}
)
SOURCE_RECORD_VERSION_COLUMNS = (
    "source_key", "source_id", "record_key", "record_hash", "target_domain",
    "entity_type", "entity_id", "event_type", "event_time", "effective_date",
    "title", "metric_name", "metric_value", "value_text", "unit", "currency",
    "source_url", "payload", "quality_status",
)


def _now() -> dt.datetime:
    return dt.datetime.now().replace(microsecond=0)


def _open_connection():
    connection = pd.read_csv(sql_configure_path, index_col="name")
    return get_sql(
        connection.loc["host", "value"],
        int(connection.loc["port", "value"]),
        connection.loc["user", "value"],
        connection.loc["password", "value"],
        DATABASE_NAME,
    )


def priority_score(
    purpose_score: float,
    joinability_score: float,
    authority_score: float,
    freshness_score: float,
    cost_score: float,
) -> float:
    """Return a 0-100 score used to prevent indiscriminate source growth."""

    score = (
        purpose_score * 30
        + joinability_score * 25
        + authority_score * 20
        + freshness_score * 15
        + cost_score * 10
    )
    return round(score, 2)


SOURCE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "source_key": "twse_company_events",
        "source_name": "TWSE listed company significant messages",
        "adapter_key": "twse_json",
        "endpoint_url": "https://openapi.twse.com.tw/v1/opendata/t187ap04_L",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "將上市公司重大訊息與既有新聞、股價、法說會資料對齊",
        "join_key": "company_code+event_date",
        "purpose_score": 1.0,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 1.0,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "intraday",
        "config": {"kind": "events", "market": "TWSE"},
        "fields": (
            ("公司代號", "entity_id", "string", "股票跨資料源關聯鍵", 1),
            ("發言日期", "event_time", "date", "重大訊息發生時間", 1),
            ("主旨", "title", "string", "事件分類與通知標題", 1),
            ("符合條款", "event_type_detail", "string", "監理事件細分類", 0),
            ("說明", "value_text", "text", "事件內容與人工追查依據", 0),
        ),
    },
    {
        "source_key": "tpex_company_events",
        "source_name": "TPEx OTC company significant messages",
        "adapter_key": "tpex_json",
        "endpoint_url": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "補齊上櫃公司重大訊息，與上市市場使用同一事件模型",
        "join_key": "company_code+event_date",
        "purpose_score": 1.0,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 1.0,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "intraday",
        "config": {"kind": "events", "market": "TPEx"},
        "fields": (
            ("SecuritiesCompanyCode", "entity_id", "string", "股票跨資料源關聯鍵", 1),
            ("發言日期", "event_time", "date", "重大訊息發生時間", 1),
            ("主旨", "title", "string", "事件分類與通知標題", 1),
            ("符合條款", "event_type_detail", "string", "監理事件細分類", 0),
            ("說明", "value_text", "text", "事件內容與人工追查依據", 0),
        ),
    },
    {
        "source_key": "twse_monthly_revenue",
        "source_name": "TWSE listed company monthly revenue",
        "adapter_key": "twse_json",
        "endpoint_url": "https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "用官方月營收補強既有營收表並統一上市櫃資料格式",
        "join_key": "company_code+report_month",
        "purpose_score": 1.0,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.9,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "intraday",
        "config": {
            "kind": "revenue",
            "market": "TWSE",
            "common_equity_only": True,
        },
        "fields": (
            ("公司代號", "entity_id", "string", "股票跨資料源關聯鍵", 1),
            ("資料年月", "effective_date", "month", "營收所屬月份", 1),
            ("營業收入-當月營收", "revenue_current", "decimal", "當月營收金額", 0),
            ("營業收入-去年當月營收", "revenue_prior_year_current", "decimal", "去年同月營收比較基準", 0),
            ("營業收入-去年同月增減(%)", "revenue_yoy_pct", "decimal", "年增率分析", 0),
            ("累計營業收入-前期比較增減(%)", "revenue_ytd_pct", "decimal", "累計營收趨勢", 0),
            ("備註", "revenue_note", "string", "公司申報營收變動說明", 0),
        ),
    },
    {
        "source_key": "tpex_monthly_revenue",
        "source_name": "TPEx OTC company monthly revenue",
        "adapter_key": "tpex_json",
        "endpoint_url": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "補齊上櫃公司官方月營收，支援上市櫃比較",
        "join_key": "company_code+report_month",
        "purpose_score": 1.0,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.9,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "intraday",
        "config": {
            "kind": "revenue",
            "market": "TPEx",
            "common_equity_only": True,
        },
        "fields": (
            ("公司代號", "entity_id", "string", "股票跨資料源關聯鍵", 1),
            ("資料年月", "effective_date", "month", "營收所屬月份", 1),
            ("營業收入-當月營收", "revenue_current", "decimal", "當月營收金額", 0),
            ("營業收入-去年當月營收", "revenue_prior_year_current", "decimal", "去年同月營收比較基準", 0),
            ("營業收入-去年同月增減(%)", "revenue_yoy_pct", "decimal", "年增率分析", 0),
            ("累計營業收入-前期比較增減(%)", "revenue_ytd_pct", "decimal", "累計營收趨勢", 0),
            ("備註", "revenue_note", "string", "公司申報營收變動說明", 0),
        ),
    },
    {
        "source_key": "twse_dividends",
        "source_name": "TWSE listed company dividends",
        "adapter_key": "twse_json",
        "endpoint_url": "https://openapi.twse.com.tw/v1/opendata/t187ap45_L",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "補強股利事件與現有股價、殖利率、通知任務的關聯",
        "join_key": "company_code+dividend_year+period",
        "purpose_score": 0.95,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.8,
        "cost_score": 0.95,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "daily",
        "config": {"kind": "dividend", "market": "TWSE"},
        "fields": (
            ("公司代號", "entity_id", "string", "股票跨資料源關聯鍵", 1),
            ("股利年度", "dividend_year", "integer", "股利所屬年度", 1),
            ("股東會日期", "effective_date", "date", "股利決議相關日期", 0),
            ("股東配發-盈餘分配之現金股利(元/股)", "cash_dividend_per_share", "decimal", "現金股利分析", 0),
        ),
    },
    {
        "source_key": "tpex_dividends",
        "source_name": "TPEx OTC company dividends",
        "adapter_key": "tpex_csv",
        "endpoint_url": "https://mopsfin.twse.com.tw/opendata/t187ap45_O.csv",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "補齊上櫃股利事件並與上市資料採同一模型",
        "join_key": "company_code+dividend_year+period",
        "purpose_score": 0.95,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.8,
        "cost_score": 0.95,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "daily",
        "config": {"kind": "dividend", "market": "TPEx"},
        "fields": (
            ("公司代號", "entity_id", "string", "股票跨資料源關聯鍵", 1),
            ("股利年度", "dividend_year", "integer", "股利所屬年度", 1),
            ("股東會日期", "effective_date", "date", "股利決議相關日期", 0),
            ("股東配發-盈餘分配之現金股利(元/股)", "cash_dividend_per_share", "decimal", "現金股利分析", 0),
        ),
    },
    {
        "source_key": "twse_tpex_sbl_available_daily",
        "source_name": "TWSE/TPEx daily available short sale shares",
        "adapter_key": "twse_json",
        "endpoint_url": "https://openapi.twse.com.tw/v1/SBL/TWT96U",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "收集個股可借券賣出股數，補足一般融資融券與主力交易欄位未涵蓋的借券供給資訊",
        "join_key": "market+company_code+snapshot_date",
        "purpose_score": 0.95,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 1.0,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "daily_after_close",
        "config": {
            "kind": "sbl_available",
            "market": "TWSE_TPEx",
            "filter_to_stock_master": True,
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "attribution": "臺灣證券交易所，{year}，集中交易市場融資融券額度調整之有價證券（上市上櫃股票當日可借券賣出股數），政府資料開放授權條款第1版",
        },
        "fields": (
            ("市場別", "market", "string", "區分上市與上櫃市場", 1),
            ("SecuritiesCompanyCode", "entity_id", "string", "個股跨資料源關聯鍵", 1),
            ("AvailableVolume", "available_short_shares", "integer", "可借券賣出股數", 1),
            ("SnapshotDate", "effective_date", "date", "每日收盤後快照日期", 1),
        ),
    },
    {
        "source_key": "tpex_securities_borrowing_daily",
        "source_name": "TPEx daily securities borrowing activity",
        "adapter_key": "tpex_json",
        "endpoint_url": "https://www.tpex.org.tw/openapi/v1/tpex_margin_sbl",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "補齊上櫃個股借券餘額、借券賣出、還券與調整資料；排除既有融券餘額與可借券賣出股數",
        "join_key": "company_code+date",
        "purpose_score": 0.95,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 1.0,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "daily_after_close",
        "config": {
            "kind": "securities_borrowing",
            "market": "TPEx",
            "filter_to_stock_master": True,
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "attribution": "證券櫃檯買賣中心，{year}，上櫃股票融券借券賣出餘額，政府資料開放授權條款第1版",
        },
        "fields": (
            ("市場別", "market", "string", "上櫃市場", 1),
            ("SecuritiesCompanyCode", "entity_id", "string", "個股跨資料源關聯鍵", 1),
            ("Date", "effective_date", "date", "借券資料日期", 1),
            ("SecuritiesBorrowingBalancePreviousDay", "securities_borrowing_balance_previous_day", "integer", "前日借券餘額", 0),
            ("SecuritiesBorrowingSale", "securities_borrowing_sale", "integer", "當日借券賣出股數", 0),
            ("SecuritiesBorrowingReturn", "securities_borrowing_return", "integer", "當日還券股數", 0),
            ("SecuritiesBorrowingAdjustment", "securities_borrowing_adjustment", "integer", "借券餘額調整股數", 0),
            ("SecuritiesBorrowingBalanceOfTheMarketDay", "securities_borrowing_balance", "integer", "當日借券餘額", 0),
        ),
    },
    {
        "source_key": "twse_director_holdings_monthly",
        "source_name": "TWSE monthly director and supervisor holdings",
        "adapter_key": "twse_json",
        "endpoint_url": "https://openapi.twse.com.tw/v1/opendata/t187ap11_L",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "保存上市公司董監及關係人逐人持股與設質月資料，作為公司治理變化訊號",
        "join_key": "company_code+report_month+role+person",
        "purpose_score": 0.9,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.9,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "monthly",
        "config": {
            "kind": "director_holdings",
            "market": "TWSE",
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "attribution": "臺灣證券交易所，{year}，上市公司董監事持股餘額明細資料，政府資料開放授權條款第1版",
        },
        "fields": (
            ("市場別", "market", "string", "上市市場", 1),
            ("公司代號", "entity_id", "string", "個股跨資料源關聯鍵", 1),
            ("出表日期", "published_date", "date", "官方資料出表日期", 1),
            ("資料年月", "effective_date", "month", "董監持股所屬年月", 1),
            ("職稱", "director_role", "string", "董監與經理人職稱", 1),
            ("姓名", "person_name", "string", "公開申報人姓名", 1),
            ("選任時持股", "shares_at_election", "integer", "選任時申報持股", 0),
            ("目前持股", "current_shares", "integer", "目前申報持股", 0),
            ("設質股數", "pledged_shares", "integer", "目前設質股數", 0),
            ("設質股數佔持股比例", "pledged_ratio_pct", "decimal", "個人持股設質比例", 0),
            ("內部人關係人目前持股合計", "related_person_current_shares", "integer", "內部人關係人持股合計", 0),
            ("內部人關係人設質股數", "related_person_pledged_shares", "integer", "內部人關係人設質股數", 0),
            ("內部人關係人設質比例", "related_person_pledged_ratio_pct", "decimal", "內部人關係人設質比例", 0),
        ),
    },
    {
        "source_key": "tpex_director_holdings_monthly",
        "source_name": "TPEx monthly director and supervisor holdings",
        "adapter_key": "tpex_json",
        "endpoint_url": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap11_O",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "保存上櫃公司董監及關係人逐人持股與設質月資料，補足大戶比例等彙總欄位無法提供的個人變化",
        "join_key": "company_code+report_month+role+person",
        "purpose_score": 0.9,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.9,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "monthly",
        "config": {
            "kind": "director_holdings",
            "market": "TPEx",
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "attribution": "證券櫃檯買賣中心，{year}，上櫃公司董監事持股餘額明細資料，政府資料開放授權條款第1版",
        },
        "fields": (
            ("市場別", "market", "string", "上櫃市場", 1),
            ("公司代號", "entity_id", "string", "個股跨資料源關聯鍵", 1),
            ("出表日期", "published_date", "date", "官方資料出表日期", 1),
            ("資料年月", "effective_date", "month", "董監持股所屬年月", 1),
            ("職稱", "director_role", "string", "董監與經理人職稱", 1),
            ("姓名", "person_name", "string", "公開申報人姓名", 1),
            ("選任時持股", "shares_at_election", "integer", "選任時申報持股", 0),
            ("目前持股", "current_shares", "integer", "目前申報持股", 0),
            ("設質股數", "pledged_shares", "integer", "目前設質股數", 0),
            ("設質股數佔持股比例", "pledged_ratio_pct", "decimal", "個人持股設質比例", 0),
            ("內部人關係人目前持股合計", "related_person_current_shares", "integer", "內部人關係人持股合計", 0),
            ("內部人關係人設質股數", "related_person_pledged_shares", "integer", "內部人關係人設質股數", 0),
            ("內部人關係人設質比例", "related_person_pledged_ratio_pct", "decimal", "內部人關係人設質比例", 0),
        ),
    },
    {
        "source_key": "tdcc_shareholder_distribution_weekly",
        "source_name": "TDCC weekly shareholder distribution by holding tier",
        "adapter_key": "twse_json",
        "endpoint_url": "https://openapi.tdcc.com.tw/v1/opendata/1-5",
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": "保存集保戶每週持股分級快照，供後續衡量持股集中與分散變化；初期只作背景，不投方向分數",
        "join_key": "security_code+effective_date+holding_tier",
        "purpose_score": 0.95,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.75,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "daily_after_close",
        "config": {
            "kind": "shareholder_distribution",
            "market": "BOTH",
            "common_equity_only": True,
            "classify_common_equity_codes": True,
            "require_full_common_equity_coverage": True,
            "source_cadence": "weekly",
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "attribution": "臺灣集中保管結算所，集保戶股權分散表，政府資料開放授權條款第1版",
        },
        "fields": (
            ("資料日期", "effective_date", "date", "每週持股資料有效日", 1),
            ("證券代號", "entity_id", "string", "官方證券代碼；以普通股主檔分類，其餘保留為未分類證券", 1),
            ("持股分級", "holding_tier", "integer", "集保持股分級序號", 1),
            ("人數", "holder_count", "integer", "該分級集保戶人數", 1),
            ("股數", "shares", "integer", "該分級集保股數", 1),
            ("占集保庫存數比例%", "share_of_tdcc_custody_pct", "decimal", "該分級股數占集保庫存比例", 1),
        ),
    },
    {
        "source_key": "cbc_fx_daily",
        "source_name": "Central Bank daily exchange rates",
        "adapter_key": "cbc_api_json",
        "endpoint_url": "https://cpx.cbc.gov.tw/API/DataAPI/Get?FileName=BP01D01",
        "target_domain": "macro",
        "entity_type": "macro",
        "purpose": "為國際股、金融股與跨市場分析提供官方匯率背景",
        "join_key": "currency_pair+effective_date",
        "purpose_score": 0.9,
        "joinability_score": 0.85,
        "authority_score": 1.0,
        "freshness_score": 1.0,
        "cost_score": 0.9,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "daily",
        "config": {"lookback_days": 365},
        "fields": (
            ("date", "effective_date", "date", "匯率資料日期", 1),
            ("pair", "entity_id", "string", "幣別對關聯鍵", 1),
            ("value", "metric_value", "decimal", "匯率數值", 1),
            ("units", "unit", "string", "資料單位", 0),
        ),
    },
)


def _quarterly_financial_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register batch official quarterly inputs for financial context and signals."""

    common_period_fields = (
        ("__company_code", "company_code", "string", "股票跨資料源關聯鍵", 1),
        ("__company_name", "company_name", "string", "官方公司名稱", 0),
        ("__reporting_year", "reporting_year", "integer", "財報年度（西元）", 1),
        ("__reporting_quarter", "reporting_quarter", "integer", "財報季度", 1),
        ("__report_period", "report_period_end", "date", "財報所屬季度末日", 1),
        ("__published_date", "published_date", "date", "官方資料出表日期", 0),
    )
    definitions: list[dict[str, Any]] = []

    for market, source_key in (
        ("TWSE", "twse_quarterly_profitability"),
        ("TPEx", "tpex_quarterly_profitability"),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} quarterly operating analysis",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": "補齊個股財報所屬期間累計營收與獲利率；有連續累計季度時推回單季營業利益率",
                "join_key": "market+company_code+fiscal_period",
                "purpose_score": 1.0,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 0.9,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily",
                "config": {
                    "kind": "quarterly_profitability",
                    "market": market,
                    "common_equity_only": True,
                },
                "fields": common_period_fields
                + (
                    ("__revenue_millions", "revenue_millions", "decimal", "截至財報期間累計營業收入（百萬元）", 0),
                    ("__gross_margin_pct", "gross_margin_pct", "decimal", "財報所屬期間累計毛利率（百分比）", 0),
                    ("__operating_margin_pct", "operating_margin_pct", "decimal", "財報所屬期間累計營業利益率（百分比）", 1),
                    ("__pretax_margin_pct", "pretax_margin_pct", "decimal", "財報所屬期間累計稅前純益率（百分比）", 0),
                    ("__net_margin_pct", "net_margin_pct", "decimal", "財報所屬期間累計稅後純益率（百分比）", 0),
                ),
            }
        )

    balance_sheet_sources = (
        *(("TWSE", sector) for sector in ("basi", "bd", "ci", "fh", "ins", "mim")),
        *(("TPEx", sector) for sector in ("bd", "ci")),
    )
    for market, sector in balance_sheet_sources:
        source_key = f"{market.lower()}_balance_sheet_{sector}"
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} quarterly balance sheet ({sector})",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": "補齊季度資產、負債、權益及流動項目，支援資產成長與財務結構訊號",
                "join_key": "market+company_code+fiscal_period",
                "purpose_score": 1.0,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 0.9,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily",
                "config": {
                    "kind": "quarterly_balance_sheet",
                    "market": market,
                    "industry_type": sector,
                },
                "fields": common_period_fields
                + (
                    ("__total_assets", "total_assets_thousand_twd", "decimal", "資產總額（新台幣千元）", 1),
                    ("__total_liabilities", "total_liabilities_thousand_twd", "decimal", "負債總額（新台幣千元）", 0),
                    ("__total_equity", "total_equity_thousand_twd", "decimal", "權益總額（新台幣千元）", 0),
                    ("__current_assets", "current_assets_thousand_twd", "decimal", "流動資產（新台幣千元）", 0),
                    ("__current_liabilities", "current_liabilities_thousand_twd", "decimal", "流動負債（新台幣千元）", 0),
                ),
            }
        )

    income_statement_sources = (
        *(('TWSE', sector) for sector in ("basi", "bd", "ci", "fh", "ins", "mim")),
        *(("TPEx", sector) for sector in ("bd", "ci")),
    )
    for market, sector in income_statement_sources:
        source_key = f"{market.lower()}_income_statement_{sector}"
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} quarterly income statement ({sector})",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": (
                    "批次收集全市場官方季綜合損益表與基本EPS，供候選訊號顯示最新財報脈絡；"
                    "不單獨增加多空分數，需有可比期歷史才計算變化"
                ),
                "join_key": "market+company_code+fiscal_period",
                "purpose_score": 1.0,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 0.9,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily",
                "config": {
                    "kind": "quarterly_income_statement",
                    "market": market,
                    "industry_type": sector,
                },
                "fields": common_period_fields
                + (
                    (
                        "__basic_eps",
                        "basic_eps_ntd_per_share",
                        "decimal",
                        "官方原列基本每股盈餘（新台幣元/股）",
                        1,
                    ),
                    (
                        "__parent_net_income_reported",
                        "parent_net_income_reported",
                        "decimal",
                        "官方原列歸屬母公司業主損益（不換算單位）",
                        0,
                    ),
                    (
                        "__reported_revenue",
                        "revenue_reported",
                        "decimal",
                        "官方原列營業收入（不換算單位）",
                        0,
                    ),
                ),
            }
        )

    return tuple(definitions)


def _insider_transfer_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register official listed/OTC insider transfer pre-notice batch feeds."""

    definitions: list[dict[str, Any]] = []
    for market in ("TWSE", "TPEx"):
        twse = market == "TWSE"
        adapter_key = "twse_json" if twse else "tpex_json"
        code_field = "公司代號" if twse else "SecuritiesCompanyCode"
        name_field = "公司名稱" if twse else "CompanyName"
        date_field = "出表日期" if twse else "Date"
        role_field = "申報人身分" if twse else "申請人身分"
        market_label = "上市" if twse else "上櫃"
        for report_type in ("planned", "untransferred"):
            source_key = f"{market.lower()}_insider_transfer_{report_type}"
            if report_type == "planned":
                fields = (
                    ("市場別", "market", "string", f"{market_label}市場", 1),
                    (code_field, "entity_id", "string", "個股跨資料源關聯鍵", 1),
                    (name_field, "company_name", "string", "官方公司名稱", 0),
                    (date_field, "published_date", "date", "官方資料出表日期", 1),
                    (role_field, "insider_role", "string", "官方申報人身分，不含姓名", 0),
                    ("預定轉讓方式及股數-轉讓方式", "transfer_method", "string", "申報轉讓方式", 1),
                    ("預定轉讓方式及股數-轉讓股數", "planned_transfer_shares", "integer", "該方式申報轉讓股數", 1),
                    ("每日於盤中交易最大得轉讓股數", "daily_max_transfer_shares", "integer", "申報每日轉讓上限", 0),
                    ("目前持有股數-自有持股", "current_direct_shares", "integer", "申報前自有持股", 0),
                    ("目前持有股數-保留運用決定權信託股數", "current_trust_shares", "integer", "申報前信託持股", 0),
                    ("預定轉讓總股數-自有持股", "planned_direct_shares", "integer", "預定轉讓自有持股", 0),
                    ("預定轉讓總股數-保留運用決定權信託股數", "planned_trust_shares", "integer", "預定轉讓信託持股", 0),
                    ("預定轉讓後持股-自有持股", "post_transfer_direct_shares", "integer", "申報後自有持股", 0),
                    ("預定轉讓後持股-保留運用決定權信託股數", "post_transfer_trust_shares", "integer", "申報後信託持股", 0),
                    ("有效轉讓期間", "transfer_period", "string", "官方申報有效轉讓期間", 1),
                )
            else:
                untransferred_direct = (
                    "未轉讓股數-自有持股" if twse else "未轉讓股數自有持股"
                )
                untransferred_trust = (
                    "未轉讓股數-保留運用決定權信託股數"
                    if twse else "未轉讓股數保留運用決定權信託股數"
                )
                current_direct = (
                    "目前持股-自有持股" if twse else "目前持有自有持股"
                )
                current_trust = (
                    "目前持股-保留運用決定權信託股數"
                    if twse else "目前持有保留運用決定權信託股數"
                )
                original_planned_direct = (
                    "原申報預定轉讓股數-自有持股"
                    if twse else "原申報預定轉讓股數自有持股"
                )
                original_planned_trust = (
                    "原申報預定轉讓股數-保留運用決定權信託股數"
                    if twse else "原申報預定轉讓股數保留運用決定權信託股數"
                )
                fields = (
                    ("市場別", "market", "string", f"{market_label}市場", 1),
                    (code_field, "entity_id", "string", "個股跨資料源關聯鍵", 1),
                    (name_field, "company_name", "string", "官方公司名稱", 0),
                    (date_field, "published_date", "date", "官方資料出表日期", 1),
                    (role_field, "insider_role", "string", "官方申報人身分，不含姓名", 0),
                    (untransferred_direct, "untransferred_direct_shares", "integer", "未轉讓自有持股數", 0),
                    (untransferred_trust, "untransferred_trust_shares", "integer", "未轉讓信託持股數", 0),
                    (current_direct, "current_direct_shares", "integer", "目前申報自有持股", 0),
                    (current_trust, "current_trust_shares", "integer", "目前申報信託持股", 0),
                    (original_planned_direct, "original_planned_direct_shares", "integer", "原申報預定自有持股轉讓數", 0),
                    (original_planned_trust, "original_planned_trust_shares", "integer", "原申報預定信託持股轉讓數", 0),
                    ("未轉讓理由", "untransferred_reason", "string", "官方未轉讓理由", 0),
                )
            endpoint = SOURCE_CONTRACTS[source_key][1]
            definitions.append(
                {
                    "source_key": source_key,
                    "source_name": f"{market} insider share transfer {report_type} report",
                    "adapter_key": adapter_key,
                    "endpoint_url": endpoint,
                    "target_domain": "stock",
                    "entity_type": "stock",
                    "purpose": (
                        "批次收集全市場官方內部人持股轉讓事前申報及未完成轉讓紀錄；"
                        "保存量化事實與上市櫃覆蓋，事前申報不視為成交或多空方向"
                    ),
                    "join_key": (
                        "market+company_code+published_date+role+transfer_details"
                    ),
                    "purpose_score": 0.95,
                    "joinability_score": 1.0,
                    "authority_score": 1.0,
                    "freshness_score": 0.8,
                    "cost_score": 1.0,
                    "approval_status": "approved",
                    "enabled": 1,
                    "required_credential": None,
                    "schedule_hint": "intraday",
                    "config": {
                        "kind": "insider_transfer",
                        "market": market,
                        "report_type": report_type,
                        "license": "Government Open Data License v1",
                        "license_url": "https://data.gov.tw/license",
                        "attribution": (
                            f"{'臺灣證券交易所' if twse else '證券櫃檯買賣中心'}，"
                            "{year}，內部人持股轉讓申報資料，政府資料開放授權條款第1版"
                        ),
                        "interpretation": "official_observation_only_until_outcome_validation",
                    },
                    "fields": fields,
                }
            )
    return tuple(definitions)


SOURCE_DEFINITIONS += _insider_transfer_source_definitions()
SOURCE_DEFINITIONS += _quarterly_financial_source_definitions()


def _quarterly_forecast_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register official quarterly forecast-versus-actual sources and events."""

    common_fields = (
        ("__company_code", "company_code", "string", "上市公司代碼", 1),
        ("__company_name", "company_name", "string", "官方公司名稱", 0),
        ("__reporting_year", "reporting_year", "integer", "財報年度（西元）", 1),
        ("__reporting_quarter", "reporting_quarter", "integer", "財報季度", 1),
        ("__report_period_end", "report_period_end", "date", "財報所屬季度末日", 1),
        ("__published_date", "published_date", "date", "官方資料出表日期", 1),
        ("__forecast_sequence", "forecast_sequence", "string", "官方財測序號", 0),
        ("__coverage_period", "coverage_period", "string", "財測涵蓋期間", 0),
    )
    tpex_event_fields = (
        ("__company_code", "company_code", "string", "上櫃公司代碼", 0),
        ("__company_name", "company_name", "string", "上櫃公司名稱", 0),
        ("__reporting_year", "reporting_year", "integer", "財報年度（西元）", 0),
        ("__reporting_quarter", "reporting_quarter", "integer", "財報季度", 0),
        ("__report_period_end", "report_period_end", "date", "財報所屬季度末日", 0),
        ("__snapshot_date", "snapshot_date", "date", "官方資料集快照出表日期", 0),
    )
    definitions: list[dict[str, Any]] = []
    source_specs = (
        {
            "source_key": "twse_quarterly_forecast_attainment",
            "kind": "quarterly_forecast_attainment",
            "endpoint_schedule": "daily_after_close",
            "source_name": "TWSE listed-company quarterly forecast attainment",
            "purpose": "保存上市公司各季財測達成 API 原始列供欄位語意核對；在實績與預測期間配對確認前不計達成率、不作通知或方向訊號",
            "value_fields": (
                ("__reported_actual", "reported_actual", "string", "TWSE 欄位所列實際數（官方原字串）", 1),
                ("__reported_actual_value", "reported_actual_value", "decimal", "TWSE 欄位所列實際數（可解析數值）", 0),
                ("__reported_forecast", "reported_forecast", "string", "TWSE 欄位所列預測數（官方原字串/區間，與實際期間配對待核）", 1),
            ),
        },
        {
            "source_key": "twse_quarterly_forecast_variance",
            "kind": "quarterly_forecast_variance",
            "endpoint_schedule": "intraday",
            "source_name": "TWSE listed-company quarterly forecast variance notices",
            "purpose": "收集TWSE官方已達當季10%或截至當季累計20%差異門檻的公司財測事件，作獨立事件通知、不納入多空分數",
            "value_fields": (
                ("__quarter_actual", "quarter_actual_reported", "string", "當季實際數（官方原字串）", 1),
                ("__quarter_actual_value", "quarter_actual_value", "decimal", "當季實際數（可解析數值）", 0),
                ("__cumulative_actual", "cumulative_actual_reported", "string", "截至當季累計實際數（官方原字串）", 1),
                ("__cumulative_actual_value", "cumulative_actual_value", "decimal", "截至當季累計實際數（可解析數值）", 0),
                ("__quarter_forecast", "quarter_forecast_reported", "string", "當季公司預測數（官方原字串/區間）", 1),
                ("__cumulative_forecast", "cumulative_forecast_reported", "string", "截至當季累計公司預測數（官方原字串/區間）", 1),
            ),
        },
        {
            "source_key": "tpex_quarterly_forecast_variance_events",
            "kind": "quarterly_forecast_variance_event",
            "endpoint_schedule": "intraday",
            "source_name": "TPEx OTC quarterly forecast variance threshold events",
            "purpose": (
                "收集政府開放資料目錄 20761 的上櫃財測差異門檻事件；"
                "只作方向未定的官方事件觀察，不納入多空分數"
            ),
            "market": "TPEx",
            "access_basis": "government_open_data_license_v1",
            "join_key": "company_code+report_period",
            "freshness_score": 0.55,
            "filter_to_stock_master": False,
            "common_fields": tpex_event_fields,
            "value_fields": (
                ("__threshold_basis", "threshold_basis", "string", "官方資料集列示門檻", 0),
            ),
        },
    )
    for spec in source_specs:
        source_key = spec["source_key"]
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": spec["source_name"],
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": spec["purpose"],
                "join_key": spec.get(
                    "join_key", "company_code+report_period+forecast_sequence"
                ),
                "purpose_score": 1.0,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": spec.get("freshness_score", 0.9),
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": spec["endpoint_schedule"],
                "config": {
                    "kind": spec["kind"],
                    "market": spec.get("market", "TWSE"),
                    "filter_to_stock_master": spec.get("filter_to_stock_master", True),
                    "access_basis": spec.get("access_basis", "official_public_openapi"),
                },
                "fields": spec.get("common_fields", common_fields) + spec["value_fields"],
            }
        )
    return tuple(definitions)


SOURCE_DEFINITIONS += _quarterly_forecast_source_definitions()


def _daytrade_eligibility_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register official listed and OTC day-trade target lists."""

    specifications = (
        (
            "TWSE", "twse_daytrade_eligibility_daily", "11661", "上市",
            ("", "Y"),
            (
                ("Date", "effective_date", "date", "官方當沖標的清單資料日", 1),
                ("Code", "entity_id", "string", "上市證券代碼", 1),
                ("Name", "security_name", "string", "上市證券名稱", 1),
                (
                    "Suspension", "daytrade_sell_then_cash_buy_suspension",
                    "string", "暫停現股賣出後現款買進當沖註記", 0,
                ),
            ),
        ),
        (
            "TPEx", "tpex_daytrade_eligibility_daily", "137680", "上櫃",
            ("", "＊", "*"),
            (
                ("資料日期", "effective_date", "date", "官方當沖標的清單資料日", 1),
                ("證券代號", "entity_id", "string", "上櫃證券代碼", 1),
                ("證券名稱", "security_name", "string", "上櫃證券名稱", 1),
                (
                    "暫停現股賣出後現款買進當沖註記",
                    "daytrade_sell_then_cash_buy_suspension",
                    "string", "暫停現股賣出後現款買進當沖註記", 0,
                ),
            ),
        ),
    )
    definitions: list[dict[str, Any]] = []
    for market_code, source_key, dataset_id, market_label, markers, fields in specifications:
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market_code} daily day-trade target eligibility list",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": (
                    f"以官方{market_label}當沖標的名單保存個股資格與限制註記；"
                    "只作交易可行性背景，不作成交量或方向訊號"
                ),
                "join_key": "market+company_code+effective_date",
                "purpose_score": 0.70,
                "joinability_score": 0.95,
                "authority_score": 1.0,
                "freshness_score": 0.95,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily",
                "config": {
                    "kind": "daytrade_eligibility",
                    "market": market_code,
                    "market_label": market_label,
                    "dataset_id": dataset_id,
                    "access_basis": "Government Open Data License v1",
                    "license_url": "https://data.gov.tw/license",
                    "valid_suspension_markers": list(markers),
                    "interpretation": "official_target_list_membership_only_not_volume_or_directional",
                    "freshness_gate": (
                        "effective_date_must_match_candidate_date_and_be_observed_before_scan"
                    ),
                    "missing_code_semantics": "unknown_until_completeness_is_proven",
                },
                "fields": fields,
            }
        )
    return tuple(definitions)


SOURCE_DEFINITIONS += _daytrade_eligibility_source_definitions()


def _daily_market_context_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register official daily benchmarks, credit, and regulatory context."""

    definitions: list[dict[str, Any]] = []
    for market, source_key, index_code, index_name, date_field, close_field in (
        (
            "TWSE", "twse_market_index_daily", "TAIEX",
            "發行量加權股價指數", "Date", "ClosingIndex",
        ),
        (
            "TPEx", "tpex_market_index_daily", "TPEX",
            "櫃買指數", "Date", "Close",
        ),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        config = {
            "kind": "market_index",
            "market": market,
            "index_code": index_code,
            "index_name": index_name,
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
        }
        if market == "TWSE":
            config["same_day_fallback"] = {
                "endpoint_url": TWSE_DAILY_INDEX_FALLBACK_URL,
                "dataset_id": "11755",
                "access_basis": "data_gov_dataset_11755_csv_resource",
            }
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} daily market index",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "macro",
                "entity_type": "macro",
                "purpose": "提供上市或上櫃官方大盤基準，將個股五日漲幅與所屬市場比較",
                "join_key": "index_code+trading_date",
                "purpose_score": 1.0,
                "joinability_score": 0.95,
                "authority_score": 1.0,
                "freshness_score": 0.95,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": config,
                "fields": (
                    (date_field, "effective_date", "date", "官方指數交易日", 1),
                    (close_field, "closing_index", "decimal", "官方收盤指數", 1),
                ),
            }
        )

    for market, source_key, index_code, index_name, date_field, value_field, dataset_id in (
        (
            "TWSE", "twse_market_total_return_index_daily", "TAIEX_TR",
            "發行量加權股價報酬指數", "Date", "TAIEXTotalReturnIndex", "11871",
        ),
        (
            "TPEx", "tpex_market_total_return_index_daily", "TPEX_TR",
            "櫃買報酬指數", "Date", "TPExTotalReturnIndex", "20219",
        ),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        config = {
            "kind": "market_total_return_index",
            "market": market,
            "index_code": index_code,
            "index_name": index_name,
            "value_field": value_field,
            "dataset_id": dataset_id,
            "access_basis": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "history_basis": "daily_latest_snapshot_accumulated_by_natural_runs",
            "interpretation": "non_directional_market_context_only",
        }
        if market == "TWSE":
            config["same_day_fallback"] = {
                "endpoint_url": TWSE_TOTAL_RETURN_FALLBACK_URL,
                "dataset_id": "11871",
                "access_basis": "data_gov_dataset_11871_csv_resource",
            }
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} daily total-return index",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "macro",
                "entity_type": "macro",
                "purpose": (
                    "以官方市場報酬指數提供全市場五交易日報酬背景；"
                    "不作個股方向分數或既有 outcome 基準"
                ),
                "join_key": "index_code+trading_date",
                "purpose_score": 0.95,
                "joinability_score": 0.95,
                "authority_score": 1.0,
                "freshness_score": 0.95,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": config,
                "fields": (
                    (date_field, "effective_date", "date", "官方報酬指數交易日", 1),
                    (value_field, "total_return_index", "decimal", "官方市場報酬指數", 1),
                ),
            }
        )

    daytrade_source_key = "tpex_daytrade_market_summary_daily"
    adapter_key, endpoint_url = SOURCE_CONTRACTS[daytrade_source_key]
    definitions.append(
        {
            "source_key": daytrade_source_key,
            "source_name": "TPEx daily OTC day-trade market aggregate",
            "adapter_key": adapter_key,
            "endpoint_url": endpoint_url,
            "target_domain": "macro",
            "entity_type": "macro",
            "purpose": (
                "保存官方上櫃市場當沖量與成交金額占比作市場活躍度背景；"
                "只有彙總資料，不代表個股或上市市場方向"
            ),
            "join_key": "market+effective_date",
            "purpose_score": 0.8,
            "joinability_score": 0.9,
            "authority_score": 1.0,
            "freshness_score": 0.8,
            "cost_score": 1.0,
            "approval_status": "approved",
            "enabled": 1,
            "required_credential": None,
            "schedule_hint": "daily_after_close",
            "config": {
                "kind": "market_daytrade_summary",
                "market": "TPEx",
                "dataset_id": "11627",
                "access_basis": "official_public_openapi",
                "license": "Government Open Data License v1",
                "license_url": "https://data.gov.tw/license",
                "interpretation": "otc_market_context_only_not_directional",
                "freshness_gate": "effective_date_must_match_signal_date",
                "revision_note": "source may revise trade-date values through T+2",
            },
            "fields": (
                ("Date", "effective_date", "date", "官方當沖統計交易日", 1),
                ("DayTradingVolume", "daytrade_volume_shares", "integer", "上櫃市場當沖成交股數", 1),
                (
                    "DayTradingVolumeOfTheMarket", "daytrade_volume_share_pct",
                    "decimal", "當沖成交股數占上櫃市場成交股數比率", 1,
                ),
                ("DayTradingValueOfBuys", "daytrade_buy_value_twd", "integer", "當沖買進成交金額（元）", 1),
                (
                    "DayTradingValueOfBuyOfTheMarket", "daytrade_buy_value_share_pct",
                    "decimal", "當沖買進成交金額占市場比率", 1,
                ),
                ("DayTradingValueOfSells", "daytrade_sell_value_twd", "integer", "當沖賣出成交金額（元）", 1),
                (
                    "DayTradingValueOfSellsOfTheMarket", "daytrade_sell_value_share_pct",
                    "decimal", "當沖賣出成交金額占市場比率", 1,
                ),
            ),
        }
    )

    for market, source_key in (
        ("TWSE", "twse_margin_balance_daily"),
        ("TPEx", "tpex_margin_balance_daily"),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} daily margin balance",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": "以官方每日融資融券餘額作槓桿籌碼風險背景，不單獨作為多空訊號",
                "join_key": "market+company_code+trading_date",
                "purpose_score": 0.9,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 1.0,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": {
                    "kind": "margin_balance",
                    "market": market,
                    "filter_to_stock_master": True,
                    "license": "Government Open Data License v1",
                    "license_url": "https://data.gov.tw/license",
                },
                "fields": (
                    ("Date", "effective_date", "date", "信用交易資料日期", 1),
                    ("SecuritiesCompanyCode", "entity_id", "string", "個股關聯代碼", 1),
                    ("margin_balance", "margin_purchase_balance", "decimal", "融資餘額", 0),
                    ("margin_previous_balance", "margin_previous_balance", "decimal", "前日融資餘額", 0),
                    ("margin_utilization_pct", "margin_utilization_pct", "decimal", "融資使用率", 0),
                    ("short_balance", "short_sale_balance", "decimal", "融券餘額", 0),
                    ("short_previous_balance", "short_previous_balance", "decimal", "前日融券餘額", 0),
                    ("short_utilization_pct", "short_utilization_pct", "decimal", "融券使用率", 0),
                ),
            }
        )
    for source_key, market, kind, source_name, purpose, fields, dataset_id in (
        (
            "twse_trading_attention_daily",
            "TWSE",
            "trading_attention",
            "TWSE daily trading-attention notices",
            "收集上市注意交易資訊，作異常交易風險背景，不作多空訊號",
            (
                ("Code", "entity_id", "string", "個股關聯代碼", 1),
                ("Name", "security_name", "string", "證券名稱", 0),
                ("Date", "effective_date", "date", "注意資訊公告日", 1),
                ("NumberOfAnnouncement", "source_number", "string", "API 原欄位，語意未確認", 0),
                ("TradingInfoForAttention", "attention_info", "text", "官方注意交易資訊", 0),
                ("ClosingPrice", "closing_price", "decimal", "公告列示收盤價", 0),
                ("PE", "pe_ratio", "decimal", "公告列示本益比", 0),
            ),
            None,
        ),
        (
            "twse_trading_disposition_daily",
            "TWSE",
            "trading_disposition",
            "TWSE daily trading-disposition notices",
            "收集上市處置期間與交易措施，供候選卡提示流動性及執行限制",
            (
                ("Code", "entity_id", "string", "個股關聯代碼", 1),
                ("Name", "security_name", "string", "證券名稱", 0),
                ("Date", "published_date", "date", "處置公告日", 1),
                ("NumberOfAnnouncement", "source_number", "string", "API 原欄位，語意未確認", 0),
                ("ReasonsOfDisposition", "reason", "string", "官方處置原因", 0),
                ("DispositionPeriod", "period", "string", "處置有效日期區間", 1),
                ("DispositionMeasures", "measures", "text", "撮合與交割措施", 0),
                ("Detail", "detail", "text", "官方處置說明全文", 0),
                ("LinkInformation", "link_information", "text", "官方延伸查詢說明", 0),
            ),
            None,
        ),
        (
            "tpex_trading_attention_daily",
            "TPEx",
            "trading_attention",
            "TPEx daily trading-attention notices",
            "收集上櫃注意交易資訊，作異常交易風險背景，不作多空訊號",
            (
                ("SecuritiesCompanyCode", "entity_id", "string", "個股關聯代碼", 1),
                ("CompanyName", "security_name", "string", "證券名稱", 0),
                ("Date", "effective_date", "date", "注意資訊公告日", 1),
                ("TradingInformation", "attention_info", "text", "官方注意交易資訊", 0),
                ("ClosePrice", "closing_price", "decimal", "公告列示收盤價", 0),
                ("PriceEarningRatio", "pe_ratio", "decimal", "公告列示本益比", 0),
            ),
            "11395",
        ),
        (
            "tpex_trading_disposition_daily",
            "TPEx",
            "trading_disposition",
            "TPEx daily trading-disposition notices",
            "收集上櫃處置期間與交易措施，供候選卡提示流動性及執行限制",
            (
                ("SecuritiesCompanyCode", "entity_id", "string", "個股關聯代碼", 1),
                ("CompanyName", "security_name", "string", "證券名稱", 0),
                ("Date", "published_date", "date", "處置公告日", 1),
                ("DispositionPeriod", "period", "string", "處置有效日期區間", 1),
                ("DispositionReasons", "reason", "string", "官方處置原因", 0),
                ("DisposalCondition", "measures", "text", "撮合與交割措施", 0),
            ),
            "11396",
        ),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": source_name,
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": purpose,
                "join_key": "market+company_code+effective_date",
                "purpose_score": 0.95,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 0.95,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": {
                    "kind": kind,
                    "market": market,
                    "filter_to_stock_master": True,
                    "access_basis": "official_public_openapi",
                    **(
                        {
                            "license": "Government Open Data License v1",
                            "license_url": "https://data.gov.tw/license",
                            "dataset_id": dataset_id,
                        }
                        if dataset_id
                        else {}
                    ),
                },
                "fields": fields,
            }
        )

    for market, source_key, source_name, dataset_id, fields in (
        (
            "TWSE",
            "twse_valuation_metrics_daily",
            "TWSE daily P/E, dividend yield, and price-to-book ratios",
            "11547",
            (
                ("Date", "effective_date", "date", "交易所估值資料日", 1),
                ("Code", "entity_id", "string", "上市證券代碼", 1),
                ("Name", "security_name", "string", "上市證券名稱", 0),
                ("PEratio", "pe_ratio", "decimal", "交易所列示本益比；空值保留為缺值", 0),
                ("DividendYield", "dividend_yield_pct", "decimal", "交易所列示殖利率百分比", 0),
                ("PBratio", "price_to_book", "decimal", "交易所列示股價淨值比", 0),
            ),
        ),
        (
            "TPEx",
            "tpex_valuation_metrics_daily",
            "TPEx daily P/E, dividend yield, and price-to-book ratios",
            "11373",
            (
                ("Date", "effective_date", "date", "交易所估值資料日", 1),
                ("SecuritiesCompanyCode", "entity_id", "string", "上櫃證券代碼", 1),
                ("CompanyName", "security_name", "string", "上櫃證券名稱", 0),
                ("PriceEarningRatio", "pe_ratio", "decimal", "交易所列示本益比；空值保留為缺值", 0),
                ("DividendPerShare", "dividend_per_share", "decimal", "交易所列示最新當季每股股利；不直接作殖利率推論", 0),
                ("YieldRatio", "dividend_yield_pct", "decimal", "交易所列示殖利率百分比", 0),
                ("PriceBookRatio", "price_to_book", "decimal", "交易所列示股價淨值比", 0),
            ),
        ),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": source_name,
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": (
                    "以交易所每日公布的本益比、殖利率與股價淨值比提供候選估值脈絡；"
                    "保留缺值與來源資料日，不單獨作多空方向或候選分數"
                ),
                "join_key": "market+company_code+effective_date",
                "purpose_score": 0.9,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 0.95,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": {
                    "kind": "valuation_metrics",
                    "market": market,
                    "license": "Government Open Data License v1",
                    "license_url": "https://data.gov.tw/license",
                    "dataset_id": dataset_id,
                    "access_basis": "official_public_openapi",
                    "interpretation": "context_only_until_outcome_validation",
                },
                "fields": fields,
            }
        )
    return tuple(definitions)


SOURCE_DEFINITIONS += _daily_market_context_source_definitions()


def _tpex_institutional_flow_source_definition() -> dict[str, Any]:
    """Register the licensed, daily TPEx stock-level institutional flow feed."""

    source_key = "tpex_institutional_flow_daily"
    adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
    return {
        "source_key": source_key,
        "source_name": "TPEx daily stock-level institutional net trading",
        "adapter_key": adapter_key,
        "endpoint_url": endpoint_url,
        "target_domain": "stock",
        "entity_type": "stock",
        "purpose": (
            "批次保存官方上櫃個股三大法人買賣超與來源有效日；先作可追溯背景資料，"
            "需經 PIT、覆蓋及多日期成熟 outcome 驗收後才可用作方向訊號"
        ),
        "join_key": "market+company_code+effective_date",
        "purpose_score": 0.95,
        "joinability_score": 1.0,
        "authority_score": 1.0,
        "freshness_score": 0.95,
        "cost_score": 1.0,
        "approval_status": "approved",
        "enabled": 1,
        "required_credential": None,
        "schedule_hint": "daily_after_close",
        "config": {
            "kind": "institutional_flow",
            "market": "TPEx",
            "market_label": "上櫃",
            "common_equity_only": True,
            "access_basis": "official_public_openapi",
            "dataset_id": "11856",
            "dataset_url": "https://data.gov.tw/dataset/11856",
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "publication_time_semantics": "not_provided_by_source",
            "freshness_gate": "effective_date_must_match_candidate_date",
            "interpretation": "otc_context_only_no_directional_vote_until_outcome_validation",
            "missing_value_semantics": "unknown_not_zero",
        },
        "fields": (
            ("Date", "effective_date", "date", "法人買賣資料有效日", 1),
            ("SecuritiesCompanyCode", "entity_id", "string", "上櫃證券代碼", 1),
            ("CompanyName", "security_name", "string", "上櫃證券名稱", 1),
            (
                "Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference",
                "foreign_net_shares_excluding_foreign_dealers",
                "integer",
                "外資及陸資不含外資自營商買賣超股數",
                1,
            ),
            (
                "ForeignDealers-Difference",
                "foreign_dealers_net_shares",
                "integer",
                "外資自營商買賣超股數（分項，不併入三大法人合計）",
                0,
            ),
            (
                "SecuritiesInvestmentTrustCompanies-Difference",
                "investment_trust_net_shares",
                "integer",
                "投信買賣超股數",
                1,
            ),
            (
                "Dealers-Difference",
                "dealers_net_shares",
                "integer",
                "自營商買賣超股數",
                1,
            ),
            (
                "TotalDifference",
                "three_institution_total_net_shares",
                "integer",
                "三大法人買賣超股數合計；核對外資、投信、自營商分項加總",
                1,
            ),
        ),
    }


SOURCE_DEFINITIONS += (_tpex_institutional_flow_source_definition(),)


def _daily_exright_preannouncement_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register both exchanges' official batch ex-right/ex-dividend notices."""

    definitions: list[dict[str, Any]] = []
    for market, source_key, dataset_id, fields, update_note in (
        (
            "TWSE",
            "twse_exright_preannouncement_daily",
            "89748",
            (
                ("Date", "effective_date", "date", "預告除權息交易日", 1),
                ("Code", "entity_id", "string", "上市證券代碼", 1),
                ("Name", "security_name", "string", "證券名稱", 0),
                ("Exdividend", "action", "string", "除權、除息或權息別", 1),
                ("CashDividend", "cash_dividend_per_share", "decimal", "公告現金股利；空值不補零", 0),
                ("StockDividendRatio", "stock_dividend_ratio", "decimal", "無償配股率；空值不補零", 0),
                ("SubscriptionRatio", "subscription_ratio", "decimal", "現金增資配股率；空值不補零", 0),
                ("SubscriptionPricePerShare", "subscription_price_per_share", "decimal", "現金增資認購價；空值不補零", 0),
            ),
            "官方資料集標示不定期更新；每日輪詢成功只證明該次批次可讀，不證明公告名單已完整更新",
        ),
        (
            "TPEx",
            "tpex_exright_preannouncement_daily",
            "11634",
            (
                ("ExRrightsExDividendDate", "effective_date", "date", "預告除權息交易日", 1),
                ("SecuritiesCompanyCode", "entity_id", "string", "上櫃證券代碼", 1),
                ("CompanyName", "security_name", "string", "證券名稱", 0),
                ("ExRrightsExDividend", "action", "string", "除權、除息或權息別", 1),
                ("CashDividend", "cash_dividend_per_share", "decimal", "公告現金股利；空值不補零", 0),
                ("StockDividendRatio", "stock_dividend_ratio", "decimal", "無償配股率；空值不補零", 0),
                ("SubscriptionRatioToNewSharesIssued", "subscription_ratio", "decimal", "現金增資配股率；空值不補零", 0),
                ("SubscriptionPricePerShare", "subscription_price_per_share", "decimal", "現金增資認購價；空值不補零", 0),
            ),
            "官方資料集標示每日更新；仍需用自然批次有效日和證券主檔匹配數驗收",
        ),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} daily ex-right and ex-dividend preannouncements",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": (
                    "以交易所全市場批次公告辨識候選及未來報酬視窗中的除權息公司行動；"
                    "只作事件與結果品質脈絡，不投多空方向票"
                ),
                "join_key": "market+company_code+exright_date",
                "purpose_score": 0.95,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 0.8 if market == "TWSE" else 0.95,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": {
                    "kind": "corporate_action_preannouncement",
                    "market": market,
                    "dataset_id": dataset_id,
                    "access_basis": "official_openapi",
                    "license": "Government Open Data License v1",
                    "license_url": "https://data.gov.tw/license",
                    "update_note": update_note,
                    "interpretation": "event_context_only_not_directional",
                    "missing_code_semantics": "unknown_unless_fresh_complete_batch_is_verified",
                },
                "fields": fields,
            }
        )
    return tuple(definitions)


SOURCE_DEFINITIONS += _daily_exright_preannouncement_source_definitions()


def _taifex_market_position_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register the official exchange's listed/OTC index-futures position context."""

    source_key = "taifex_index_futures_positions_daily"
    adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
    return (
        {
            "source_key": source_key,
            "source_name": "TAIFEX index-futures institutional positions",
            "adapter_key": adapter_key,
            "endpoint_url": endpoint_url,
            "target_domain": "macro",
            "entity_type": "macro",
            "purpose": (
                "保存期交所上市與上櫃指數期貨三大法人淨未平倉，作為收盤市場部位背景；"
                "法人為彙總類別，不單獨計入個股方向分數"
            ),
            "join_key": "contract_code+institution_type+effective_date",
            "purpose_score": 0.95,
            "joinability_score": 0.95,
            "authority_score": 1.0,
            "freshness_score": 0.9,
            "cost_score": 0.95,
            "approval_status": "approved",
            "enabled": 1,
            "required_credential": None,
            "schedule_hint": "daily_after_close",
            "config": {
                "kind": "taifex_index_futures_position",
                "contract_codes": ["臺股期貨", "櫃買指數期貨"],
                "access_basis": "official_openapi",
                "documentation_url": "https://openapi.taifex.com.tw/",
                "interpretation": "market_context_only_until_outcome_validation",
            },
            "fields": (
                ("Date", "effective_date", "date", "期交所資料日期", 1),
                ("ContractCode", "contract_code", "string", "期貨商品", 1),
                ("Item", "institution_type", "string", "法人彙總類別", 1),
                ("TradingVolume(Net)", "net_trading_lots", "integer", "多空交易口數淨額", 0),
                (
                    "TradingValue(Net)(Thousands)", "net_trading_value_thousands",
                    "decimal", "多空交易契約金額淨額（千元）", 0,
                ),
                ("OpenInterest(Net)", "net_open_interest_lots", "integer", "多空未平倉口數淨額", 1),
                (
                    "ContractValueofOpenInterest(Net)(Thousands)",
                    "net_open_interest_value_thousands", "decimal",
                    "多空未平倉契約金額淨額（千元）", 0,
                ),
            ),
        },
    )


SOURCE_DEFINITIONS += _taifex_market_position_source_definitions()


def _taifex_option_position_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register aggregate TAIFEX index-option calls/puts by institution class."""

    source_key = "taifex_index_options_positions_daily"
    adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
    return (
        {
            "source_key": source_key,
            "source_name": "TAIFEX index-options institutional positions",
            "adapter_key": adapter_key,
            "endpoint_url": endpoint_url,
            "target_domain": "macro",
            "entity_type": "macro",
            "purpose": (
                "保存臺指選擇權買權/賣權三類法人淨未平倉，作為收盤市場部位背景；"
                "多腿策略與法人彙總不可直接解讀為多空預測或個股訊號"
            ),
            "join_key": "contract_code+call_put+institution_type+effective_date",
            "purpose_score": 0.9,
            "joinability_score": 0.95,
            "authority_score": 1.0,
            "freshness_score": 0.9,
            "cost_score": 0.95,
            "approval_status": "approved",
            "enabled": 1,
            "required_credential": None,
            "schedule_hint": "daily_after_close",
            "config": {
                "kind": "taifex_index_option_position",
                "contract_codes": ["臺指選擇權"],
                "call_put_types": ["CALL", "PUT"],
                "access_basis": "official_openapi",
                "documentation_url": "https://openapi.taifex.com.tw/",
                "interpretation": "market_context_only_until_outcome_validation",
            },
            "fields": (
                ("Date", "effective_date", "date", "期交所資料日期", 1),
                ("ContractCode", "contract_code", "string", "選擇權商品", 1),
                ("CallPut", "call_put", "string", "買權或賣權", 1),
                ("Item", "institution_type", "string", "法人彙總類別", 1),
                ("TradingVolume(Net)", "net_trading_lots", "integer", "多空交易口數淨額", 0),
                (
                    "TradingValue(Net)(Thousands)", "net_trading_value_thousands",
                    "decimal", "多空交易契約金額淨額（千元）", 0,
                ),
                ("OpenInterest(Net)", "net_open_interest_lots", "integer", "多空未平倉口數淨額", 1),
                (
                    "ContractValueofOpenInterest(Net)(Thousands)",
                    "net_open_interest_value_thousands", "decimal",
                    "多空未平倉契約金額淨額（千元）", 0,
                ),
            ),
        },
    )


SOURCE_DEFINITIONS += _taifex_option_position_source_definitions()


def _taifex_futures_large_trader_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register TAIFEX's daily, exchange-wide large-trader OI batch feed."""

    source_key = "taifex_futures_large_trader_positions_daily"
    adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
    return (
        {
            "source_key": source_key,
            "source_name": "TAIFEX futures large-trader open interest",
            "adapter_key": adapter_key,
            "endpoint_url": endpoint_url,
            "target_domain": "macro",
            "entity_type": "macro",
            "purpose": (
                "批次保存各期貨契約前五/前十名買方與賣方未平倉口數及市場總未平倉；"
                "買賣兩側分開排名，不相減成淨部位，僅作市場集中度背景"
            ),
            "join_key": "contract+settlement_month+trader_type+effective_date",
            "purpose_score": 0.9,
            "joinability_score": 0.95,
            "authority_score": 1.0,
            "freshness_score": 0.75,
            "cost_score": 1.0,
            "approval_status": "approved",
            "enabled": 1,
            "required_credential": None,
            "schedule_hint": "daily_after_close",
            "config": {
                "kind": "taifex_futures_large_trader_oi",
                "access_basis": "official_openapi",
                "dataset_id": "11337",
                "dataset_url": "https://data.gov.tw/dataset/11337",
                "license": "Government Open Data License v1",
                "license_url": "https://data.gov.tw/license",
                "catalog_update_frequency_note": (
                    "data.gov.tw metadata says monthly while the official API has returned "
                    "daily Date rows; enforce freshness from effective_date"
                ),
                "interpretation": "market_context_only_not_directional",
                "missing_value_semantics": "unknown_not_zero",
                "source_field_aliases": {
                    "Date": ["日期"],
                    "Contract": ["契約"],
                    "ContractName": ["商品名稱(契約名稱)"],
                    "SettlementMonth": ["到期月份(週別)"],
                    "TypeOfTraders": ["交易人類別"],
                    "Top5Buy": ["前五大交易人買方數量"],
                    "Top5Sell": ["前五大交易人賣方數量"],
                    "Top10Buy": ["前十大交易人買方數量"],
                    "Top10Sell": ["前十大交易人賣方數量"],
                    "OIOfMarket": ["全市場未沖銷部位數"],
                },
            },
            "fields": (
                ("Date", "effective_date", "date", "期交所資料日期", 1),
                ("Contract", "contract", "string", "期貨商品代碼", 1),
                ("ContractName", "contract_name", "string", "期貨商品名稱", 0),
                ("SettlementMonth", "settlement_month", "string", "到期月份代碼", 1),
                ("TypeOfTraders", "trader_type", "string", "交易人類別代碼", 1),
                ("Top5Buy", "top5_buy_lots", "integer", "前五名買方未平倉口數", 1),
                ("Top5Sell", "top5_sell_lots", "integer", "前五名賣方未平倉口數", 1),
                ("Top10Buy", "top10_buy_lots", "integer", "前十名買方未平倉口數", 1),
                ("Top10Sell", "top10_sell_lots", "integer", "前十名賣方未平倉口數", 1),
                ("OIOfMarket", "market_open_interest_lots", "integer", "市場總未平倉口數", 1),
            ),
        },
    )


SOURCE_DEFINITIONS += _taifex_futures_large_trader_source_definitions()


def _daily_market_quote_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register licensed daily quote feeds for a consistently defined breadth cohort."""

    definitions: list[dict[str, Any]] = []
    for (
        market,
        source_key,
        source_name,
        code_field,
        name_field,
        close_field,
        volume_field,
        dataset_url,
    ) in (
        (
            "TWSE", "twse_equity_quotes_daily", "TWSE daily equity quotes",
            "Code", "Name", "ClosingPrice", "TradeVolume",
            "https://data.gov.tw/dataset/11549",
        ),
        (
            "TPEx", "tpex_equity_quotes_daily", "TPEx daily equity quotes",
            "SecuritiesCompanyCode", "CompanyName", "Close", "TradingShares",
            "https://data.gov.tw/dataset/11370",
        ),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        config = {
            "kind": "market_quote",
            "market": market,
            "filter_to_stock_master": True,
            "common_equity_only": True,
            "access_basis": "official_openapi",
            "license": "Government Open Data License v1",
            "license_url": "https://data.gov.tw/license",
            "dataset_url": dataset_url,
        }
        if market == "TWSE":
            config["same_day_fallback"] = {
                "endpoint_url": TWSE_DAILY_QUOTES_FALLBACK_URL,
                "dataset_id": "11549",
                "access_basis": "data_gov_dataset_11549_csv_resource",
            }
        definitions.append(
            {
                "source_key": source_key,
                "source_name": source_name,
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": "以政府開放個股日行情中的漲跌欄，計算 CFI ES* 證券的市場廣度背景",
                "join_key": "market+company_code+trading_date",
                "purpose_score": 0.9,
                "joinability_score": 1.0,
                "authority_score": 1.0,
                "freshness_score": 0.95,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": config,
                "fields": (
                    ("Date", "effective_date", "date", "行情資料日期", 1),
                    (code_field, "entity_id", "string", "證券代號", 1),
                    ("Change", "daily_price_change", "decimal", "官方每日漲跌值", 1),
                    (close_field, "closing_price", "decimal", "收盤價", 0),
                    (volume_field, "trade_volume", "integer", "成交股數", 0),
                    (name_field, "security_name", "string", "證券名稱", 0),
                ),
            }
        )
    return tuple(definitions)


SOURCE_DEFINITIONS += _daily_market_quote_source_definitions()


def _tpex_foreign_investor_holding_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register TPEx's open-licensed daily foreign ownership ranking batch."""

    source_key = "tpex_foreign_investor_holdings_daily"
    adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
    return (
        {
            "source_key": source_key,
            "source_name": "TPEx daily foreign investor holding ratios",
            "adapter_key": adapter_key,
            "endpoint_url": endpoint_url,
            "target_domain": "stock",
            "entity_type": "stock",
            "purpose": (
                "保存上櫃普通股僑外資及陸資持股比例日快照；僅作持股結構背景，"
                "不解讀為當日買賣流或個股方向票"
            ),
            "join_key": "market+company_code+effective_date",
            "purpose_score": 0.85,
            "joinability_score": 0.95,
            "authority_score": 1.0,
            "freshness_score": 0.9,
            "cost_score": 1.0,
            "approval_status": "approved",
            "enabled": 1,
            "required_credential": None,
            "schedule_hint": "daily_after_close",
            "config": {
                "kind": "foreign_investor_holding",
                "market": "TPEX",
                "market_label": "上櫃",
                "common_equity_only": True,
                "access_basis": "data_gov_resource",
                "dataset_id": "20214",
                "dataset_url": "https://data.gov.tw/dataset/20214",
                "license": "Government Open Data License v1",
                "license_url": "https://data.gov.tw/license",
                "publication_time_semantics": "not_provided_by_resource_event_time_null",
                "freshness_gate": "effective_date_must_match_candidate_date",
                "interpretation": "otc_context_only_not_net_flow_or_directional_vote",
                "missing_value_semantics": "unknown_not_zero",
            },
            "fields": (
                ("資料日期", "effective_date", "date", "持股資料有效日", 1),
                ("排行", "rank", "integer", "持股比例排行", 1),
                ("代號", "entity_id", "string", "上櫃證券代碼", 1),
                ("名稱", "security_name", "string", "上櫃證券名稱", 1),
                ("發行股數", "issued_shares", "integer", "發行股數", 1),
                (
                    "僑外資及陸資尚可投資股數", "foreign_available_shares", "integer",
                    "僑外資及陸資尚可投資股數", 1,
                ),
                (
                    "僑外資及陸資持有股數", "foreign_held_shares", "integer",
                    "僑外資及陸資持有股數", 1,
                ),
                (
                    "僑外資及陸資尚可投資比率", "foreign_available_ratio_pct", "decimal",
                    "僑外資及陸資尚可投資比率", 1,
                ),
                (
                    "僑外資及陸資持股比率", "foreign_holding_ratio_pct", "decimal",
                    "僑外資及陸資持股比率", 1,
                ),
                (
                    "法令投資上限比率", "legal_investment_cap_pct", "decimal",
                    "法令投資上限比率", 1,
                ),
                ("備註", "note", "string", "來源備註", 0),
            ),
        },
    )


SOURCE_DEFINITIONS += _tpex_foreign_investor_holding_source_definitions()


def _major_shareholder_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register the official daily listed/OTC over-10% shareholder batches."""

    definitions = []
    for market, source_key, dataset_id, date_field, code_field, name_field in (
        ("TWSE", "twse_major_shareholders_daily", "18422", "出表日期", "公司代號", "公司名稱"),
        ("TPEX", "tpex_major_shareholders_daily", "18423", "Date", "SecuritiesCompanyCode", "CompanyName"),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        market_label = "上市" if market == "TWSE" else "上櫃"
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} daily shareholders over 10 percent",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": (
                    f"保存{market_label}公司官方揭露持股超過10%的大股東名稱快照；"
                    "來源沒有股數或精確持股比例，僅作身分變化背景，不投方向分數"
                ),
                "join_key": "market+company_code+effective_date+holder_name",
                "purpose_score": 0.85,
                "joinability_score": 0.95,
                "authority_score": 1.0,
                "freshness_score": 0.8,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": {
                    "kind": "major_shareholders",
                    "market": market,
                    "access_basis": "data_gov_resource",
                    "dataset_id": dataset_id,
                    "dataset_url": f"https://data.gov.tw/dataset/{dataset_id}",
                    "license": "Government Open Data License v1",
                    "license_url": "https://data.gov.tw/license",
                    "source_cadence": "daily",
                    "disclosure_scope": "reported_shareholders_over_10pct_only",
                    "publication_time_semantics": "report_date_only_event_time_null",
                    "known_limit": (
                        "holder_identity_only_no_share_count_or_exact_ratio; "
                        "absence_is_unknown_not_zero"
                    ),
                    "interpretation": (
                        "snapshot_context_only_until_change_history_and_outcomes_validate"
                    ),
                    "missing_value_semantics": "unknown_not_zero",
                },
                "fields": (
                    (date_field, "effective_date", "date", "官方資料出表日期", 1),
                    (code_field, "entity_id", "string", f"{market_label}證券代碼", 1),
                    (name_field, "company_name", "string", "公司名稱", 1),
                    ("大股東名稱", "holder_name", "string", "官方揭露的大股東名稱", 1),
                ),
            }
        )
    return tuple(definitions)


SOURCE_DEFINITIONS += _major_shareholder_source_definitions()


def _director_shareholding_shortfall_source_definitions() -> tuple[dict[str, Any], ...]:
    """Register sparse official listed/OTC statutory-shortfall batches."""

    definitions = []
    for market, source_key, dataset_id in (
        ("TWSE", "twse_director_shareholding_shortfall_daily", "20767"),
        ("TPEX", "tpex_director_shareholding_shortfall_daily", "20768"),
    ):
        adapter_key, endpoint_url = SOURCE_CONTRACTS[source_key]
        market_label = "上市" if market == "TWSE" else "上櫃"
        director_prefix = (
            "全體董事" if market == "TWSE" else "全體董事不包含獨立董事"
        )
        fields = [
            ("出表日期", "effective_date", "date", "官方資料出表日期", 1),
            ("公司代號", "entity_id", "string", f"{market_label}證券代碼", 1),
            ("公司名稱", "company_name", "string", "公司名稱", 1),
            ("已發行股份總額", "issued_shares", "integer", "已發行股份總額", 1),
        ]
        for suffix, target, description, required in (
            ("應持有股數", "director_required_shares", "董事法定應持股數", 1),
            ("本人實際持有股數", "director_actual_shares", "董事本人實持股數", 1),
            ("法人代表人分戶集保股數", "director_legal_representative_shares", "董事法人代表分戶集保股數", 0),
            ("保留運用決定權信託股數", "director_trust_shares", "董事保留運用決定權信託股數", 0),
            ("不足股數", "director_shortfall_shares", "董事持股不足股數", 1),
        ):
            fields.append((f"{director_prefix}{suffix}", target, "integer", description, required))
        for suffix, target, description in (
            ("應持有股數", "supervisor_required_shares", "監察人法定應持股數"),
            ("本人實際持有股數", "supervisor_actual_shares", "監察人本人實持股數"),
            ("法人代表人分戶集保股數", "supervisor_legal_representative_shares", "監察人法人代表分戶集保股數"),
            ("保留運用決定權信託股數", "supervisor_trust_shares", "監察人保留運用決定權信託股數"),
            ("不足股數", "supervisor_shortfall_shares", "監察人持股不足股數"),
        ):
            fields.append((f"全體監察人{suffix}", target, "integer", description, 0))
        fields.append(("持股不足已通知其董監", "notification_status", "string", "官方通知狀態文字", 0))
        definitions.append(
            {
                "source_key": source_key,
                "source_name": f"{market} statutory director shareholding shortfall list",
                "adapter_key": adapter_key,
                "endpoint_url": endpoint_url,
                "target_domain": "stock",
                "entity_type": "stock",
                "purpose": (
                    f"批次保存官方列示的{market_label}董監持股不足法定成數公司，"
                    "只附治理風險脈絡、不投方向票；名單外狀態未知"
                ),
                "join_key": "market+company_code+report_date",
                "purpose_score": 0.9,
                "joinability_score": 0.95,
                "authority_score": 1.0,
                "freshness_score": 0.7,
                "cost_score": 1.0,
                "approval_status": "approved",
                "enabled": 1,
                "required_credential": None,
                "schedule_hint": "daily_after_close",
                "config": {
                    "kind": "director_shareholding_shortfall",
                    "market": market,
                    "access_basis": "data_gov_openapi_resource",
                    "dataset_id": dataset_id,
                    "dataset_url": f"https://data.gov.tw/dataset/{dataset_id}",
                    "license": "Government Open Data License v1",
                    "license_url": "https://data.gov.tw/license",
                    "source_cadence": "irregular",
                    "disclosure_scope": "currently_reported_statutory_shareholding_shortfalls_only",
                    "publication_time_semantics": "report_date_only_event_time_null",
                    "missing_value_semantics": "unknown_not_zero_or_compliance",
                    "interpretation": "governance_risk_context_only_not_directional_score",
                },
                "fields": tuple(fields),
            }
        )
    return tuple(definitions)


SOURCE_DEFINITIONS += _director_shareholding_shortfall_source_definitions()


def _trading_status_source_definitions():
    definitions = []
    for market, key, dataset_id in (
        ("TWSE", "twse_trading_status_daily", "11760"),
        ("TPEx", "tpex_trading_status_daily", "11736"),
    ):
        adapter, endpoint = SOURCE_CONTRACTS[key]
        definitions.append({
            "source_key": key, "source_name": f"{market} official trading restrictions",
            "adapter_key": adapter, "endpoint_url": endpoint, "target_domain": "stock",
            "entity_type": "stock", "purpose": "停止交易、變更交易與分盤限制直接覆寫候選操作建議；完整名單快照保留解除狀態",
            "join_key": "market+company_code+snapshot_date",
            "purpose_score": 1.0, "joinability_score": 1.0, "authority_score": 1.0,
            "freshness_score": 0.9, "cost_score": 1.0,
            "approval_status": "approved", "enabled": 1, "required_credential": None,
            "schedule_hint": "daily_after_close",
            "config": {"kind": "trading_status", "market": market,
                "filter_to_stock_master": True, "dataset_id": dataset_id,
                "license": "Government Open Data License v1",
                "license_url": "https://data.gov.tw/license",
                "event_time_max_age_days": 7,
                "date_basis": "official_report_date" if market == "TPEx" else "observed_date_no_report_date",
                "interpretation": "risk_action_guard_not_directional_vote"},
            "fields": (("__entity_id", "entity_id", "string", "主檔對齊之限制名單快照證券代號", 1),),
        })
    return tuple(definitions)


SOURCE_DEFINITIONS += _trading_status_source_definitions()
SOURCE_DEFINITIONS_BY_KEY = {item["source_key"]: item for item in SOURCE_DEFINITIONS}


def _runtime_definition_matches(source: dict[str, Any]) -> bool:
    definition = SOURCE_DEFINITIONS_BY_KEY.get(source.get("source_key"))
    return bool(
        definition
        and validate_source_contract(source)
        and source.get("adapter_key") == definition["adapter_key"]
        and source.get("endpoint_url") == definition["endpoint_url"]
        and source.get("target_domain") == definition["target_domain"]
        and source.get("entity_type") == definition["entity_type"]
        and source.get("config") == definition["config"]
        and source.get("schedule_hint") == definition["schedule_hint"]
        and source.get("schedule_hint") in SUPPORTED_SCHEDULE_HINTS
    )


def _bootstrap_catalog(cursor) -> None:
    now = _now()
    source_sql = """
        INSERT INTO source_registry (
            source_key, source_name, adapter_key, endpoint_url, target_domain,
            entity_type, purpose, join_key, purpose_score, joinability_score, authority_score,
            freshness_score, cost_score, priority_score, approval_status, enabled,
            required_credential, schedule_hint, config_text, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            source_name=VALUES(source_name), adapter_key=VALUES(adapter_key),
            endpoint_url=VALUES(endpoint_url), target_domain=VALUES(target_domain),
            entity_type=VALUES(entity_type),
            purpose=VALUES(purpose), join_key=VALUES(join_key),
            purpose_score=VALUES(purpose_score), joinability_score=VALUES(joinability_score),
            authority_score=VALUES(authority_score), freshness_score=VALUES(freshness_score),
            cost_score=VALUES(cost_score), priority_score=VALUES(priority_score),
            required_credential=VALUES(required_credential), schedule_hint=VALUES(schedule_hint),
            config_text=VALUES(config_text), updated_at=VALUES(updated_at)
    """
    field_sql = """
        INSERT INTO source_fields (
            source_key, source_field, target_field, data_type, purpose,
            required_flag, enabled, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s)
        ON DUPLICATE KEY UPDATE
            target_field=VALUES(target_field), data_type=VALUES(data_type),
            purpose=VALUES(purpose), required_flag=VALUES(required_flag),
            updated_at=VALUES(updated_at)
    """
    for source in SOURCE_DEFINITIONS:
        score = priority_score(
            source["purpose_score"],
            source["joinability_score"],
            source["authority_score"],
            source["freshness_score"],
            source["cost_score"],
        )
        cursor.execute(
            source_sql,
            (
                source["source_key"], source["source_name"], source["adapter_key"],
                source["endpoint_url"], source["target_domain"], source["entity_type"],
                source["purpose"],
                source["join_key"], source["purpose_score"], source["joinability_score"],
                source["authority_score"], source["freshness_score"], source["cost_score"],
                score,
                source["approval_status"]
                if score >= AUTO_ENABLE_THRESHOLD
                and source["schedule_hint"] in SUPPORTED_SCHEDULE_HINTS
                else "review",
                source["enabled"]
                if score >= AUTO_ENABLE_THRESHOLD
                and source["schedule_hint"] in SUPPORTED_SCHEDULE_HINTS
                else 0,
                source["required_credential"], source["schedule_hint"],
                json.dumps(source["config"], ensure_ascii=False, sort_keys=True), now, now,
            ),
        )
        expected_source_fields = [field[0] for field in source["fields"]]
        if expected_source_fields:
            placeholders = ", ".join("%s" for _ in expected_source_fields)
            cursor.execute(
                "UPDATE source_fields SET enabled=0, updated_at=%s "
                "WHERE source_key=%s AND enabled=1 "
                f"AND source_field NOT IN ({placeholders})",
                (now, source["source_key"], *expected_source_fields),
            )
        for field in source["fields"]:
            cursor.execute(field_sql, (source["source_key"], *field, now, now))


def ensure_schema_and_catalog() -> None:
    conn, cursor, engine = _open_connection()
    try:
        create_schema(cursor)
        _bootstrap_catalog(cursor)
        cursor.execute(
            "SELECT state_text FROM market_signal_state "
            "WHERE state_key='intraday_source_record_id_cursor'"
        )
        if cursor.fetchone() is None:
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM source_records")
            baseline_id = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                """
                INSERT IGNORE INTO market_signal_state (state_key, state_text, updated_at)
                VALUES ('intraday_source_record_id_cursor', %s, %s)
                """,
                (str(baseline_id), _now()),
            )
        conn.commit()
    finally:
        conn.close()
        engine.dispose()


def discover_approved_sources(schedule_hint: str = "daily") -> list[dict[str, Any]]:
    if schedule_hint not in SUPPORTED_SCHEDULE_HINTS:
        raise ValueError(f"unsupported source schedule: {schedule_hint}")
    conn, cursor, engine = _open_connection()
    try:
        cursor.execute(
            """
            SELECT id, source_key, source_name, adapter_key, endpoint_url,
                   target_domain, entity_type, purpose, join_key, priority_score,
                   required_credential, schedule_hint, config_text
            FROM source_registry
            WHERE enabled=1 AND approval_status='approved'
              AND priority_score >= %s
              AND schedule_hint=%s
            ORDER BY priority_score DESC, source_key
            """,
            (AUTO_ENABLE_THRESHOLD, schedule_hint),
        )
        columns = [column[0] for column in cursor.description]
        result = []
        for row in cursor.fetchall():
            item = dict(zip(columns, row))
            item["priority_score"] = float(item["priority_score"])
            item["config"] = json.loads(item.pop("config_text") or "{}")
            if (
                not _runtime_definition_matches(item)
                or (
                    item["required_credential"]
                    and not os.getenv(item["required_credential"])
                )
            ):
                raise ValueError(
                    f"source registry contract mismatch: {item['source_key']}"
                )
            cursor.execute(
                """
                SELECT source_field, target_field, data_type, required_flag
                FROM source_fields
                WHERE source_key=%s AND enabled=1
                ORDER BY id
                """,
                (item["source_key"],),
            )
            item["fields"] = [
                {
                    "source_field": row[0],
                    "target_field": row[1],
                    "data_type": row[2],
                    "required_flag": bool(row[3]),
                }
                for row in cursor.fetchall()
            ]
            required_source_fields = {
                field[0]
                for field in SOURCE_DEFINITIONS_BY_KEY[item["source_key"]]["fields"]
                if field[4]
            }
            active_source_fields = {field["source_field"] for field in item["fields"]}
            if not required_source_fields.issubset(active_source_fields):
                raise ValueError(
                    f"required source fields are disabled or missing: {item['source_key']}"
                )
            result.append(item)
        if len(result) > MAX_MAPPED_SOURCES:
            raise ValueError("approved source count exceeds mapped task limit")
        return result
    finally:
        conn.close()
        engine.dispose()


def discover_approved_source_keys(schedule_hint: str = "daily") -> list[str]:
    """Return only compact keys for Airflow XCom and dynamic mapping."""

    return [
        source["source_key"]
        for source in discover_approved_sources(schedule_hint=schedule_hint)
    ]


def load_approved_source(source_key: str) -> dict[str, Any]:
    """Reload and revalidate one source immediately before ingestion."""

    definition = SOURCE_DEFINITIONS_BY_KEY.get(source_key)
    if definition is None:
        raise ValueError(f"source is not currently approved: {source_key}")
    for source in discover_approved_sources(schedule_hint=definition["schedule_hint"]):
        if source["source_key"] == source_key:
            return source
    raise ValueError(f"source is not currently approved: {source_key}")


def _load_stock_market_codes(
    *, common_equity_only: bool = False,
) -> set[tuple[str, str]]:
    """Load current security codes, optionally restricting breadth to CFI ES* shares."""

    connection = pd.read_csv(sql_configure_path, index_col="name")
    conn, cursor, engine = get_sql(
        connection.loc["host", "value"],
        int(connection.loc["port", "value"]),
        connection.loc["user", "value"],
        connection.loc["password", "value"],
        "stock",
    )
    try:
        equity_filter = " AND CFICode LIKE 'ES%'" if common_equity_only else ""
        cursor.execute(
            "SELECT DISTINCT 證券代號, 市場別 FROM 全證券代碼相關資料表 "
            "WHERE 證券代號 IS NOT NULL AND 市場別 IS NOT NULL"
            + equity_filter
        )
        return {
            (str(code).strip(), str(market).strip())
            for code, market in cursor.fetchall()
            if str(code).strip() and str(market).strip()
        }
    finally:
        conn.close()
        engine.dispose()


def _write_run(
    cursor,
    *,
    source_key: str,
    started_at: dt.datetime,
    finished_at: dt.datetime | None,
    status: str,
    http_status: int | None,
    row_count: int,
    accepted_count: int,
    rejected_count: int,
    duplicate_count: int,
    dag_run_id: str | None = None,
    dag_id: str | None = None,
    task_id: str | None = None,
    map_index: int | None = None,
    try_number: int | None = None,
    error_message: str | None = None,
) -> None:
    cursor.execute(
        """
        INSERT INTO source_runs (
            source_key, started_at, finished_at, status, http_status,
            row_count, accepted_count, rejected_count, duplicate_count,
            dag_run_id, dag_id, task_id, map_index, try_number, error_message, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            source_key, started_at, finished_at, status, http_status,
            row_count, accepted_count, rejected_count, duplicate_count, dag_run_id,
            dag_id, task_id, map_index, try_number, error_message, _now(),
        ),
    )


def _raw_value(raw_record: dict[str, Any], source_field: str) -> Any:
    if source_field in raw_record:
        return raw_record[source_field]
    wanted = source_field.strip()
    for key, value in raw_record.items():
        if str(key).strip() == wanted:
            return value
    return None


def _source_field_value(
    source: dict[str, Any], raw_record: dict[str, Any], source_field: str,
) -> Any:
    """Resolve a canonical source field against its allowlisted raw aliases."""

    value = _raw_value(raw_record, source_field)
    if value is not None:
        return value
    aliases = (source.get("config") or {}).get("source_field_aliases", {})
    for alias in aliases.get(source_field, ()):
        value = _raw_value(raw_record, alias)
        if value is not None:
            return value
    return None


def _required_fields_missing(source: dict[str, Any], record: dict[str, Any]) -> list[str]:
    raw_record = record.get("_raw_record") or {}
    return [
        field["source_field"]
        for field in source.get("fields", [])
        if field.get("required_flag")
        and _source_field_value(source, raw_record, field["source_field"]) is None
    ]


def _typed_field_value(value: Any, data_type: str) -> tuple[str | None, float | None, dt.date | None]:
    if value is None or str(value).strip() == "":
        return None, None, None
    text_value = _bounded_text(value)
    if data_type == "date":
        parsed = parse_roc_date(value)
        return (text_value, None, parsed) if parsed else (text_value, None, None)
    if data_type == "month":
        parsed = parse_roc_month(value)
        return (text_value, None, parsed) if parsed else (text_value, None, None)
    if data_type in {"decimal", "integer"}:
        return text_value, parse_number(value), None
    return text_value, None, None


def _field_hash(record_hash: str, source_field: str) -> str:
    return hashlib.sha256(f"{record_hash}|{source_field}".encode("utf-8")).hexdigest()


def _version_value(column: str, value: Any) -> str | None:
    if value is None:
        return None
    if column == "metric_value":
        try:
            return format(
                Decimal(str(value)).quantize(
                    Decimal("0.00000001"), rounding=ROUND_HALF_UP
                ),
                "f",
            )
        except (InvalidOperation, ValueError):
            return str(value)
    if column == "event_time":
        if isinstance(value, dt.datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, dt.date):
            return f"{value.isoformat()} 00:00:00"
    if column == "effective_date" and isinstance(value, (dt.datetime, dt.date)):
        if isinstance(value, dt.datetime):
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)


def _record_version_hash(record: dict[str, Any]) -> str:
    canonical = {
        column: _version_value(column, record.get(column))
        for column in SOURCE_RECORD_VERSION_COLUMNS
    }
    payload = json.dumps(
        canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _insert_source_record_versions(
    cursor,
    snapshots: list[tuple[dict[str, Any], str]],
    *,
    capture_basis: str,
    lineage: dict[str, Any],
) -> None:
    if not snapshots:
        return
    version_columns = SOURCE_RECORD_VERSION_COLUMNS + (
        "version_hash", "captured_at", "capture_basis", "dag_id",
        "dag_run_id", "task_id", "map_index", "try_number",
    )
    insert_sql = (
        "INSERT INTO source_record_versions ("
        + ", ".join(version_columns)
        + ") VALUES ("
        + ", ".join(["%s"] * len(version_columns))
        + ")"
    )
    captured_at = dt.datetime.now()
    params = [
        tuple(snapshot[column] for column in SOURCE_RECORD_VERSION_COLUMNS)
        + (
            version_hash, captured_at, capture_basis,
            lineage.get("dag_id"), lineage.get("dag_run_id"),
            lineage.get("task_id"), lineage.get("map_index"),
            lineage.get("try_number"),
        )
        for snapshot, version_hash in snapshots
    ]
    cursor.executemany(insert_sql, params)


def _airflow_lineage() -> dict[str, Any]:
    try:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        task_instance = context.get("ti")
        if task_instance is None:
            return {}
        return {
            "dag_id": str(getattr(task_instance, "dag_id", "")) or None,
            "dag_run_id": str(getattr(task_instance, "run_id", "")) or None,
            "task_id": str(getattr(task_instance, "task_id", "")) or None,
            "map_index": getattr(task_instance, "map_index", None),
            "try_number": getattr(task_instance, "try_number", None),
        }
    except Exception:
        return {}


def ingest_source(source: dict[str, Any] | str) -> dict[str, Any]:
    if isinstance(source, dict):
        source_key = str(source.get("source_key") or "").strip()
        if not source_key:
            raise ValueError("source key is required")
        source = load_approved_source(source_key)
    else:
        source = load_approved_source(source)
    started_at = _now()
    conn, cursor, engine = _open_connection()
    try:
        if not _runtime_definition_matches(source):
            raise ValueError("source contract is not allowlisted")
        if source.get("required_credential") and not os.getenv(source["required_credential"]):
            raise RuntimeError(
                f"required credential is not available: {source['required_credential']}"
            )
        allowed_stock_market_codes = (
            _load_stock_market_codes(
                common_equity_only=bool(source["config"].get("common_equity_only"))
            )
            if (
                source["config"].get("filter_to_stock_master")
                or source["config"].get("classify_common_equity_codes")
            )
            else None
        )
        records, http_status = fetch_and_normalize(
            source, allowed_stock_market_codes=allowed_stock_market_codes
        )
        unique_records = list({record["record_hash"]: record for record in records}.values())
        duplicate_count = len(records) - len(unique_records)
        for record in unique_records:
            missing_fields = _required_fields_missing(source, record)
            if missing_fields:
                record["quality_status"] = "quarantine"
        accepted = [record for record in unique_records if record["quality_status"] == "ok"]
        rejected_count = len(unique_records) - len(accepted)
        now = _now()
        lineage = _airflow_lineage()
        existing_records: dict[str, dict[str, Any]] = {}
        record_hashes = [record["record_hash"] for record in unique_records]
        selected_columns = ", ".join(SOURCE_RECORD_VERSION_COLUMNS)
        for offset in range(0, len(record_hashes), 5000):
            batch_hashes = record_hashes[offset:offset + 5000]
            if not batch_hashes:
                continue
            placeholders = ", ".join(["%s"] * len(batch_hashes))
            cursor.execute(
                f"SELECT {selected_columns} FROM source_records "
                f"WHERE record_hash IN ({placeholders})",
                tuple(batch_hashes),
            )
            for row in cursor.fetchall():
                existing_records[str(row[3])] = dict(zip(SOURCE_RECORD_VERSION_COLUMNS, row))

        latest_version_hashes: dict[str, str | None] = {}
        for offset in range(0, len(record_hashes), 5000):
            batch_hashes = record_hashes[offset:offset + 5000]
            if not batch_hashes:
                continue
            placeholders = ", ".join(["%s"] * len(batch_hashes))
            cursor.execute(
                "SELECT record_hash, version_hash FROM source_record_versions "
                f"WHERE record_hash IN ({placeholders}) "
                "ORDER BY captured_at DESC, id DESC",
                tuple(batch_hashes),
            )
            for record_hash, version_hash in cursor.fetchall():
                latest_version_hashes.setdefault(str(record_hash), version_hash)

        pre_ingest_versions: list[tuple[dict[str, Any], str]] = []
        source_feed_versions: list[tuple[dict[str, Any], str]] = []
        for record in unique_records:
            snapshot = {
                column: (source["id"] if column == "source_id" else record.get(column))
                for column in SOURCE_RECORD_VERSION_COLUMNS
            }
            version_hash = _record_version_hash(snapshot)
            previous = existing_records.get(record["record_hash"])
            previous_hash = (
                _record_version_hash(previous) if previous is not None else None
            )
            latest_hash = latest_version_hashes.get(record["record_hash"])
            if (
                previous is not None
                and previous_hash != version_hash
                and latest_hash != previous_hash
            ):
                pre_ingest_versions.append((previous, previous_hash))
            if latest_hash != version_hash:
                source_feed_versions.append((snapshot, version_hash))

        _insert_source_record_versions(
            cursor,
            pre_ingest_versions,
            capture_basis="pre_ingest_current",
            lineage=lineage,
        )

        # updated_at is the last content-change time used by point-in-time
        # signal joins. Re-polling an identical source row must not make it
        # appear newly available; observed_at below still records the latest
        # successful observation.
        upsert_sql = """
            INSERT INTO source_records (
                source_key, source_id, record_key, record_hash, target_domain,
                entity_type, entity_id, event_type, event_time, effective_date,
                title, metric_name, metric_value, value_text, unit, currency,
                source_url, payload, quality_status, observed_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                updated_at=IF(
                    NOT (source_id <=> VALUES(source_id))
                    OR NOT (BINARY target_domain <=> BINARY VALUES(target_domain))
                    OR NOT (BINARY entity_type <=> BINARY VALUES(entity_type))
                    OR NOT (BINARY entity_id <=> BINARY VALUES(entity_id))
                    OR NOT (BINARY event_type <=> BINARY VALUES(event_type))
                    OR NOT (event_time <=> VALUES(event_time))
                    OR NOT (effective_date <=> VALUES(effective_date))
                    OR NOT (BINARY title <=> BINARY VALUES(title))
                    OR NOT (BINARY metric_name <=> BINARY VALUES(metric_name))
                    OR NOT (metric_value <=> VALUES(metric_value))
                    OR NOT (BINARY value_text <=> BINARY VALUES(value_text))
                    OR NOT (BINARY unit <=> BINARY VALUES(unit))
                    OR NOT (BINARY currency <=> BINARY VALUES(currency))
                    OR NOT (BINARY source_url <=> BINARY VALUES(source_url))
                    OR NOT (BINARY payload <=> BINARY VALUES(payload))
                    OR NOT (BINARY quality_status <=> BINARY VALUES(quality_status)),
                    VALUES(updated_at), updated_at
                ),
                source_id=VALUES(source_id), target_domain=VALUES(target_domain),
                entity_type=VALUES(entity_type), entity_id=VALUES(entity_id),
                event_type=VALUES(event_type), event_time=VALUES(event_time),
                effective_date=VALUES(effective_date), title=VALUES(title),
                metric_name=VALUES(metric_name), metric_value=VALUES(metric_value),
                value_text=VALUES(value_text), unit=VALUES(unit), currency=VALUES(currency),
                source_url=VALUES(source_url), payload=VALUES(payload),
                quality_status=VALUES(quality_status), observed_at=VALUES(observed_at),
                updated_at=VALUES(updated_at)
        """
        params = [
            (
                record["source_key"], source["id"], record["record_key"], record["record_hash"],
                record["target_domain"], record["entity_type"], record["entity_id"],
                record["event_type"], record["event_time"], record["effective_date"],
                record["title"], record["metric_name"], record["metric_value"],
                record["value_text"], record["unit"], record["currency"],
                record["source_url"], record["payload"], record["quality_status"],
                now, now, now,
            )
            for record in unique_records
        ]
        if params:
            cursor.executemany(upsert_sql, params)

        field_upsert_sql = """
            INSERT INTO source_record_fields (
                field_hash, record_hash, source_key, source_field, target_field,
                data_type, value_text, value_numeric, value_date, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                updated_at=IF(
                    NOT (BINARY target_field <=> BINARY VALUES(target_field))
                    OR NOT (BINARY data_type <=> BINARY VALUES(data_type))
                    OR NOT (BINARY value_text <=> BINARY VALUES(value_text))
                    OR NOT (value_numeric <=> VALUES(value_numeric))
                    OR NOT (value_date <=> VALUES(value_date)),
                    VALUES(updated_at), updated_at
                ),
                target_field=VALUES(target_field), data_type=VALUES(data_type),
                value_text=VALUES(value_text), value_numeric=VALUES(value_numeric),
                value_date=VALUES(value_date)
        """
        field_params = []
        for record in unique_records:
            raw_record = record.get("_raw_record") or {}
            for field in source.get("fields", []):
                raw_value = _source_field_value(
                    source, raw_record, field["source_field"]
                )
                if raw_value is None or str(raw_value).strip() == "":
                    continue
                value_text, value_numeric, value_date = _typed_field_value(
                    raw_value, field["data_type"]
                )
                field_params.append(
                    (
                        _field_hash(record["record_hash"], field["source_field"]),
                        record["record_hash"], record["source_key"], field["source_field"],
                        field["target_field"], field["data_type"], value_text,
                        value_numeric, value_date, now, now,
                    )
                )
        if field_params:
            cursor.executemany(field_upsert_sql, field_params)
        _insert_source_record_versions(
            cursor,
            source_feed_versions,
            capture_basis="source_feed",
            lineage=lineage,
        )
        _write_run(
            cursor,
            source_key=source["source_key"],
            started_at=started_at,
            finished_at=_now(),
            status="success",
            http_status=http_status,
            row_count=len(records),
            accepted_count=len(accepted),
            rejected_count=rejected_count,
            duplicate_count=duplicate_count,
            **lineage,
            error_message=None,
        )
        cursor.execute(
            "UPDATE source_registry SET last_success_at=%s, last_error=NULL, updated_at=%s WHERE source_key=%s",
            (_now(), _now(), source["source_key"]),
        )
        conn.commit()
        return {
            "source_key": source["source_key"],
            "status": "success",
            "row_count": len(records),
            "accepted_count": len(accepted),
            "rejected_count": rejected_count,
        }
    except Exception as exc:
        try:
            conn.rollback()
            error_response = getattr(exc, "response", None)
            failure_http_status = getattr(error_response, "status_code", None)
            if failure_http_status is None:
                failure_http_status = getattr(exc, "http_status", None)
            error_message = f"{type(exc).__name__}: {exc}"
            response_bytes = getattr(exc, "response_bytes_received", None)
            if response_bytes is not None:
                error_message += (
                    " [source_response"
                    f" source_key={getattr(exc, 'source_key', source['source_key'])}"
                    f" content_type={getattr(exc, 'response_content_type', 'unknown')}"
                    f" content_length={getattr(exc, 'response_content_length', 'unknown')}"
                    f" bytes_received={response_bytes}]"
                )
            error_message = error_message[:4000]
            _write_run(
                cursor,
                source_key=source["source_key"],
                started_at=started_at,
                finished_at=_now(),
                status="failed",
                http_status=failure_http_status,
                row_count=0,
                accepted_count=0,
                rejected_count=0,
                duplicate_count=0,
                **_airflow_lineage(),
                error_message=error_message,
            )
            cursor.execute(
                "UPDATE source_registry SET last_error=%s, updated_at=%s WHERE source_key=%s",
                (error_message, _now(), source["source_key"]),
            )
            conn.commit()
        finally:
            raise
    finally:
        conn.close()
        engine.dispose()
