# Purpose-driven source extension

The source-extension DAGs add approved, high-value sources to Airflow through
dynamic task mapping. They do not crawl every discovered URL or alter the
existing news and stock raw tables.

## Collection schedules

| DAG | Schedule (Asia/Taipei) | Sources |
|---|---|---|
| `source_extension_collect_process` | Daily at 04:00 | Dividends, Central Bank FX, quarterly profitability, and quarterly balance sheets; triggers `portfolio_data_notify` after collection |
| `source_extension_collect_intraday` | Weekdays every 15 minutes, 07:00–22:45 | Company events, monthly revenue, TWSE material company-forecast variances, and insider transfer reports; triggers the holdings notifier and the intraday market-signal scan |
| `source_extension_collect_weekend` | Saturday and Sunday at 08:00 | Rechecks company events, monthly revenue, and insider transfer reports once each morning; triggers the intraday market-signal scan |
| `source_extension_collect_after_close` | Weekdays at 20:00 | TWSE/TPEx official daily quotes and indexes; TPEx stock-level institutional flow; TDCC weekly shareholder-distribution snapshot; TWSE/TPEx over-10% shareholder lists; margin balances; TAIFEX index-futures and index-options positions; available short-sale shares; TPEx securities-borrowing activity; attention and disposition notices; TWSE forecast-attainment records; triggers the daily market-signal scan |
| `source_extension_collect_monthly` | Day 25 at 04:00 | TWSE and TPEx director holdings snapshots |

Each DAG discovers only enabled, approved sources in its registry
`schedule_hint` group. The weekend schedule reuses the `intraday` group; source
cadence groups do not have to match the calendar schedule. This keeps monthly
and after-close endpoints out of the existing 04:00 daily collection.

## TDCC weekly shareholder-distribution snapshot

The official TDCC OpenAPI route `/v1/opendata/1-5` returns the full
`集保戶股權分散表` as one unpaginated JSON response. The after-close collector
polls it on trading weekdays because source cadence groups are schedule hints;
the underlying effective date is weekly and must pass a 10-calendar-day
freshness gate. Each observed common-equity code must have the currently
observed holding tiers 1–17 exactly once. Missing tiers are quarantined, and
listed/OTC coverage must be complete before the source is treated as complete.

The first read-only snapshot check returned HTTP 200, 69,326 rows, and an
effective date of 2026-10-02. It matched 1,054/1,054 listed and 890/892 OTC
ordinary-share codes in the current local master. The unmatched OTC rows remain
coverage gaps; the collector does not fill them with zero. Non-common or
unmatched securities stay stored as `security` records and are excluded from
the stock signal join. This source is context-only until tier definitions,
weekly PIT history, and matured outcomes support a tested feature.

## Daily major-shareholder disclosure snapshots

The after-close collector also registers the official TWSE and TPEx daily
over-10% shareholder lists (`twse_major_shareholders_daily` and
`tpex_major_shareholders_daily`). The open-data records contain a report date,
company code/name, and holder name, but no share count or exact ownership
ratio. They are sparse disclosure snapshots, so row counts are not market
coverage and an absent holder is unknown rather than zero or evidence of an
exit. The collector retains dated PIT records and applies a five-calendar-day
effective-date freshness gate. Treat list changes as context only until source
revision behavior and matured outcomes have been checked; these records do not
add directional scores or notifications.

## Intraday quote access boundary

The intraday schedule currently gathers company events, monthly-revenue
releases, forecast-variance notices, and insider-transfer disclosures. The
2026-10-05 09:15 Taipei natural batch succeeded for all 10 mapped tasks, but
none was a live individual-stock quote feed. Its 09:30 signal child had zero
price/flow/fundamental rows, while `stock.個股成交資訊` still ended at
2026-10-02. Those zero counts mean no current quote input was available; they
do not mean that stocks had zero turnover or that the market had no signal.

TWSE describes real-time and 20-minute-delayed stock/index data as separate
information products governed by usage contracts and fees
([TWSE information services](https://wwwc.twse.com.tw/zh/products/information/information.html),
[usage terms](https://wwwc.twse.com.tw/zh/products/information/use.html)). TPEx
also offers real-time and delayed trading information through contracted
products ([TPEx information purchase](https://www.tpex.org.tw/web/service/info_service/info_service.php?l=zh-tw)).
The official pages describe browsing access separately from contractual data
use; there is no project-specific evidence authorizing automated bulk retrieval
from those web pages. No live individual-stock feed is integrated until an
authorized batch contract/provider is available; keep open-data daily-close
quotes as the price basis and treat intraday runs as event/disclosure
processing while quote coverage is unavailable.

### Official batch quotes for daily direction and outcome measurement

The daily market scanner reads closing price and share volume from the
point-in-time snapshots of `twse_equity_quotes_daily` and
`tpex_equity_quotes_daily`. Its price and volume families no longer read
`stock.個股成交資訊`; the outcome ledger also measures stock returns from the
same exchange-published quote sources and uses the corresponding official
market-index series as benchmarks. The source keys are restricted to the
approved market-wide daily feeds, and rows are matched to the current listed or
OTC master without filling missing securities with zero.

The five-session momentum and 20-session volume features become available only
after enough dated quote snapshots have accumulated. A successful collection
task alone does not make either market complete: the scanner separately checks
effective date, valid close, and listed/OTC coverage. Until enough official
history is stored, the corresponding technical feature remains unavailable;
legacy per-security website queries are not a fallback.

### Read-only natural-run audit

Run `PYTHONPATH=/opt/airflow/dags python -m source_extension.audit_runs
--dag-id source_extension_collect_weekend` inside the Airflow container to
inspect the latest scheduled run. Pass `--run-id` to inspect a specific run or
`--schedule-hint` when auditing a DAG outside the built-in mapping. The JSON
report includes latest source attempts, raw/accepted/quarantined/duplicate
counts, accepted effective-date bounds, current-code versus database-registry
contract drift, and accepted entity-code matches by listed/OTC master. For a
successful stock feed explicitly configured with `common_equity_only`, it also
reports matched accepted codes divided by the current common-equity master for
the market that feed covers. Other markets are marked out of scope with null
counts/rates, not zero coverage. This is a current-master join-coverage
diagnostic, not a point-in-time universe denominator or proof that every
expected daily record or event was collected. Event and aggregate feeds do not
receive this percentage. Endpoint URLs are not emitted. The audit never turns
an absent or unmatched row into zero. Its status describes task/registry checks
only: even all tasks succeeding leaves source freshness and dataset completeness
unproven.

The `common_equity_only` registry flag declares the denominator for coverage
reporting; by itself it does not filter fetched rows. A source is filtered to
the common-equity master only when `filter_to_stock_master` is separately set.
This keeps non-common accepted rows visible as unmatched quality evidence while
reporting common-equity coverage for full-market quarterly profitability and
monthly revenue feeds.

Every source-extension collection DAG also runs this read-only audit inside its
existing `verify_collection_and_triggers` task. The task log retains the full
JSON report; its XCom keeps only the compact summary and sources needing review.
The task fails closed when the audit is unavailable, partial, or cannot
attribute the run, including missing runtime sources or registry contract drift.
Any nonzero quarantine or duplicate count is also carried as a row-quality flag
into `data_review_required` and the compact issue list, even when the mapped
ingestion task itself succeeded. These flags request source-data review; they do
not alter candidate scores or convert incomplete values into zero.
The after-close audit compares each official market-index effective date with
the latest accepted `twse_equity_quotes_daily` or
`tpex_equity_quotes_daily` effective date on or before the source-run date. It
uses the same approved quote sources as the daily scanner. A date mismatch
flags the index source for review and includes both dates in the issue summary;
it does not change candidate scores or assume either source is the correct
trading date. A missing or failed comparison lookup remains unavailable rather
than becoming an aligned or zero-valued observation. This checks effective-date
alignment only; it does not prove historical row availability or preserve
revised source vintages.
When all scheduled sources are present and all ingestions/triggers finish, its
XCom reports either `data_review_required` for row-quality flags or
`complete_completeness_unproven` when no such flags exist. Neither status
asserts full market coverage or predictive value.

The weekend DAG uses Airflow's trigger timetable so both Saturday and Sunday
08:00 Asia/Taipei ticks start at their scheduled time. A cron data-interval
timetable would make the Sunday interval wait until the following Saturday
tick, leaving a six-day gap between runs. Its first trigger is bounded to
2026-10-04 08:00 Asia/Taipei to avoid replaying an older weekend interval;
the natural 2026-10-04 08:00 tick fired. That first run had two stale TPEx
registry contracts time out, so one fired tick does not establish reliable
source collection or recurring cadence.

## First-wave sources

| Source | Purpose | Main fields |
|---|---|---|
| TWSE significant messages | Link listed-company events to stock/news data | `company_code`, `event_date`, `title`, `event_description` |
| TPEx significant messages | Provide the same event model for OTC companies | `company_code`, `event_date`, `title`, `event_description` |
| TWSE/TPEx monthly revenue | Compare listed and OTC revenue trends; retain issuer explanations for material base effects | `company_code`, `report_month`, `revenue_current`, `revenue_yoy_pct`, `revenue_ytd_pct`, `revenue_note` |
| TWSE/TPEx dividends | Support dividend-event and yield analysis | `company_code`, `dividend_year`, `cash_dividend_per_share`, `effective_date` |
| TWSE/TPEx daily valuation metrics | Add same-date exchange-reported P/E, dividend yield, and price-to-book context to daily candidate cards | `market`, `company_code`, `effective_date`, `pe_ratio`, `dividend_yield_pct`, `price_to_book` |
| Central Bank daily FX | Add official FX context to international and financial-stock analysis | `effective_date`, `currency_pair`, `rate_to_usd`, `unit` |
| TWSE TWT96U available short-sale shares | Daily stock-level available-to-borrow quantity for both markets | `market`, `company_code`, `available_short_shares`, `capture_date` |
| TPEx securities-borrowing activity | OTC stock-level borrowing balance, sale, return, and adjustment | `company_code`, `date`, `securities_borrowing_*` |
| TWSE TAIEX daily close | Listed-market benchmark for the trailing five-session stock return comparison | `trading_date`, `closing_index` |
| TPEx index daily close | OTC-market benchmark for the trailing five-session stock return comparison | `trading_date`, `closing_index` |
| TWSE/TPEx daily margin balances | Current financing/short balances and utilization as leverage-positioning context | `market`, `company_code`, `trading_date`, balances and utilization rates |
| TAIFEX index-futures institutional positions | Market-wide futures positioning context for TAIEX and TPEx index futures; not an individual-stock signal | `effective_date`, `contract`, `institution_type`, net trades and net open interest |
| TAIFEX index-options institutional positions | Market-wide TAIEX calls/puts positioning context by institution class; no individual-stock direction | `effective_date`, `contract`, `call_put`, `institution_type`, net trades and net open interest |
| TWSE director holdings | Listed-company monthly person-level insider holdings and pledges | `company_code`, `report_month`, `role`, `person`, holdings and pledge fields |
| TPEx director holdings | OTC-company monthly person-level insider holdings and pledges | `company_code`, `report_month`, `role`, `person`, holdings and pledge fields |
| TWSE/TPEx quarterly operating analysis | Compare listed and OTC quarterly profitability | `company_code`, `report_period_end`, quarterly revenue, gross/operating/pre-tax/net margins |
| TWSE quarterly balance sheets | Collect sector-specific listed-company financial position data | `company_code`, `report_period_end`, total assets/liabilities/equity, current assets/liabilities where published |
| TPEx quarterly balance sheets | Collect OTC financial position data for the live `bd` and `ci` feeds | `company_code`, `report_period_end`, total assets/liabilities/equity, current assets/liabilities where published |
| TWSE daily attention and disposition notices | Add official abnormal-trading risk and active execution restrictions to eligible listed-stock cards | `company_code`, announcement date, attention details, active period, reason, execution measures |
| TPEx daily attention and disposition notices | Add official abnormal-trading risk and active execution restrictions to eligible OTC-stock cards | `company_code`, announcement date, attention details, active period, reason, execution measures |
| TWSE quarterly company-forecast attainment | Preserve the official raw rows for field-alignment research before measuring issuer forecast track records | `company_code`, report quarter, forecast sequence, coverage period, source-reported actual and forecast |
| TWSE quarterly company-forecast variance notices | Alert on TWSE's published material company-forecast variance list | `company_code`, report quarter, forecast sequence, coverage period, quarterly and cumulative actual/forecast values |

The official endpoints are documented by [TWSE OpenAPI](https://openapi.twse.com.tw/),
[TPEx OpenAPI](https://www.tpex.org.tw/openapi/), and the [Central Bank API guide](https://cpx.cbc.gov.tw/Data/ExportToAPIInfo).

### TPEx revenue and company-event feeds

The official open-data catalog exposes [TPEx daily significant messages,
dataset 18418](https://data.gov.tw/dataset/18418) and [TPEx monthly revenue,
dataset 56510](https://data.gov.tw/dataset/56510), each under Government Open
Data License v1. Their linked MOPS CSV resources remain official catalog
resources, but the current code contracts for `tpex_company_events` and
`tpex_monthly_revenue` use the corresponding TPEx OpenAPI JSON routes because
the MOPS host has timed out in natural collection. These two source keys and
the large TPEx quote feed use the bounded 64 KiB byte-range reader. It requires
HTTP 206, a stable `ETag` or `Last-Modified` validator, identity encoding, a
matching `Content-Range`, and a complete JSON array under the 10 MiB limit. Each
range segment has at most three attempts; partial or changing representations
fail closed. Event publication time remains separate from revenue report month.

Read-only endpoint preflights on 2026-10-04 returned 42 event rows (34 distinct
company codes, all 34 matched to the current OTC master; latest event date
2026-10-01) and 892 revenue rows (all normalized as `ok`; 891/892 distinct
codes matched; latest report month 2026-08). An earlier official MOPS CSV
preflight showed event data dated 2026-10-03, later than the JSON feed's latest
event date. These are endpoint-shape checks, not ingestion receipts, and the
date difference remains unresolved; neither endpoint is assumed comprehensive
or fresher from a single read.

The natural 2026-10-04 08:00 weekend run still read the pre-change database
registry contracts at `mopsfin.twse.com.tw`. Each TPEx source timed out twice
after its 60-second request timeout and recorded 0 rows, 0 accepted, and 0
quarantined. The schema bootstrap for that run occurred before the code
contract change. At 09:06 Asia/Taipei the database registry still showed the
old `tpex_csv` URLs; the next natural daily schema bootstrap is expected on
2026-10-05 at 04:00, with the first subsequent intraday collection no earlier
than 07:00. Verify the updated registry, effective dates, accepted/quarantined
counts, duplicates, and OTC master coverage from those natural runs; do not
count this failed run as proof of the JSON path.

### Daily valuation context

The after-close batch collects `BWIBBU_ALL` from TWSE and
`tpex_mainboard_peratio_analysis` from TPEx under the official open-data
licenses ([TWSE dataset 11547](https://data.gov.tw/dataset/11547),
[TPEx dataset 11373](https://data.gov.tw/dataset/11373)). The records retain
the exchange's effective date and reported P/E, dividend yield, and price-to-
book values; TPEx's separately reported current-season dividend per share is
retained in the raw/typed source fields but is not used to infer a cross-market
yield. Empty and nonnumeric ratios remain missing.

The daily scanner attaches valuation context only when its effective date
equals the price snapshot date and the record was available before the scan.
Cards label these ratios as exchange-reported context, link to the official
endpoint, and exclude them from candidate selection, directional families,
score, and ranking. A point-in-time multiple is not itself a buy/sell signal;
relative valuation or valuation-change rules require a stored history and
outcome validation first. The database registry now shows both valuation
sources enabled under `daily_after_close` (updated 2026-10-04 08:00 Taipei),
but neither has a `source_runs` row yet. Their first natural collection and
same-date listed/OTC coverage check remain pending the next weekday after-close
batch; registry presence is not collection evidence.

TPEx's all-market daily-close quote response is about 4.7 MB and has previously
ended before its declared `Content-Length`. It now shares the bounded 64 KiB
segmented reader used by the TPEx JSON feeds. The adapter accepts a segment only
when HTTP 206, `Content-Range`, `Content-Length`, total representation size and
the stable `ETag` or `Last-Modified` validator agree; each segment gets at most
three attempts. This avoids relying on one multi-megabyte stream or an open-ended
resume that the endpoint did not complete reliably. The reader fails closed on
an ignored or mismatched range, changed representation, incomplete segment,
unsupported encoding, invalid JSON or size-limit breach. The task log records
request count, bytes received, expected total, row count and final HTTP status.
Natural runs still need effective-date, accepted/quarantined-row, and
listed/OTC coverage checks.
The `market_quote` adapter requires a numeric exchange-reported `Change` value
for an `ok` row. Non-numeric markers such as `---` or `除息` remain quarantined
even when other quote fields are present; retain the raw marker and never map
it to a zero change or zero trading activity.

The quarterly financial feeds are polled daily so new filings and revisions are
picked up after release. The balance-sheet endpoints are separated by market
and sector (`basi`, `bd`, `ci`, `fh`, `ins`, `mim` for TWSE; `bd`, `ci` for
TPEx). A live check found blank placeholder rows from the other four TPEx
sector endpoints, so those are not scheduled. These are source inputs for
profitability and asset-growth analysis; they do not by themselves calculate
finished investment factors or create portfolio notifications.

The after-close scan also stores the official TAIEX and TPEx mainboard index
series. It compares a stock's trailing five-session close-to-close return with
the matching market index over the same window and displays the simple return
difference in percentage points. This is market-relative context, not a
risk-adjusted alpha estimate. Official daily margin-balance records are kept as
positioning context: current utilization and, after six daily snapshots exist,
the five-session change in financing/short balances. They never add a bullish
or bearish family vote. The TPEx open-data catalog marks its daily index and
margin-balance feeds free and Government Open Data License v1
([index history, dataset 11391](https://data.gov.tw/dataset/11391);
[margin balances, dataset 11387](https://data.gov.tw/dataset/11387)). The TWSE
counterparts are documented in the [TWSE OpenAPI catalog](https://openapi.twse.com.tw/).
The after-close collector runs on weekdays; it does not backfill missed days
automatically, so the benchmark comparison and five-session margin context
appear only after enough dated records have accumulated.

### TPEx daily day-trade market aggregate

The approved `tpex_daytrade_market_summary_daily` source uses the official
TPEx OpenAPI for government dataset [11627](https://data.gov.tw/dataset/11627).
The dataset is free, updated daily, and licensed under Government Open Data
License v1. Its rows are market aggregates only: day-trade shares and their
market share, plus buy/sell amounts and market shares. There is no security
code, so this source cannot supply or validate individual-stock day-trade
coverage.

The daily digest reads this context only when the latest accepted effective
date matches the price snapshot date and the record was available before that
scan started. A missing, quarantined, or older record is reported without
carrying values forward or converting absence to zero. Same-day values are
preliminary: TPEx says T-day records can be adjusted through T+2. The displayed
five-session change therefore ends at T-2 and is shown only when eight complete
official TPEx index sessions and corresponding aggregate records are available.
TPEx historical rows may be revised in place, and this source does not retain
old vintages; it remains non-directional market context, excluded from stock
family votes, scores, and candidate thresholds.
Rows with an invalid date remain quarantined for audit, and multiple distinct
records for one effective date are all quarantined rather than choosing one.

### TWSE daily day-trade target eligibility list

The `twse_daytrade_eligibility_daily` source captures the official TWSE
[`TWTB4U` OpenAPI](https://openapi.twse.com.tw/) listed-stock target list,
catalogued as government dataset [11661](https://data.gov.tw/dataset/11661).
The dataset is free, daily, and under Government Open Data License v1. Its
published fields identify the data date, security code/name, and the note for
temporarily suspending sell-then-cash-buy day trades. The adapter stores one
affirmative membership row per returned security/date and preserves the
suspension note. It does not represent day-trade shares, buy/sell amounts, or
price direction.

The source is collected in the existing 04:00 daily batch so its effective
date can be checked against that day's signal snapshot and the collection time
against scan start. The endpoint returns its latest snapshot and does not
provide historical vintages through the checked contract; query-date
parameters did not change the returned date. A read-only 2026-10-04 preflight
returned HTTP 200, 1,228 rows / 1,228 unique codes, all dated ROC 1151005
(2026-10-05). Of those, 972 codes matched the current 1,054-code listed
common-stock master; the remaining feed codes and master omissions are not
filled as zero or treated as negative eligibility. The next natural batch
must validate its own response and market-master join before the data is used
in a candidate card. Until then this feed is collected as execution context,
not a directional family or score input.

The official open-data catalog also lists TPEx dataset
[137680](https://data.gov.tw/dataset/137680) for daily OTC day-trade target
eligibility with the same code/name/suspension-note fields and Government Open
Data License v1. Its catalogued resource is the TPEx CSV
[`intraday_trading_list_result.php`](https://www.tpex.org.tw/web/stock/trading/intraday_trading/intraday_trading_list_result.php?l=zh-tw&o=data).
A read-only preflight through the Airflow worker's existing allowlisted batch
fetcher returned HTTP 200 with `application/csv`: 841 rows / 841 unique codes,
all dated 2026-10-02, with blank or `＊` suspension markers. Of 892 current OTC
common-stock master entries, 724 feed codes matched; 117 feed codes did not
match that common-stock master and 168 master codes were absent from this
snapshot. These are join diagnostics, not inferred exclusions: an unmatched
or absent code is unknown, never zero volume or negative eligibility. The
browser 403 and Windows transport error seen earlier were environment-specific
observations; the worker can retrieve the official batch resource.

The `tpex_daytrade_eligibility_daily` source now uses this catalogued CSV in
the existing 04:00 daily batch alongside the TWSE list. The adapter preserves
the official effective date and suspension note, accepts the feed's
`application/csv` MIME for this source only, and fails closed on empty or
conflicting responses. Both market lists are execution context only: they do
not represent day-trade volume, buy/sell amounts, or price direction, and
neither provides checked historical vintages. Use a record only when its
effective date matches the signal snapshot and its collection completed before
the scan started; do not carry forward older dates or convert missing rows to
zero. The preflight did not write the database or create a `source_runs` row.
Wait for the next natural bootstrap/collection to validate catalog
registration, raw/accepted/quarantined/duplicate counts, effective date,
listed/OTC master joins, and source receipt before treating the batch as
operationally collected. Do not replace this source with individual-security
pages or paid-market-data routes.

### TWSE per-security day-trade statistics: license and batch gap

The public TWSE OpenAPI contract at
[`/v1/exchangeReport/TWTB4U`](https://openapi.twse.com.tw/v1/swagger.json)
contains `Date`, `Code`, `Name`, and `Suspension`: it is an eligibility and
restriction snapshot, not per-security day-trade volume or buy/sell amounts.
Do not infer numeric activity from this feed or from a missing membership row.

TWSE's [Data E-Shop product TWTB5U](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)
is a separate daily product. Its
catalogue lists per-security day-trade shares and buy/sell amounts, a roughly
20:00 production time, daily cadence, data from 2020-06-01, and XLS delivery.
The listed price is NT$1,500/month for internal use or NT$3,000/month when
transmitting the purchased content to information users. TWSE states that
broker submissions can be revised through T+2 and that T+2 is the final
amount. The catalogue offers online download and email delivery but does not
document a machine API, an automated delivery schedule, or a service-level
contract. Confirm those terms and the permitted use in a private notification
channel before any subscription or integration.

The online order terms further restrict redistribution: internal-use products
are limited to the subscriber's internal users (for an organization, its
directors, supervisors, managers, or employees); external-use products may be
transmitted only to the subscriber's information users with required source
and order attribution and no onward redistribution. The exchange also states
that it does not warrant product accuracy or completeness. The public terms do
not establish that a private Discord destination or unattended email ingestion
is permitted. Treat TWTB5U as a promising licensed trading-intensity feed, but
keep it out of the collector until entitlement, delivery automation, usage
rights, correction vintages, and completeness checks are contractually clear.
The linked example workbook was not parsed in this review, so exact cell types,
row-count/completeness markers, and the denominator contract remain unverified.

The legacy collector's `exchangeReport/TWTB4U?...selectType=All` website
request is not the OpenAPI schema above and is not an approved replacement for
the separately licensed TWTB5U product. Keep `stock.當沖資訊` out of
`source_extension` until a licensed, schedulable batch delivery is confirmed.
Do not connect the TPEx individual-security website query either; retain its
official OAS market aggregate only as freshness-gated, non-directional
context. If TWTB5U later becomes authorized and machine-deliverable, retain
T, T+1, and T+2 vintages; distinguish preliminary from final values; measure
per-security day-trade share of total turnover only after confirming the
numerator/denominator definitions; evaluate the result as trading-intensity
context against mature, cost-aware outcomes before assigning any direction.

TAIFEX's official OpenAPI `MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate`
is collected after close and filtered to `臺股期貨` and `櫃買指數期貨` across
the three published institutional categories. A complete date therefore has
six contract/category records. The collector retains source-reported net
trading and net open-interest fields. Five-session changes are shown only when
six synchronized, complete dates are available; absent categories remain
missing and are never filled as zero. The categories aggregate many
institutions and do not identify a specific institution's strategy. This feed
is labeled market context and does not add a directional score or vote until
out-of-sample outcome evidence supports that use. TAIFEX's API is an official
source; this integration does not claim a separate redistribution license.

The official TAIFEX `MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate`
endpoint is collected in the same after-close batch and filtered to
`臺指選擇權`, `CALL`/`PUT`, and the three published institutional categories.
A complete effective date therefore has six rows. The source preserves the
exchange's net open-interest values and only shows same-date rows; missing
categories are never filled as zero. The categories aggregate multiple
institutions, while options positions may be parts of multi-leg strategies.
Neither CALL nor PUT net positions are read as a simple bullish/bearish vote;
the daily digest labels them market context and excludes them from stock-level
signal scoring. Five-session changes require six synchronized complete dates.
The endpoint was directly checked on 2026-10-03: 30 rows in the full response,
with six `臺指選擇權` rows dated 2026-10-02 and non-null net open interest for
all six. This is an endpoint-shape check, not a scheduled ingestion run or a
redistribution-license determination. See the [TAIFEX OpenAPI](https://openapi.taifex.com.tw/)
and [calls/puts institutional methodology](https://www.taifex.com.tw/cht/3/callsAndPutsDate).

## Automated quarterly balance-sheet context

The existing official TWSE and TPEx quarterly balance-sheet batch feeds now
also provide point-in-time context on eligible daily candidates. The scan reads
only `quarterly_balance_sheet` records whose official publication time and
database update time were available before the scan cutoff, whose report period
is no later than the signal date, and whose period end is within 180 days of
that date. On non-financial companies, the card may show the current ratio
(`current assets / current liabilities`) and liabilities-to-equity ratio when
the source fields and positive denominator are present. Missing or zero
denominators stay unavailable; they are not converted to zero. Generic ratios
are omitted for financial, banking, insurance, securities, and futures
industries.

These are sourced quarter-end stock figures for risk review. They do not
represent operating cash flow, are not an independent directional signal, and
do not change family votes, candidate thresholds, or ranking. The card links
back to the official source and states this limitation. As of the 2026-10-02
natural collection, the eight sector feeds had a latest reporting period of
2026-06-30 and 1,975 normalized rows accepted with no rejected rows on their
latest mapped attempts. The current-master join matched 1,053/1,054 listed and
891/892 OTC codes; unmatched accepted codes remain a coverage reconciliation
item, not zero-value data.

The public [TWSE OpenAPI catalog](https://openapi.twse.com.tw/v1/swagger.json)
and [TPEx OpenAPI catalog](https://www.tpex.org.tw/openapi/swagger.json) list
company-level income-statement and balance-sheet routes, but no route named for
company-level cash-flow statements was found in a 2026-10-04 keyword scan of
the 143 TWSE and 225 TPEx published paths/operation summaries/descriptions.
This is scoped only to those JSON catalogs, not proof that no other official
batch file exists. TPEx's [OTC general-industry financial dataset, 94865](https://data.gov.tw/dataset/94865)
is explicitly labeled monthly and Government Open Data License v1; its listed
fields cover income-statement items and basic EPS, not cash-flow statements.

### MOPS quarterly XBRL archive: batch-source candidate, not yet approved

A follow-up search found a materially different official route: the MOPS
[XBRL case-document batch page](https://mopsov.twse.com.tw/mops/web/t203sb02)
offers one downloadable archive per quarter, and the TWSE's 2024 user manual
says that a selected quarter downloads that quarter's filings for all
companies. The same manual describes platform coverage as listed, OTC,
emerging-market companies and financial-holding subsidiaries, with XBRL
financial data from 2009 Q4. A bounded read-only inspection downloaded the
official 2026 Q2 archive to a
temporary file (111,404,708 bytes), scanned its ZIP directory and parsed XBRL
fact names from all 2,703 HTML members, then removed the temporary file. All
2,703 contain `xbrli:context`; 2,620 filenames match the quarter-report suffix
pattern, representing 2,609 distinct four-digit codes. Among the current common
stock master (1,054 listed, 892 OTC), operating-cash-flow facts match 1,053
listed and 891 OTC codes; investing-cash-flow facts match 1,053 and 890;
financing-cash-flow facts match 1,051 and 888. A few files lack each major
cash-flow concept. The archive has more codes than the current common-stock
universe, so unmatched codes are not zero-valued observations. Multiple report
files per code and the canonical consolidated/individual selection rule still
need resolution.

The scan found no filing/submission timestamp tokens among the tested metadata
field names. The extracted XBRL date facts span 2024-04-01 to 2027-08-06,
which are reporting/context facts and are not evidence of when a filing became
available. This batch is therefore promising for scheduled full-market cash-
flow context, but it is not PIT-safe and must not enter signal cards or scores
until filing-time, amendment/version history, taxonomy/unit/period selection,
license and repeated automated-access permissions are verified. See the [TWSE
XBRL manual](https://investoredu.twse.com.tw/FileSystem/eBook/%E5%A6%82%E4%BD%95%E4%BD%BF%E7%94%A8%E5%84%AA%E5%8C%96%E5%85%AC%E9%96%8B%E8%B3%87%E8%A8%8A%E8%A7%80%E6%B8%AC%E7%AB%99.pdf).

TWSE separately advertises an XBRL push-delivery package for IFRS financial
reports at NT$40,000/month; that paid push service is distinct from the public
quarterly download page. See [package details](https://eshop.twse.com.tw/zh/mops/detail/000000007af53455017af57c6e5c000c)
and the [package list and prices](https://eshop.twse.com.tw/zh/mops/list). Do
not infer that the public batch archive carries the same fee, or that its
download path grants an unrestricted automated redistribution license.
Before adding it to `source_registry`, verify the permitted recurring access,
archive completeness for both markets, parsing/coverage, filing publication
time and restatement lineage. Until those gates pass, keep it as a source
candidate only; do not turn cash-flow statements into a vote. Existing
balance-sheet ratios remain separate from cash-flow inference.

### Official MOPS quarterly report corrections: full-market event candidate

The official MOPS [financial-report correction query](https://mopsov.twse.com.tw/mops/web/t56sb31_q1)
offers an all-market query by report year and quarter, with company code left
blank. A read-only 2026-10-05 preflight for ROC year 115, quarter 02 posted to
the MOPS query form and returned HTTP 200, 49 correction-event rows covering
47 distinct security codes, with announcement dates from 2026-08-10 through
2026-10-02 and no unparseable dates. The response fields include security
code/name, announcement date, report type, and correction description/content.
These are event-grain rows: multiple corrections for one company are expected
and must not be collapsed to one issuer record.

This could complement the MOPS [quarterly XBRL batch archive](https://mopsov.twse.com.tw/mops/web/t203sb02)
as a report-version and data-quality context feed. A correction can invalidate
previously collected quarter values, including cash-flow fields, but a notice
does not by itself establish materiality or a bearish direction. Keep the
source-reported correction text, distinguish document/format completion from
numerical or accounting-content changes, and retain the corrected filing as a
new version rather than overwrite the point-in-time record. Do not add a
directional vote until a stable classifier and mature outcomes support it.

The separate MOPS financial-report-announcement page `t163sb01` currently
requires a company code: a read-only all-market/blank-code query returned the
official response “請輸入公司代號”. It therefore does not supply an all-market
initial filing-time feed through the checked form. The XBRL batch archive is
available by quarter, but the public page and prior archive inspection do not
establish an item-level filing timestamp, immutable revision history, or
permission for recurring automated downloads and storage. First-observed
archive versions could only support a conservative PIT clock after recurring
access and version-retention terms are confirmed; they cannot be backdated to
the report period end.

This was a one-time official website POST preflight, not an OpenAPI contract,
Airflow source run, or data-ingestion receipt. No official open-data license or
automated reuse terms for this query were established in this qualification,
so do not add it to `source_registry` or poll it as a signal yet. Before
integration, confirm recurring machine access and storage/display/backtest
rights, stable fields and event keys, complete listed/OTC scope, publication
and first-observed timestamps, correction/version semantics, response limits,
and natural-run counts/coverage. Exact-name searches did not locate this
correction query in the Government Open Data catalog; that search result is not
itself proof that no separate permission exists.

## TEJ full-market vendor data: candidate only, not configured

The current [TEJ TQuant dataset catalogue](https://tquant.tejwin.com/%E8%B3%87%E6%96%99%E9%9B%86/) describes `TWN/APISHRACT` as daily Taiwan listed/OTC data including delisted securities, institutional trades, margin, securities lending, day-trading and weekly shareholding. It lists `vol_dt` (day-trade shares, thousand shares) and `vol_dtp` (day-trade share of total turnover), and states the dataset is from TWSE/TPEx. The page describes point-in-time datasets and says margin/borrow/day-trade data update at 21:30. These are provider catalogue claims, not a DataFlow collection receipt or a verified row-level PIT contract.

TEJ documents a REST API that retrieves datatables with an API key, while its E-Shop says some databases are free and others paid and a trial key must be requested. It is not yet confirmed that `TWN/APISHRACT` is entitled for this project, available through the documented REST interface under a specific plan, or licensed for automated storage/display. Do not call the endpoint, use a trial key, or add a source contract until access and use terms are explicitly verified. Its delisted-security history could improve survivorship-safe outcome research; daily day-trade share/turnover can be tested as a speculation/attention context, not assumed bullish or bearish. Reconcile the vendor schema against existing exchange feeds before storing duplicate fields.

Do not treat broker articles, abstracts, or issuer-by-issuer report reviews as automated signals. A broker-consensus feed is only a future batch-source candidate if licensed market-wide machine access, analyst identity, horizon, first-published/revised timestamps, revision history, coverage, and reuse rights are established. See [TEJ REST documentation](https://api.tej.com.tw/document_rest.html) and [E-Shop API access](https://eshop.tej.com.tw/E-Shop/Edata_api).

## CMoney analyst-estimate products: batch-feed candidate, not configured

CMoney's public table descriptions provide a concrete batch-feed candidate to qualify without reading reports issuer by issuer. The market-wide `M037` monthly institutional earnings/EPS table is described as using the Taiwan listed/OTC security list, refreshing at 09:50 and 19:54, and retaining each institution's latest valid estimate from the preceding six months. Its fields include estimated revenue, net income and EPS, month-over-month earnings-estimate change, high/low EPS, contributing institution count and report update date. The separate `M188` table is foreign-institution-only. These public tables are provider-described aggregates; they do not expose the underlying report rationale or prove that every estimate has a retrievable analyst-level timestamped revision record. See the [M037 field and schedule description](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M037), [M188 description](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M188), and [table catalogue](https://www.cmoney.com.tw/S2_content.asp).

CMoney also advertises a market-wide HTTPS/JSON data API with daily updates, and its Hunter Report product advertises daily estimate-change tracking and historical estimate paths. The public materials reviewed do not establish that M037/M188 or Hunter Report are available through a licensed bulk API for this project, nor the API table IDs, listed/OTC row coverage, historic vintage/revision retention, publication-time contract, request limits, or storage/display/backtest rights. Do not scrape the browser tables or wire an endpoint based on the general API marketing page. Treat access, schema, timestamp semantics, survivorship coverage, history depth and reuse rights as qualification gates; do not add a source definition before those are confirmed.

If the gates pass, first collect immutable daily point-in-time snapshots and verify API coverage against the listed/OTC master. Candidate features are changes in consensus EPS/revenue, estimate dispersion, and contributor-count changes, compared with official MOPS revenue/financial disclosures and subsequent market-adjusted outcomes. Preserve provider publication/update time separately from collection time. CMoney's `M866` broker-accuracy table is described as showing only the top ten brokers for each security on the latest quarter; that selected cross-section is not a historical analyst-quality series and must not be used as a backtest input without a point-in-time archive. No provider evidence here supports a live signal yet.

### Public API qualification update — 2026-10-04

CMoney now has a live public Owl API product/platform page. CMoney describes a Taiwan-market historical-data API with automatic updates, HTTPS/JSON, and multi-stock query modes; Owl's public terms limit use to personal or enterprise internal research and prohibit reproduction, resale, or distribution without written consent. The platform page's general history/API claims do not identify M037, M188, or Hunter Report as entitled API products. See [CMoney API product](https://www.cmoney.com.tw/product/api) and [Owl platform and terms](https://owl.cmoney.tw/home).

The current public M037 description still gives a useful candidate contract: daily updates scheduled for 09:50 and 19:54; a rolling six-month window that keeps each institution's latest estimate per security; consensus revenue/net-income/EPS, net-income estimate change, EPS high/low, institution count, and report update date. It is an aggregate of published forecasts, not the report text or a versioned analyst-by-analyst event stream. See [M037 description](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M037). The separate Hunter Report product advertises historical estimate paths and daily changes, but the product page does not establish machine API entitlement or API fields; see [Hunter Report](https://www.cmoney.com.tw/product/hunter-report).

Treat M037/Hunter as a high-value batch-source qualification candidate, not an integrated signal. Before adding a registry definition, confirm the subscribed API product and endpoint, full listed/OTC row universe, per-row as-of/publication timestamp, immutable historical vintages versus backfilled current values, rate limits/SLA, analyst identity/forecast horizon fields, and rights for local retention, backtesting, and Discord summaries. If only aggregate M037 is available, test consensus-revision breadth, estimate dispersion and contributor-count changes as a separate PIT feature; do not claim individual report quality, rationale, or analyst skill. Preserve the six-month rolling window semantics and do not treat M188 as an independent family from M037.

## Automated insider-transfer observation

The source registry adds four official batch endpoints: TWSE and TPEx
pre-transfer declarations (twse_insider_transfer_planned,
tpex_insider_transfer_planned) and their untransferred follow-up reports
(twse_insider_transfer_untransferred,
tpex_insider_transfer_untransferred). They are allowlisted in the adapter
and scheduled with the existing intraday collection group. The regulator's
open-data entries document the OTC planned-transfer report
([dataset 22814](https://data.gov.tw/dataset/22814)), listed untransferred
report ([dataset 22815](https://data.gov.tw/dataset/22815)), and OTC
untransferred report ([dataset 22816](https://data.gov.tw/dataset/22816)).
The listed planned-transfer dataset is
[dataset 22813](https://data.gov.tw/dataset/22813);
the [TWSE OpenAPI catalog](https://openapi.twse.com.tw/) lists the listed
pre-transfer route. Dataset pages identify Government Open Data License v1.

The records preserve source date, market, security code, role, method,
declared shares, reported holdings, and validity window when the feed provides
them. Names and transferees are omitted from normalized payloads. These APIs
provide a report date rather than a verified intraday publication timestamp,
so cards show date precision. A pre-transfer declaration is a plan, not a
completed transaction; untransferred rows are a separate later observation.
Neither is added to the directional score. Eligible records are surfaced as
neutral factual observations and can be compared with later market outcomes
before any directional interpretation is considered.

The live read-only contract probe on 2026-10-04 returned one valid, positive-
quantity planned row from each market, both dated 2026-10-02. Each
untransferred endpoint returned one date-only blank sentinel with no valid
security code or quantity. The normalizer quarantines these sentinel rows
instead of turning them into zero-share observations. These are event feeds:
an empty event list does not mean missing stock coverage or zero trading.
Natural scheduled runs still need to verify effective dates, raw/accepted/
quarantined counts, source freshness, and listed/OTC coverage.

## Non-duplication and licensing

Before adding these sources, the existing stock schema was checked for overlap.
Aggregate-only sources were excluded where the information is already
represented in collected individual-stock records. The available-short-sale
endpoint reports both markets in one feed; the adapter splits it by market and
keeps only codes in `stock.全證券代碼相關資料表`, excluding ETFs and other
non-stock instruments. The TPEx borrowing feed applies the same stock-master
filter.
The TPEx borrowing adapter keeps only `SecuritiesBorrowingBalancePreviousDay`,
`SecuritiesBorrowingSale`, `SecuritiesBorrowingReturn`,
`SecuritiesBorrowingAdjustment`, and
`SecuritiesBorrowingBalanceOfTheMarketDay`. It drops credit-short-sale
`Sale*` fields already stored in `stock.融資融券`, as well as the available
short-sale field collected from TWT96U. Company names are omitted because the
stock master already supplies them.

Director holdings are person-level public filings, not market or holder
percentage aggregates. Their role/person/month records add holdings-at-election,
current shares, pledged shares, and related-person holdings that are not
present in `stock.大戶比例`; company names are again omitted. The TWSE/TPEx
catalog entries identify these as Government Open Data License v1 datasets
([TWSE 22811](https://data.gov.tw/dataset/22811),
[TPEx 22812](https://data.gov.tw/dataset/22812)). The two stock-lending
datasets also identify Government Open Data License v1
([TWSE/TPEx 11872](https://data.gov.tw/dataset/11872),
[TPEx 11632](https://data.gov.tw/dataset/11632)). Each registry config stores
the provider, full dataset name, and license-version attribution template;
replace `{year}` with the data-release year when redistributing or presenting
the collected information. The [license terms](https://data.gov.tw/license)
require explicit attribution.

At signal time, the same person's current shares can appear in more than one
role row for a single reporting month. The daily feature therefore counts one
value per person/company/month. When those repeated rows disagree on current
shares, the company-period is omitted from the insider directional family
instead of choosing a value. The current runtime only contains a 2026-08
holdings period; the month-over-month insider family cannot be evaluated until
a second distinct period is collected naturally.

### Automated pledged-share risk context

The daily scan also derives a neutral risk context from the official TWSE and
TPEx monthly director-holdings batch feeds. For each company and reporting
month it deduplicates role rows by the filed person's name, then computes
`sum(direct pledged shares) / sum(direct current shares)` across unique filers.
Related-person pledge fields are excluded because they can be repeated in more
than one filer row. A conflicting or missing current/pledged amount, a
negative amount, pledged shares above current holdings, or a non-positive
denominator suppresses that company's month. The data must be point-in-time
available by the scan and both the report month and date-only publication date
must be within 90 days. The card links the official source and labels the
period, filer count, direct-filer basis, and that this is context only.

This ratio describes pledged shares within the reported direct holders' own
holdings. It is not the company's total share-pledge ratio, a margin-call
estimate, or a directional vote; it does not change candidate eligibility,
score, or thresholds. A read-only audit of the 2026-08 period found the
reported percentage field differed by more than 0.25 percentage points from
the count-derived person ratio in 29 TWSE and 15 TPEx accepted rows. The
context therefore recomputes from the accepted share counts and does not use
the reported percentage field. The audit found one effective month only, so
it cannot support a pledge-trend or predictive claim.

The latest natural holdings batch in the inspected runtime was 2026-09-24 UTC:
TWSE 27,475 raw / 26,804 accepted / 0 quarantined / 671 duplicates and TPEx
17,568 raw / 17,139 accepted / 0 quarantined / 429 duplicates; both reported
effective month 2026-08 and date-only publication date 2026-09-18. The accepted
entity-code crosswalk matched 1,054/1,054 current listed-market master codes
and 890/892 OTC master codes. Thirty additional TWSE codes and one TPEx code
did not match the current market master; they remain unmapped rather than
being treated as zero holdings. Within this month, five TWSE company-periods
with conflicting repeated-person current shares were suppressed, as were two
company-periods without a positive direct-holdings denominator; no TPEx
company-period was suppressed. A read-only no-send preview enriched the saved
10/02 50-candidate snapshot: 14 candidates had positive direct pledged-share
ratios, one appeared among the eight visible digest embeds, and the three
serialized request bodies were 7,859 / 9,182 / 9,424 bytes, all below the
10,000-byte limit. This was a historical formatter preview, not a new scan or
external delivery.

The separate TDCC `1-5` shareholding-distribution dataset is a full-market,
weekly batch keyed by effective date, security code, and holding band. Its
official API is `https://openapi.tdcc.com.tw/v1/opendata/1-5`; the raw fields
include holder count, shares, and share of custody inventory by band. The
legacy collector previously called a `getOD.ashx` URL that now redirects and
could finish with zero inserted rows while Airflow marked the task successful.
It now uses the official OpenAPI and fails closed for redirects, empty or
mixed-date payloads, malformed distribution values, missing total-holding
tier 17, stale snapshots, or less than 95% listed/OTC coverage in either
market. The collection log reports the effective date, market-date lag, raw
and transformed row counts, and each market's coverage. The collector still
uses the existing `stock.大戶比例` table; this does not add a second fetch.

The collector's quality log now separates accepted in-scope rows, quarantined
invalid target rows, duplicate natural keys/rows, and rows outside the current
stock-code master. Duplicate `(effective date, security code, holding tier)`
keys and invalid target values fail the batch closed; out-of-master rows are
counted as out of scope, not as zero holdings. These are run diagnostics rather
than persisted quarantine records, so each successful task still needs its
effective date and both market coverage figures checked.

The legacy Saturday `stock_collect_weekday_process` still includes this source.
After the TDCC endpoint repair landed after its 2026-10-03 run, a focused
`stock_major_holder_monday_refresh_process` definition adds a Monday 09:00
Asia/Taipei trigger schedule for this source alone. Its first scheduled time is
2026-10-05; it does not repeat the weekly margin, revenue, or earnings-call
tasks. The 2026-10-05 09:00 Asia/Taipei natural run succeeded: effective date
2026-10-02, market-date lag 0, 69,326 raw rows, 33,048 rows in the current
stock-code master, 1,944 summaries inserted, listed 1,054/1,054, and OTC
890/892. Two absent OTC codes remain unknown. This first run preceded the new
accepted/quarantine/duplicate counters, so those values were not reported and
must not be inferred as zero; the next natural collection should confirm them.
The live endpoint preflight is not a database collection receipt.

The candidate scan reads those weekly snapshots as point-in-time context only.
It requires the latest two effective dates within 14 days of each other, the
latest snapshot within 14 calendar days of the candidate's market date, at
least 95% valid coverage for both TWSE and TPEx in both snapshots,
non-conflicting rows for the specific company, and both rows loaded by scan
start. Missing
company rows are never treated as zero. A change above the legacy 2 percentage
point display threshold appears on an already-qualified candidate card as
TDCC ownership-distribution context; it does not add a directional family,
change the score, or independently create a candidate. The threshold has not
been calibrated as a predictive rule.

The metadata is not perfectly aligned across official pages: the government
catalog labels dataset 11452 as monthly, while TDCC's live query page says the
distribution is based on the last business day of each week. The implementation
therefore records the provider-stated effective date and gates on actual data
freshness rather than assuming that a successful weekly task delivered a new
weekly snapshot. The dataset is free and lists Government Open Data License
v1 ([catalog record](https://data.gov.tw/dataset/11452), [TDCC API catalog](https://openapi.tdcc.com.tw/),
[TDCC query description](https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock)).

The TPEx holdings catalog labels dataset 22812 as annual, but its live official
API returns report dates and data months (for example, `出表日期=1150918` and
`資料年月=11508` on 2026-09-23), showing a monthly snapshot. The collector
follows the live endpoint's monthly output and runs on the 25th to allow the
monthly report to appear. Recheck the endpoint if its publishing behavior
changes.

TPEx trading-attention and disposition notices use the official OpenAPI feeds
documented by [dataset 11395](https://data.gov.tw/dataset/11395) and
[dataset 11396](https://data.gov.tw/dataset/11396). They are filtered to
securities matched to the current stock master. The daily digest uses them as
abnormal-trading and active execution-restriction context for OTC candidates;
they do not add bullish or bearish votes. Natural after-close runs remain the
source of persistent records and market-coverage counts.

The TWT96U response has no source date. `capture_date` records the Taiwan-local
date when the endpoint was collected; it is an observation date, not a
provider-stated trading date. The weekday schedule may therefore retain an
unchanged observation on market holidays.

Quarterly profitability and balance-sheet values are stored in the same
normalized record tables as the other extensions. Their original API row is
retained in `payload`, while `source_record_fields` exposes the typed company,
period, revenue, margin, and balance-sheet values for downstream joins. The
daily signal scan uses published operating-margin changes, but does not yet
turn balance-sheet rows or Central Bank FX observations into stock-level
factors. Borrowing and dividend changes are visible as background only; they
do not count as bullish or bearish evidence.

TWSE's quarterly forecast-attainment feed is collected after close as a raw
observation only. A live field comparison found that for 2412 and 2515 its
"截至該季" actual matches the cumulative actual in the variance feed, while
its forecast field matches that feed's **quarterly** forecast rather than its
cumulative forecast. The fields therefore cannot be treated as a comparable
actual/forecast pair until TWSE clarifies or a broader cross-check resolves the
mapping. Its separate variance feed is polled with intraday official
disclosures. That endpoint selects rows whose quarterly actual-to-forecast
difference is at least 10%, or whose cumulative difference is at least 20%.
The alert preserves those explicitly paired quarterly and cumulative values,
deduplicates identical rows across forecast sequence IDs, and remains a
non-directional company event. Neither feed represents brokerage analyst
forecasts; only the variance event is notified, and neither adds a directional
family or enters the daily signal outcome table.

## Full-market scan and notification

The daily stock-trend scan uses the same Taiwan market-calendar check as the
regular stock collector. A matching row in holiday.taiwan skips the daily
price-based scan; its run is recorded with status skipped and source_status
market_closed. The notification task treats this as a normal skip. Intraday
company-event, revenue, and official insider-transfer scans continue on
weekends and holidays so recent disclosures are still processed.

September 3 is Military Day, but it has no separate hard-coded market-open
exception. It follows the same holiday.taiwan lookup: a matching row means
closed, and no matching row means the daily stock-trend scan may run. This
keeps September 3 aligned with the holiday table rather than a fixed
month/day override.

`market_signal_notify` is the final stage after the relevant collection DAGs:

1. Collection writes official normalized records into `crawler.source_records`,
   preserves first-seen/changed versions in `crawler.source_record_versions`,
   and logs each source run.
2. The intraday scan evaluates newly inserted company events, newly published
   monthly-revenue records, TWSE-listed material company-forecast variance
   rows, and official insider-transfer observations dated within the prior
   seven calendar days. Insider-transfer cards are neutral; a planned transfer
   is not treated as an executed sale. This catches Friday and
   holiday disclosures when the next collection runs. Common routine notices
   remain stored in the source table but are suppressed from Discord. Material
   headlines are labeled as a risk, a catalyst candidate, or direction not yet
   determined. Revenue notifications require a large year-over-year change or
   a material change from the prior report. Forecast-variance alerts show the
   issuer's quarterly and cumulative actual/forecast fields, publication date,
   coverage period, and official source; they do not infer a direction or
   represent sell-side analyst forecasts.
   Treasury-stock events use only the batched TWSE/TPEx official significant-
   message feeds. The classifier requires a company buyback in the headline,
   excludes subsidiary buybacks, and raises a neutral event only when the
   official title states a reporting threshold or parsed amount/disclosed
   share ratio reaches NT$300 million/2%. Cards extract planned and reported
   actual shares, and show the announcement-listed actual shares divided by planned shares
   only when both values are present and planned shares are positive. They also
   show reported cumulative treasury-stock ratio, executed amount, and price
   range when present. This derived ratio is descriptive progress context, not
   evidence of future buying pressure or a directional return signal. The legacy MOPS
   web-query collector is not used as a fallback feed for this signal.
3. The after-close scan reads every listed and OTC equity in
   `stock.全證券代碼相關資料表`, then joins official batch close/volume history,
   published revenue, and available point-in-time financial and ownership
   records. The legacy per-security `三大法人買賣` website-query table is not
   loaded. `tpex_institutional_flow_daily` is collected as a separate official
   OTC batch source but is not yet a directional input; TWSE still lacks a
   confirmed authorized market-wide stock-level flow batch for this project.
   Until these source paths pass natural-run freshness, coverage, PIT, and
   multi-date outcome checks, institutional flow remains unavailable/unknown
   for both markets. A missing flow row never means zero net trading.
4. A stock enters the daily candidate set only when at least three valid,
   non-neutral directional families are available: five-session price return
   (absolute move at least 8%), volume expansion (latest volume at least 2x
   the prior 20-session average and same-day move at least 3%), monthly-revenue
   strength or deterioration, quarterly operating-margin change, and
   insider-holdings change. Institutional accumulation/distribution is
   excluded until an authorized, point-in-time batch source has passed the
   validation gates above. Monthly revenue only
   compares with the immediately preceding report month; when the YoY level
   and its adjacent-month change meet opposite directional thresholds, revenue
   is explicitly neutral for that scan. Operating margin needs a change of at
   least 3 percentage points or a sign crossing; aggregated insider holdings
   need to change by at least 5%. Holdings are deduplicated by person within
   each company/report month, and a company-period with conflicting values for
   one person is excluded from this family. A majority of available directional families
   sets the displayed direction; a tie is labeled conflicting. The list sorts
   by the count of families aligned with that direction, not by a weighted
   score or predicted return. Candidate details also persist the number and
   names of broad input groups represented by the directional families. Price
   and volume are grouped together as one market-tape input because both come
   from the daily quote history; displaying them as two families does not make
   them independent evidence. This diagnostic does not change the three-family
   eligibility rule, family votes, score, or ranking. Borrowing, dividends,
   official indexes, margin balances, TWSE attention notices, and active TWSE dispositions remain
   context only. The after-close source extension collects TWSE's official
   `/announcement/notice` and `/announcement/punish` endpoints; it skips the
   blank attention sentinel returned before publication, filters to the
   listed equity master, and shows only same-date attention notices or
   dispositions whose parsed start/end dates include the price snapshot.
   Disposition cards retain the official reason and execution measures; the
   attention row links to TWSE's code/date-filtered lookup and the disposition
   row links to its official lookup page.
   Neither endpoint adds a directional family, changes the score, or changes
   ranking. The API's `NumberOfAnnouncement` is preserved as an uninterpreted
   source field because its meaning was not established from the published
   schema. Cards include the five-session
   comparison against the corresponding market index, counter-signals, a prior
   20-session closing-high confirmation level when available, a recent
   10-session closing-low weakening reference, and a no-chase instruction
   after a 15% five-session rise. These are observation rules, not probabilities,
   calibrated returns, or price targets.
   The daily market-breadth context reports distinct accepted CFI ES* quote
   codes by exchange against the current stock-master count, with quarantined
   codes and accepted rows lacking a price change shown separately. This is a
   row-count view, not a point-in-time universe or a trading-coverage verdict:
   a missing code may be suspended, have no trade, or reflect a collection
   gap. Missing quotes are never represented as zero change or flat.
   Coverage diagnostics also count stocks with a valid close and positive
   trading volume that have no institutional-flow row within the seven-day
   freshness window. The count is persisted by market and shown as an unknown
   coverage gap; the scanner does not infer zero institutional net flow or add
   a directional vote from a missing row. Older coverage snapshots keep this
   field null until naturally collected again.
   Candidate cards also compare the stock's five-session close return with
   the equal-stock median return for its current exchange and industry group,
   excluding the candidate from its own peer median. Both calculations use
   the same six synchronized closing dates and the existing market-index
   windows. The peer context is shown only when there are at least 10 other
   securities, at least 10 valid aligned peer returns, and at least 70% peer
   coverage against that candidate's leave-one-out universe in the current
   security master.
   Missing or unaligned quotes are excluded, never filled with zero. Cards
   disclose the median, stock-minus-industry difference, and valid/group count.
   This uses the scan-time industry classification and the existing raw-close
   series (not company-action-adjusted total returns); it is context only and
   does not alter directional families, score, candidate threshold, or rank.
   Market-breadth and TAIFEX futures/options context is read when the daily
   notification is formatted, after the candidate snapshot exists. Those reads
   use the candidate run's `started_at` availability cutoff as well as its
   effective date, so a delayed notification retry cannot include a source
   revision first collected after the scan. Missing scan time fails closed to
   no context; it is not a zero reading.
5. The daily digest goes to `新聞即時通` with its as-of date, source coverage,
   candidate count, and highest-ranked names. The intraday notifier sends new
   qualifying announcements in bounded batches. Delivery state is recorded in
   `crawler.market_signal_notifications`.

Both signal notifications use Discord embeds: the close digest starts with a
coverage and candidate-distribution summary, then gives each highlighted stock
its own colored card with supporting factors, opposing evidence, market-relative
performance, an observation instruction, and a price reference. The daily
digest enforces Discord's aggregate embed-character and embed-count limits;
when the full set does not fit, its footer states how many candidate cards were
omitted. Coverage reports the price and recent institutional-flow populations;
it does not imply that every optional source is present for every stock.
Each intraday disclosure, revenue, forecast-variance, or insider-transfer
observation gets its own card with a short
interpretation and a clickable official-source link. Batches respect Discord's
embed count and aggregate-text limits; notification ledger rows keep the exact
JSON payload that was sent.

Intraday monthly-revenue cards compare the current YoY rate only with the exact
preceding calendar month's reported rate; a missing month is shown as missing
instead of substituting an older observation. When three consecutive months
and all matching prior-year revenue amounts are available by the scan cutoff,
the card also shows their aggregate YoY rate and the exchange-reported
cumulative YoY rate. Incomplete months or invalid baselines are disclosed, not
filled with zero. These are explanatory context fields: the existing alert
thresholds and directional score are unchanged.

`crawler.market_signal_runs` and `crawler.market_signal_candidates` preserve
each analysis snapshot and its as-of date. A stale or incomplete price/source
set is marked partial in the digest. Legacy margin, large-holder, and monthly-
revenue tables are not treated as current inputs when their dates are stale.

Recent news context uses the crawler's `news.created_at` field, not a verified
article-page timestamp. Rows whose stored time is midnight also match by the
calendar date overlapping the 48-hour cutoff, which reduces false negatives
for date-only sources. Cards label these rows as date-only and disclose that
the original publication time and timezone are unverified; they remain
non-directional context.

Candidate-news matching is intentionally bounded to the latest 10,000 inserted
article IDs, then selects up to 6,000 rows in the time window. The scan records
the eligible row count inside that ID window and flags when the sample limit
truncates it. A no-match result is therefore a no-match in the scanned sample,
not evidence that the issuer had no coverage; no-match cards show eligible and
sample counts, and any card warns when the time-window sample was truncated.
The ID bound is retained because `news.created_at` is not indexed in the
current news table.

The PTS article parser reads publication time from the `<time>` element first,
then the page's `article:published_time` metadata and JSON-LD `datePublished`.
ISO timestamps with offsets are normalized to Asia/Taipei before the legacy
news table stores them as naive datetimes. Articles with no parseable source
time remain excluded by the news collector's valid-time filter.

The scheduled news collector logs per-source URL, parsed, valid-time, and
accepted counts, plus fetch/parse rejects, invalid-time quarantines, and
content/dedup rejects. Its coverage status distinguishes `no_urls`,
`no_parsed_articles`, `no_valid_time`, `empty_after_filter`, `partial`, and
`complete`; an Airflow task success alone does not assert source completeness.

The TVBS news crawler now reads the current `/realtime` page's JSON-LD
`NewsArticle` collection, including its `@graph` structure, and uses the
article page's `article-editor-content` when `articleBody` is absent. The
Newtalk crawler now reads dated `/news/summary/{date}/1` article links and
prefers the article page's `<time datetime>` or JSON-LD `datePublished` for its
publication time. Both changes were made after the 2026-10-03 natural batch and
still require a later scheduled run to verify counts and saved timestamps.
SETN's current `/viewall?p=N` layout uses `.news_list_item` and `/news/{id}`
links, with relative publication ages in `.time_box .time`; its former parser
expected `ViewAll.aspx`, `.newsItems`, `NewsID=`, and `#Content1`. The current
detail layout instead exposes the body in `.article_content_area` and a
`NewsArticle` JSON-LD block. A read-only probe at 2026-10-04 00:22 Taipei
found 30 list cards; the eight newest samples all had JSON-LD `+00:00` wall
times that matched the list's minute/hour age labels, while UTC interpretation
would place them eight hours ahead. A follow-up at 00:41 found `p=1` through
`p=4` returned the same 30 article IDs (CloudFront cache hits), so the crawler
now stops after the first repeated page and logs the repeat instead of treating
four identical pages as broader coverage. It currently proves only the latest
30 unique list cards, not complete site-wide coverage. The parser now accepts
the offset-zero wall time only when the captured list-relative age corroborates
it; missing,
unzoned, or mismatched evidence is withheld. Other explicit offsets are
normalized to Asia/Taipei. The source offset conflict remains visible in task
logs. The next natural run must verify URL/parsed/valid-time/accepted and
quarantine counts. No DAG was triggered for this change.

The intraday scanner uses the first-inserted `source_records.id` as its
notification cursor. On first activation it records a baseline at the current
maximum id and sends no historical announcement backlog. Later scans revisit a
10,000-id overlap and records updated in the prior day, then deduplicate by
content fingerprint. This catches delayed commits and material corrections
without repeating a delivered message. Only records whose publication/event
date falls in the prior seven calendar days are eligible; older backfills do
not become “breaking” notifications. Market, revenue, financial, and holdings features
use only effective periods at or before the price snapshot and publication
times available to the scan. A future corporate-action date is used only when
the source had already published it. This path does not yet settle historical
candidates against later returns or run a hindsight backtest, and later price
movement is never used to rewrite an earlier alert. The current
[TradingAgents project](https://github.com/TauricResearch/TradingAgents)
provides two transferable evaluation ideas: preserve each point-in-time
decision and later compare its realized return with a regional benchmark over
a declared holding window. The signal pipeline currently borrows the benchmark
comparison and preserves dated run/candidate snapshots, but it does not yet
claim outcome calibration or accuracy; that requires a separate, leakage-safe
evaluation over settled windows. Its multi-agent LLM debate is not used to
replace these deterministic, source-backed Taiwan-market rules.

This first scan covers the current listed/OTC **equity stock master**. ETFs,
warrants, bonds, and derivatives need their own instrument universes and signal
rules before they can be included. The 15-minute schedule polls official
filings and revenue endpoints; it is not a live quote feed. Daily price/volume
signals are calculated after the close. Exchange real-time prices require the
appropriate market-data contract and license; see [TWSE market-data terms](https://www.twse.com.tw/zh/products/information/use.html)
and [real-time information service](https://wwwc.twse.com.tw/zh/products/information/real-time.html).

## Runtime flow

1. `ensure_source_extension_schema` creates the metadata tables from the current schema definitions under a MySQL advisory lock. Existing tables are not upgraded.
2. It bootstraps the allowlisted catalog without re-enabling a source that an operator disabled.
3. `discover_approved_sources` selects only `enabled=1`, `approval_status='approved'`, `priority_score >= 75`, the static source contract, the DAG's cadence, available credentials, and active required fields.
4. Airflow maps compact source keys; each mapped task reloads and revalidates its source before fetching.
5. Each source is fetched, normalized, quality-checked, and idempotently upserted into `crawler.source_records`; first-seen and changed normalized rows are also appended to `crawler.source_record_versions`.
6. Every run is recorded in `crawler.source_runs` with Airflow DAG/task/map/attempt lineage; duplicate rows from upstream are counted separately.
7. A terminal verifier checks every mapped ingestion and requested downstream trigger so a failed source or skipped trigger cannot leave the collection DAG green.

`source_records.created_at` is the first insert time; `updated_at` is intended
to mean the last change to normalized source content. Re-fetching an identical
record refreshes `observed_at` while preserving `updated_at`; a changed filing
advances `updated_at`. `source_record_fields` uses the same content-change rule.
The current schema includes an append-only version table with the normalized
row, canonical content hash, capture time, capture basis, and Airflow lineage.
Each natural ingestion stores a row when a stable business key is first observed
or its normalized content changes; the current `source_records` row and version
insert commit in the same transaction. Identical re-fetches do not add versions.

This starts a forward-looking history only. It cannot recover vintages already
overwritten before the version table existed, prove the source publication time,
or record source rows that disappear from a later snapshot. The live scanner
reads `source_records`; historical as-of research must explicitly select the
latest `source_record_versions` row per stable key at or before its cutoff.
Existing timestamps are not backdated.

## Common extension fields

`crawler.source_records` keeps a stable cross-domain shape:

- identity: `source_key`, `record_key`, `record_hash`
- join keys: `target_domain`, `entity_type`, `entity_id`
- time: `event_time`, `effective_date`, `observed_at`
- signal: `event_type`, `title`, `metric_name`, `metric_value`, `value_text`, `unit`, `currency`
- lineage and quality: `source_url`, `payload`, `quality_status`, `created_at`, `updated_at`

`crawler.source_fields` stores each source's field purpose, target mapping,
type, and required flag.  `crawler.source_record_fields` stores the selected
typed values for every normalized record.  This allows new fields to be added
to the catalog without repeatedly altering legacy wide tables or reparsing the
raw JSON in every downstream query.

`crawler.source_record_versions` is the append-only normalized revision log.
Use `record_hash` as the stable row key and select by `captured_at` for an
as-of view; do not substitute `effective_date` for when DataFlow first observed
the value. Its `capture_basis` distinguishes a source response from a
pre-ingest current value preserved when a correction first appears after the
version log is enabled.

For an as-of read, choose the latest captured version per stable key first,
then apply quality/effective-date filters. This prevents a later correction
from making an older value reappear just because it matched a filter:

```sql
SELECT v.*
FROM source_record_versions AS v
WHERE v.captured_at <= %s
  AND NOT EXISTS (
      SELECT 1
      FROM source_record_versions AS newer
      WHERE newer.record_hash = v.record_hash
        AND newer.captured_at <= %s
        AND (
            newer.captured_at > v.captured_at
            OR (newer.captured_at = v.captured_at AND newer.id > v.id)
        )
  );
```

The current `market_signals.py` scanner does not use this history query yet;
it remains a foundation for PIT research after natural version rows accrue.

## Market notification delivery evidence

`crawler.market_signal_notifications` records transport status, platform HTTP
status, and the Discord receipt ID separately. A `sent` state is receipt-backed
only when `platform_receipt_id` is present. The current schema includes
`notification_error_type`, which stores only a safe exception class name (for
example, `ConnectTimeout`); it never stores exception text, webhook URLs,
headers, or credentials. HTTP responses remain represented by
`platform_http_status`.

## Adding a new approved task

Add a source definition in `dags/source_extension/registry.py` only when it has
a concrete join key and downstream purpose. Use an existing allowlisted
adapter where possible. A source becomes an Airflow mapped task only when its
registry row is approved, enabled, contract-valid, uses a supported schedule
(`intraday`, `daily`, `daily_after_close`, or `monthly`), and scores at least 75. New source
adapters must first be added to the code-controlled allowlist;
arbitrary database URLs cannot become network tasks.  To pause a source
without code changes:

```sql
UPDATE crawler.source_registry
SET enabled = 0, approval_status = 'disabled', updated_at = NOW()
WHERE source_key = 'example_source';
```

Arbitrary URLs, private APIs, unreviewed generated parsers, and broad source
discovery remain outside the automatic path.

## Portfolio data notifications

`portfolio_data_notify` is separate from ETL failure notifications.  It reuses
`stock.持股清單` rows where `notify_status=1` and the existing `新聞即時通`
Discord channel, but records its own delivery state in
`crawler.portfolio_notification_record`.

The daily source collection triggers the notification DAG after every mapped
source task succeeds; the notifier does not pair DAG runs by timestamp. Its
first run creates a baseline without sending historical alerts; later runs
send only new matched records. Each data alert is explicitly labeled as an
automatically detected data warning, with its reporting month, current metric,
source, and original data endpoint. Delivery records use `pending`, `sent`,
`failed`, and `baseline` states, so failed deliveries can be retried without
mixing them with the news ledger.

The first rules notify only when a tracked holding has a material company
event, a monthly-revenue warning (default: year-over-year revenue at or below
-20%, a turn from positive to negative, or a 20-point deterioration), or a new
dividend/ex-rights record.  Rules are stored in
`crawler.portfolio_alert_rules`, so thresholds can be changed without mixing
them into the news notification ledger.  Existing rule configuration is kept
when the DAG bootstraps its default rules.

### TPEx dividend batch source

The TPEx dividend collector uses the official data.gov.tw dataset resource
[`上櫃股利分派情形` (dataset 58285)](https://data.gov.tw/dataset/58285),
which documents daily updates and Government Open Data License v1. Its CSV
resource is
[`t187ap45_O.csv`](https://mopsfin.twse.com.tw/opendata/t187ap45_O.csv).
The previous registry route returned 2,483 archived dividend rows on the
2026-10-04 natural collection, with the newest source `出表日期` at ROC 1100804
(2021-08-04). The government platform says the older confirmed-only dataset
was unpublished because dataset 58285 provides more complete information
([closure notice](https://data.gov.tw/news/29113)).

A read-only check of the replacement CSV returned HTTP 200, 963 rows, and a
latest output date of ROC 1151003 (2026-10-03). Its 892 distinct codes matched
890 current OTC common-stock master codes; two feed codes are outside that
master, and two current master codes had no row. Missing rows are not turned
into zero dividends. The registry now uses a bounded CSV adapter with a fixed
host/path contract and maps the replacement's current field names. Dividend
context still requires a published event within the existing 45-day window
and never adds a directional vote. This change awaits the next natural daily
collection for accepted/rejected counts, effective-date coverage, and OTC
master reconciliation; no DAG was manually triggered. The 2026-10-04 08:00
natural schema bootstrap has since updated the database registry to the
replacement CSV URL. The 04:00 daily batch preceded that registry update: its
2,483 accepted rows still ended at source output date 2021-08-04, so that run
does not validate the replacement. Verify it in the next natural daily batch.

### Market-wide ex-right/ex-dividend preannouncement batches

Two official OpenAPI sources now feed the after-close collector:

- TWSE [`TWT48U_ALL`](https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL),
  government dataset [89748](https://data.gov.tw/dataset/89748). The dataset
  metadata says updates are irregular, so a successful daily poll does not prove
  that the publisher refreshed its announcement list that day.
- TPEx [`tpex_exright_prepost`](https://www.tpex.org.tw/openapi/v1/tpex_exright_prepost),
  government dataset [11634](https://data.gov.tw/dataset/11634), whose metadata
  says daily updates.

Both datasets are free under Government Open Data License v1. They enumerate
announced ex-right/ex-dividend dates and related values; blank values stay
missing. Candidate context filters to dates after the scan's market date. The
adapter stores the effective ex-date and action facts, but
leaves `event_time` unset because neither feed provides a reliable publication
timestamp. Candidate cards may show a matching future event only when the
source record was already updated before the scan cutoff. No matching row is
not displayed as proof that no action exists. This context does not add a
directional vote, and it does not yet adjust the outcome ledger's raw-close
returns.

A read-only worker preflight at 2026-10-05 01:40 Taipei received 58 TWSE rows
(58 distinct codes; 10 matched the current 1,054-code listed common-equity
master) and 80 TPEx rows (78 distinct codes, no duplicate code/date keys; 16
matched the current 892-code OTC common-equity master). These matched counts
describe securities with returned event rows, not feed completeness or master
coverage. The TWSE effective-date range was 2026-10-05–2026-10-28; TPEx was
2026-09-23–2026-11-05. This endpoint preflight is not a persisted source run.
Natural after-close collection must still verify HTTP status, accepted and
quarantined counts, duplicate keys, latest source/effective dates, matched
listed/OTC event codes, and source-catalog contract. Until that receipt exists,
an empty or stale response remains unknown.

\n