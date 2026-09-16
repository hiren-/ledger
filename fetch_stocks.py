from __future__ import annotations

import math
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from tradingview_screener import Query, col


# ============================================================
# CONFIG
# ============================================================

MARKET = "india"

# Minimum Piotroski score to flag
MIN_F_SCORE = 7

# Minimum market cap in INR
MIN_MARKET_CAP = 1_000_000_000  # ₹100 Cr

# Maximum number of stocks returned by TradingView
FETCH_LIMIT = 5000

OUTPUT_CSV = "piotroski_india.csv"
OUTPUT_XLSX = "piotroski_india.xlsx"


# ============================================================
# TRADINGVIEW FIELDS  (union of both source files)
# ============================================================

FIELDS = [
    # -------------------- Identification / profile --------------------
    "ticker",
    "name",
    "description",
    "exchange",
    "sector",
    "industry",
    "country",
    "type",
    "currency",
    "number_of_employees",
    "number_of_shareholders",

    # -------------------- Market / price --------------------
    "close",
    "open",
    "high",
    "low",
    "change",
    "market_cap_basic",
    "volume",
    "average_volume_10d_calc",
    "average_volume_30d_calc",
    "average_volume_90d_calc",
    "relative_volume_10d_calc",
    "beta_1_year",
    "beta_3_year",
    "volatility_1_week",
    "volatility_1_month",
    "volatility_1_year",

    # -------------------- 52-week / all-time range --------------------
    "price_52_week_high",
    "price_52_week_low",
    "High.1M",
    "Low.1M",
    "High.3M",
    "Low.3M",
    "High.6M",
    "Low.6M",
    "High.All",
    "Low.All",

    # -------------------- Valuation --------------------
    "price_earnings_ttm",
    "price_earnings_forward",
    "price_book_ratio",
    "price_book_fq",
    "price_sales_ratio",
    "price_earnings_growth_ttm",
    "price_free_cash_flow_ttm",
    "price_to_cash_f_operating_activities_ttm",
    "price_cash_flow_ttm",
    "enterprise_value_ebitda_ttm",
    "enterprise_value_ebit_ttm",
    "enterprise_value_fy",
    "enterprise_value_fq",
    "market_cap_to_ebitda_ttm",
    "dividend_yield_recent",
    "dividends_yield_fy",
    "dividend_payout_ratio_fy",
    "book_value_per_share_fy",
    "book_tangible_per_share_fy",
    "earnings_per_share_fy",
    "earnings_per_share_forecast_next_fy",
    "revenue_forecast_next_fy",
    "earnings_per_share_forecast_next_fq",
    "earnings_per_share_diluted_ttm",
    "earnings_per_share_fq",
    "basic_eps_net_income",
    "last_annual_eps",

    # -------------------- FY / TTM financials --------------------
    "net_income",
    "net_income_fy",
    "gross_profit",
    "gross_profit_fy",
    "gross_profit_fq",
    "total_revenue",
    "total_revenue_fy",
    "last_annual_revenue",
    "ebitda",
    "ebitda_fy",
    "ebitda_ttm",
    "ebit_fy",
    "ebit_ttm",

    "total_assets_fy",
    "total_liabilities_fy",
    "total_equity_fy",
    "total_current_assets_fy",
    "total_current_liabilities_fy",
    "working_capital_fy",
    "inventory_fy",
    "receivables_fy",
    "payables_fy",

    "long_term_debt_fy",
    "short_term_debt_fy",
    "total_debt_fy",
    "net_debt",
    "net_debt_fy",
    "cash_n_short_term_invest_fy",
    "cash_and_short_term_investments_fy",
    "cash_n_equivalents_fy",
    "cash_and_equivalents_fy",
    "goodwill",

    "cash_f_operating_activities_fy",
    "cash_f_operating_activities_ttm",
    "cash_f_investing_activities_fy",
    "cash_f_financing_activities_fy",
    "capital_expenditure_fy",
    "free_cash_flow_fy",
    "free_cash_flow_ttm",
    "free_cash_flow_margin_fy",
    "free_cash_flow_margin_ttm",

    "shares_outstanding_fy",
    "shares_outstanding",
    "total_common_shares_outstanding",

    # -------------------- Growth --------------------
    "ebitda_yoy_growth_fy",
    "ebitda_yoy_growth_fq",
    "ebitda_yoy_growth_ttm",
    "net_income_yoy_growth_fy",
    "net_income_yoy_growth_fq",
    "net_income_qoq_growth_fq",
    "net_income_yoy_growth_ttm",
    "gross_profit_yoy_growth_fy",
    "gross_profit_yoy_growth_fq",
    "gross_profit_qoq_growth_fq",
    "gross_profit_yoy_growth_ttm",
    "free_cash_flow_yoy_growth_fy",
    "free_cash_flow_yoy_growth_fq",
    "free_cash_flow_yoy_growth_ttm",
    "total_revenue_yoy_growth_fy",
    "total_assets_yoy_growth_fy",
    "eps_diluted_growth_percent_fy",
    "earnings_per_share_diluted_yoy_growth_fy",
    "earnings_per_share_diluted_yoy_growth_ttm",
    "earnings_per_share_diluted_qoq_growth_fq",

    # -------------------- Margins --------------------
    "gross_profit_margin_fy",
    "gross_margin",
    "ebitda_margin_fy",
    "oper_income_margin_fy",
    "operating_margin",
    "operating_margin_fy",
    "net_income_bef_disc_oper_margin_fy",
    "after_tax_margin",
    "pre_tax_margin",
    "pretax_margin_fy",
    "net_margin_fy",

    # -------------------- Liquidity / leverage ratios --------------------
    "current_ratio",
    "quick_ratio",
    "cash_ratio",
    "debt_to_equity",
    "debt_to_assets",
    "net_debt_to_ebitda",
    "total_debt_to_ebitda",
    "interest_coverage",

    # -------------------- Returns --------------------
    "return_on_assets",
    "return_on_equity",
    "return_on_invested_capital",
    "return_on_capital_employed",

    # -------------------- Efficiency / turnover --------------------
    "inventory_turnover_fy",
    "receivable_turnover_fy",
    "days_inventory_fy",
    "days_receivable_fy",
    "asset_turnover",

    # -------------------- Dividends --------------------
    "dividends_paid",
    "dps_common_stock_prim_issue_fy",
    "dps_common_stock_prim_issue_yoy_growth_fy",
    "dividends_per_share_fq",

    # -------------------- Performance --------------------
    "Perf.1M",
    "Perf.3M",
    "Perf.6M",
    "Perf.YTD",
    "Perf.Y",
    "Perf.5Y",
    "Perf.10Y",
    "Perf.All",

    # -------------------- TradingView technical ratings --------------------
    "Recommend.All",
    "Recommend.MA",
    "Recommend.Other",
    "Recommend.All|1",
    "Recommend.MA|1",
    "Recommend.Other|1",

    "Rec.Stoch.RSI",
    "Rec.WR",
    "Rec.BBPower",
    "Rec.UO",
    "Rec.Ichimoku",
    "Rec.VWMA",
    "Rec.HullMA9",

    # -------------------- Oscillators --------------------
    "RSI",
    "RSI[1]",
    "RSI7",
    "RSI7[1]",
    "Stoch.K",
    "Stoch.D",
    "Stoch.K[1]",
    "Stoch.D[1]",
    "CCI20",
    "CCI20[1]",
    "ADX",
    "ADX+DI",
    "ADX-DI",
    "ADX+DI[1]",
    "ADX-DI[1]",
    "AO",
    "AO[1]",
    "AO[2]",
    "Mom",
    "Mom[1]",
    "MACD.macd",
    "MACD.signal",
    "Stoch.RSI.K",
    "W.R",
    "BBPower",
    "UO",

    # -------------------- Moving averages --------------------
    "EMA5",
    "SMA5",
    "EMA10",
    "SMA10",
    "EMA20",
    "SMA20",
    "EMA30",
    "SMA30",
    "EMA50",
    "SMA50",
    "EMA100",
    "SMA100",
    "EMA200",
    "SMA200",
    "Ichimoku.BLine",
    "VWMA",
    "HullMA9",

    # -------------------- Pivot points --------------------
    "Pivot.M.Classic.S3",
    "Pivot.M.Classic.S2",
    "Pivot.M.Classic.S1",
    "Pivot.M.Classic.P",
    "Pivot.M.Classic.R1",
    "Pivot.M.Classic.R2",
    "Pivot.M.Classic.R3",

    # -------------------- Volatility / bands --------------------
    "ATR",
    "BB.upper",
    "BB.lower",
    "BB.basis",
]


# ============================================================
# HELPERS
# ============================================================

def safe_number(value):
    """Convert TradingView values to float. NaN/None/invalid -> None."""
    if value is None:
        return None
    try:
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    except (TypeError, ValueError):
        return None


def safe_div(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


def safe_mul(a, b):
    if a is None or b is None:
        return None
    return a * b


def safe_sub(a, b):
    if a is None or b is None:
        return None
    return a - b


def safe_add(*args):
    if any(a is None for a in args):
        return None
    return sum(args)


def pct_change(current, previous):
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous) - 1.0


def to_pct(x):
    return None if x is None else x * 100.0


def pct_from(current, reference):
    """% distance of current from reference."""
    if current is None or reference is None or reference == 0:
        return None
    return (current / reference - 1.0) * 100.0


def score_boolean(condition):
    return 1 if condition else 0


def rating_label(value):
    """TradingView numeric technical rating (-1..1) -> label."""
    if value is None:
        return None
    if value >= 0.5:
        return "Strong Buy"
    if value >= 0.1:
        return "Buy"
    if value > -0.1:
        return "Neutral"
    if value > -0.5:
        return "Sell"
    return "Strong Sell"


def trend_label(value):
    """Given a percentage distance, return a simple trend label."""
    if value is None:
        return None
    if value >= 10:
        return "Strong Uptrend"
    if value >= 2:
        return "Uptrend"
    if value > -2:
        return "Sideways"
    if value > -10:
        return "Downtrend"
    return "Strong Downtrend"


def higher_better(value, floor, ceiling):
    """Map value from [floor, ceiling] -> [0, 1]; None if value is None."""
    if value is None:
        return None
    v = (value - floor) / (ceiling - floor)
    return max(0.0, min(1.0, v))


def lower_better(value, best, worst):
    """Map value from [best, worst] -> [1, 0]; None if value is None."""
    if value is None:
        return None
    if value <= best:
        return 1.0
    if value >= worst:
        return 0.0
    return (worst - value) / (worst - best)


def band_score(value, ideal_low, ideal_high, worst_low, worst_high):
    """Return 0..1: 1 inside [ideal_low, ideal_high], fading outside."""
    if value is None:
        return None
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < ideal_low:
        if value <= worst_low:
            return 0.0
        return (value - worst_low) / (ideal_low - worst_low)
    if value >= worst_high:
        return 0.0
    return (worst_high - value) / (worst_high - ideal_high)


def weighted_avg(pairs):
    """pairs = list of (value_or_None, weight). Weighted mean of available."""
    num = 0.0
    den = 0.0
    for val, w in pairs:
        if val is None:
            continue
        num += val * w
        den += w
    if den == 0:
        return None
    return num / den


def to_score_100(x):
    """Convert a 0-1 score to a 0-100 score."""
    return None if x is None else round(x * 100.0, 2)


# ============================================================
# DOWNLOAD TRADINGVIEW DATA
# ============================================================

def download_india_stocks():
    print("Downloading Indian equities from TradingView...")

    query = (
        Query()
        .select(*FIELDS)
        .set_markets(MARKET)
        .where(col("market_cap_basic") >= MIN_MARKET_CAP)
        .order_by("market_cap_basic", ascending=False)
        .limit(FETCH_LIMIT)
    )

    count, df = query.get_scanner_data()

    print(f"TradingView returned {count} instruments.")
    print(f"DataFrame contains {len(df)} rows.")

    return df


# ============================================================
# PER-ROW METRICS
# ============================================================

def calculate_metrics(row):
    """
    Compute the union of:
      - Piotroski-style quality score (partial, only current-FY tests)
      - Extended (estimated) F-score
      - ~150 fundamental, valuation, dividend, technical and composite metrics
    """

    # ============================================================
    # RAW INPUTS
    # ============================================================
    # Price / market
    close = safe_number(row.get("close"))
    market_cap = safe_number(row.get("market_cap_basic"))
    volume = safe_number(row.get("volume"))
    avg_vol_10 = safe_number(row.get("average_volume_10d_calc"))
    avg_vol_30 = safe_number(row.get("average_volume_30d_calc"))
    avg_vol_90 = safe_number(row.get("average_volume_90d_calc"))
    rel_vol_10 = safe_number(row.get("relative_volume_10d_calc"))

    high_52w = safe_number(row.get("price_52_week_high"))
    low_52w = safe_number(row.get("price_52_week_low"))
    all_time_high = safe_number(row.get("High.All"))
    all_time_low = safe_number(row.get("Low.All"))

    # Income statement
    net_income = safe_number(row.get("net_income"))
    gross_profit = safe_number(row.get("gross_profit"))
    revenue = safe_number(row.get("total_revenue"))
    ebit = safe_number(row.get("ebit_ttm")) or safe_number(row.get("ebit_fy"))
    ebitda = (
        safe_number(row.get("ebitda"))
        or safe_number(row.get("ebitda_ttm"))
        or safe_number(row.get("ebitda_fy"))
    )

    # Balance sheet
    total_assets = safe_number(row.get("total_assets_fy"))
    total_liabilities = safe_number(row.get("total_liabilities_fy"))
    total_equity = safe_number(row.get("total_equity_fy"))
    total_current_assets = safe_number(row.get("total_current_assets_fy"))
    total_current_liabilities = safe_number(row.get("total_current_liabilities_fy"))
    working_capital = safe_number(row.get("working_capital_fy"))
    inventory = safe_number(row.get("inventory_fy"))
    receivables = safe_number(row.get("receivables_fy"))
    payables = safe_number(row.get("payables_fy"))

    long_term_debt = safe_number(row.get("long_term_debt_fy"))
    short_term_debt = safe_number(row.get("short_term_debt_fy"))
    total_debt = safe_number(row.get("total_debt_fy"))
    net_debt = safe_number(row.get("net_debt"))            # file1 field
    net_debt_fy = safe_number(row.get("net_debt_fy"))      # file2 field
    goodwill = safe_number(row.get("goodwill"))

    cash_sti = safe_number(row.get("cash_n_short_term_invest_fy"))
    cash_equiv = safe_number(row.get("cash_n_equivalents_fy"))
    cash_and_sti_fy = safe_number(row.get("cash_and_short_term_investments_fy"))
    cash_and_equiv_fy = safe_number(row.get("cash_and_equivalents_fy"))

    # Cash flows
    cfo = safe_number(row.get("cash_f_operating_activities_fy"))
    cfi = safe_number(row.get("cash_f_investing_activities_fy"))
    cff = safe_number(row.get("cash_f_financing_activities_fy"))
    capex = safe_number(row.get("capital_expenditure_fy"))
    fcf_fy = safe_number(row.get("free_cash_flow_fy"))
    fcf_ttm = safe_number(row.get("free_cash_flow_ttm"))

    # Per share / EPS
    shares = safe_number(row.get("shares_outstanding_fy"))
    bvps = safe_number(row.get("book_value_per_share_fy"))
    eps_ttm = safe_number(row.get("earnings_per_share_diluted_ttm"))
    eps_fy = safe_number(row.get("last_annual_eps")) or safe_number(row.get("earnings_per_share_fy"))
    eps_forecast_q = safe_number(row.get("earnings_per_share_forecast_next_fq"))
    eps_forward_fy = safe_number(row.get("earnings_per_share_forecast_next_fy"))
    rev_forward = safe_number(row.get("revenue_forecast_next_fy"))

    # Dividends
    dividends_paid = safe_number(row.get("dividends_paid"))
    div_yield_recent = safe_number(row.get("dividend_yield_recent"))

    # ============================================================
    # PIOTROSKI
    # ============================================================
    f1 = net_income is not None and net_income > 0
    roa = safe_div(net_income, total_assets)
    f2 = roa is not None and roa > 0
    f3 = cfo is not None and cfo > 0
    f4 = cfo is not None and net_income is not None and cfo > net_income
    profitability_score = sum(map(score_boolean, [f1, f2, f3, f4]))

    debt_ratio = safe_div(long_term_debt, total_assets)
    current_ratio_calc = safe_div(total_current_assets, total_current_liabilities)
    current_ratio = safe_number(row.get("current_ratio")) or current_ratio_calc
    quick_ratio = safe_number(row.get("quick_ratio"))
    cash_ratio = safe_number(row.get("cash_ratio"))
    debt_to_equity = safe_number(row.get("debt_to_equity"))

    no_new_shares = None
    debt_improved = None
    current_ratio_improved = None

    revenue_growth = safe_number(row.get("total_revenue_yoy_growth_fy"))
    earnings_growth = safe_number(row.get("net_income_yoy_growth_fy"))

    leverage_score = sum(
        1 for flag in (debt_improved, current_ratio_improved, no_new_shares) if flag is True
    )

    gross_margin_fy = safe_number(row.get("gross_profit_margin_fy"))
    gross_margin_growth = safe_number(row.get("gross_profit_yoy_growth_fy"))
    gross_margin_improved = None

    asset_turnover = safe_div(revenue, total_assets)
    asset_turnover_improved = None

    efficiency_score = sum(
        1 for flag in (gross_margin_improved, asset_turnover_improved) if flag is True
    )

    total_score = profitability_score + leverage_score + efficiency_score

    # Extended F-score (file2 heuristic estimate)
    est_lev = leverage_score
    if debt_to_equity is not None and debt_to_equity < 0.5:
        est_lev = min(3, est_lev + 1)
    if current_ratio is not None and current_ratio >= 1.5:
        est_lev = min(3, est_lev + 1)
    if current_ratio is not None and current_ratio >= 2.0:
        est_lev = min(3, est_lev + 0)

    est_eff = efficiency_score
    if gross_margin_fy is not None and gross_margin_fy >= 0.20:
        est_eff = min(2, est_eff + 1)
    if asset_turnover is not None and asset_turnover >= 0.5:
        est_eff = min(2, est_eff + 1)

    extended_f_score = profitability_score + est_lev + est_eff

    # ============================================================
    # MARGINS
    # ============================================================
    gross_margin_ttm = safe_number(row.get("gross_margin"))
    ebitda_margin = safe_number(row.get("ebitda_margin_fy"))
    operating_margin_fy = safe_number(row.get("oper_income_margin_fy"))
    operating_margin_ttm = safe_number(row.get("operating_margin"))
    operating_margin_fy_alt = safe_number(row.get("operating_margin_fy"))
    net_margin_ttm = safe_number(row.get("after_tax_margin"))
    net_margin_fy_befdisc = safe_number(row.get("net_income_bef_disc_oper_margin_fy"))
    net_margin_fy = safe_number(row.get("net_margin_fy"))
    pretax_margin_ttm = safe_number(row.get("pre_tax_margin"))
    pretax_margin_fy = safe_number(row.get("pretax_margin_fy"))
    fcf_margin_fy = safe_number(row.get("free_cash_flow_margin_fy"))
    fcf_margin_ttm = safe_number(row.get("free_cash_flow_margin_ttm"))

    # ============================================================
    # QUALITY / BALANCE-SHEET INSIGHTS
    # ============================================================
    total_liabilities_est = None
    if total_current_liabilities is not None and long_term_debt is not None:
        total_liabilities_est = total_current_liabilities + long_term_debt

    equity_est = None
    if total_assets is not None and total_liabilities_est is not None:
        equity_est = total_assets - total_liabilities_est

    equity_multiplier_est = safe_div(total_assets, equity_est)

    # DuPont using TTM net margin (file1)
    roe_dupont_est = None
    if net_margin_ttm is not None and asset_turnover is not None and equity_multiplier_est is not None:
        roe_dupont_est = net_margin_ttm / 100.0 * asset_turnover * equity_multiplier_est * 100.0

    # DuPont using FY net margin and true equity (file2)
    dupont_leverage = safe_div(total_assets, total_equity)
    dupont_roe_fy = safe_mul(safe_mul(net_margin_fy, asset_turnover), dupont_leverage)

    cash_to_debt = safe_div(cash_sti, total_debt)
    net_debt_to_ebitda = safe_div(net_debt, ebitda)
    total_debt_to_ebitda = safe_div(total_debt, ebitda)
    goodwill_pct_assets = safe_div(goodwill, total_assets)
    if goodwill_pct_assets is not None:
        goodwill_pct_assets *= 100.0

    cash_conversion = safe_div(cfo, net_income)
    accruals_ratio = None
    if net_income is not None and cfo is not None and total_assets:
        accruals_ratio = (net_income - cfo) / total_assets

    interest_cov = safe_number(row.get("interest_coverage"))

    # Partial Altman (file1)
    altman_z_partial_est = None
    if total_assets and total_assets != 0 and total_liabilities_est is not None:
        wc = None
        if total_current_assets is not None and total_current_liabilities is not None:
            wc = total_current_assets - total_current_liabilities
        if wc is not None and ebitda is not None and revenue is not None and equity_est is not None:
            x1 = wc / total_assets
            x3 = ebitda / total_assets
            x4 = safe_div(equity_est, total_liabilities_est) or 0
            altman_z_partial_est = round(3.26 * x1 + 6.72 * x3 + 1.05 * x4, 2)

    # Full Altman Z'' (file2)
    altman_z = None
    z_x1 = safe_div(working_capital, total_assets) if working_capital is not None else \
        safe_div(safe_sub(total_current_assets, total_current_liabilities), total_assets)
    z_x2 = safe_div(total_equity, total_assets)
    z_x3 = safe_div(ebit, total_assets)
    z_x4 = safe_div(total_equity, total_liabilities)
    if None not in (z_x1, z_x2, z_x3, z_x4):
        altman_z = 6.56 * z_x1 + 3.26 * z_x2 + 6.72 * z_x3 + 1.05 * z_x4

    altman_zone = None
    if altman_z is not None:
        if altman_z > 2.6:
            altman_zone = "Safe"
        elif altman_z >= 1.1:
            altman_zone = "Grey"
        else:
            altman_zone = "Distress"

    # ============================================================
    # DIVIDENDS
    # ============================================================
    payout_ratio = None
    if dividends_paid is not None and net_income is not None and net_income != 0:
        payout_ratio = abs(dividends_paid) / net_income * 100.0

    dps_fy = safe_number(row.get("dps_common_stock_prim_issue_fy"))
    dps_growth = safe_number(row.get("dps_common_stock_prim_issue_yoy_growth_fy"))

    # ============================================================
    # VALUATION
    # ============================================================
    pe = safe_number(row.get("price_earnings_ttm"))
    pe_fwd = safe_number(row.get("price_earnings_forward"))
    pb = safe_number(row.get("price_book_ratio"))
    ps = safe_number(row.get("price_sales_ratio"))
    peg = safe_number(row.get("price_earnings_growth_ttm"))
    p_fcf = safe_number(row.get("price_free_cash_flow_ttm"))
    p_cfo = safe_number(row.get("price_to_cash_f_operating_activities_ttm"))
    ev_ebitda = safe_number(row.get("enterprise_value_ebitda_ttm"))
    ev_ebit = safe_number(row.get("enterprise_value_ebit_ttm"))
    ev_fy = safe_number(row.get("enterprise_value_fy"))
    ev_mrq = safe_number(row.get("enterprise_value_fq"))
    mcap_to_ebitda = safe_number(row.get("market_cap_to_ebitda_ttm"))

    earnings_yield = (1.0 / pe) if (pe is not None and pe > 0) else None
    fcf_yield = safe_div(fcf_ttm, market_cap)
    cfo_yield = safe_div(cfo, market_cap)
    book_yield = (1.0 / pb) if (pb is not None and pb > 0) else None

    graham_number = None
    if eps_ttm is not None and bvps is not None and eps_ttm > 0 and bvps > 0:
        graham_number = math.sqrt(22.5 * eps_ttm * bvps)

    graham_upside_pct = None
    if graham_number is not None and close not in (None, 0):
        graham_upside_pct = (graham_number / close - 1.0) * 100.0

    graham_mos = None
    if graham_number is not None and close is not None and graham_number > 0:
        graham_mos = (graham_number - close) / graham_number

    forward_eps_annualized = eps_forecast_q * 4 if eps_forecast_q is not None else None
    forward_pe = safe_div(close, forward_eps_annualized)

    # ============================================================
    # PRICE RANGE
    # ============================================================
    pct_from_52w_high = pct_from(close, high_52w)
    pct_from_52w_low = pct_from(close, low_52w)
    pct_from_ath = pct_from(close, all_time_high)
    pct_from_atl = pct_from(close, all_time_low)

    range_52w_pos = None
    if close is not None and high_52w and low_52w and high_52w > low_52w:
        range_52w_pos = (close - low_52w) / (high_52w - low_52w) * 100.0

    # ============================================================
    # TECHNICAL RATINGS
    # ============================================================
    tech_rating_raw = safe_number(row.get("Recommend.All"))
    ma_rating_raw = safe_number(row.get("Recommend.MA"))
    osc_rating_raw = safe_number(row.get("Recommend.Other"))
    tech_rating_prev = safe_number(row.get("Recommend.All|1"))
    ma_rating_prev = safe_number(row.get("Recommend.MA|1"))
    osc_rating_prev = safe_number(row.get("Recommend.Other|1"))

    # ============================================================
    # MOVING AVERAGES / TREND
    # ============================================================
    sma20 = safe_number(row.get("SMA20"))
    sma50 = safe_number(row.get("SMA50"))
    sma100 = safe_number(row.get("SMA100"))
    sma200 = safe_number(row.get("SMA200"))
    ema20 = safe_number(row.get("EMA20"))
    ema50 = safe_number(row.get("EMA50"))
    ema200 = safe_number(row.get("EMA200"))

    def _pct_above(px, ma):
        if px is None or ma is None or ma == 0:
            return None
        return (px / ma - 1.0) * 100.0

    dist_sma20 = _pct_above(close, sma20)
    dist_sma50 = _pct_above(close, sma50)
    dist_sma100 = _pct_above(close, sma100)
    dist_sma200 = _pct_above(close, sma200)
    dist_ema20 = _pct_above(close, ema20)
    dist_ema50 = _pct_above(close, ema50)
    dist_ema200 = _pct_above(close, ema200)

    trend_flags = [
        1 if (close is not None and sma20 is not None and close > sma20) else 0,
        1 if (close is not None and sma50 is not None and close > sma50) else 0,
        1 if (close is not None and sma100 is not None and close > sma100) else 0,
        1 if (close is not None and sma200 is not None and close > sma200) else 0,
    ]
    trend_alignment = sum(trend_flags)

    golden_cross_20_50 = None
    if sma20 is not None and sma50 is not None:
        golden_cross_20_50 = "YES" if sma20 > sma50 else "NO"

    golden_cross_50_200 = None
    if sma50 is not None and sma200 is not None:
        golden_cross_50_200 = "YES" if sma50 > sma200 else "NO"

    death_cross_20_50 = None
    if sma20 is not None and sma50 is not None:
        death_cross_20_50 = "YES" if sma20 < sma50 else "NO"

    above_sma50 = (
        "YES" if (close is not None and sma50 is not None and close > sma50) else "NO"
    )
    above_sma200 = (
        "YES" if (close is not None and sma200 is not None and close > sma200) else "NO"
    )

    # ============================================================
    # OSCILLATORS
    # ============================================================
    rsi = safe_number(row.get("RSI"))
    rsi_prev = safe_number(row.get("RSI[1]"))
    rsi7 = safe_number(row.get("RSI7"))
    stoch_k = safe_number(row.get("Stoch.K"))
    stoch_d = safe_number(row.get("Stoch.D"))
    stoch_rsi = safe_number(row.get("Stoch.RSI.K"))
    cci = safe_number(row.get("CCI20"))
    adx = safe_number(row.get("ADX"))
    adx_plus = safe_number(row.get("ADX+DI"))
    adx_minus = safe_number(row.get("ADX-DI"))
    ao = safe_number(row.get("AO"))
    ao_prev = safe_number(row.get("AO[1]"))
    mom = safe_number(row.get("Mom"))
    mom_prev = safe_number(row.get("Mom[1]"))
    macd = safe_number(row.get("MACD.macd"))
    macd_sig = safe_number(row.get("MACD.signal"))
    wr = safe_number(row.get("W.R"))
    bbpower = safe_number(row.get("BBPower"))
    uo = safe_number(row.get("UO"))

    rsi_zone = None
    if rsi is not None:
        if rsi >= 70:
            rsi_zone = "Overbought"
        elif rsi <= 30:
            rsi_zone = "Oversold"
        elif rsi >= 60:
            rsi_zone = "Bullish"
        elif rsi <= 40:
            rsi_zone = "Bearish"
        else:
            rsi_zone = "Neutral"

    adx_strength = None
    if adx is not None:
        if adx >= 40:
            adx_strength = "Very Strong"
        elif adx >= 25:
            adx_strength = "Strong"
        elif adx >= 20:
            adx_strength = "Developing"
        else:
            adx_strength = "Weak/Ranging"

    macd_cross = None
    if macd is not None and macd_sig is not None:
        if macd > macd_sig:
            macd_cross = "Bullish"
        elif macd < macd_sig:
            macd_cross = "Bearish"
        else:
            macd_cross = "Flat"

    # ============================================================
    # VOLATILITY / BANDS
    # ============================================================
    atr = safe_number(row.get("ATR"))
    bb_upper = safe_number(row.get("BB.upper"))
    bb_lower = safe_number(row.get("BB.lower"))
    bb_basis = safe_number(row.get("BB.basis"))

    atr_pct = None
    if atr is not None and close not in (None, 0):
        atr_pct = atr / close * 100.0

    bb_position = None
    if close is not None and bb_upper is not None and bb_lower is not None and bb_upper > bb_lower:
        bb_position = (close - bb_lower) / (bb_upper - bb_lower) * 100.0

    bb_width = None
    if bb_upper is not None and bb_lower is not None and bb_basis:
        bb_width = (bb_upper - bb_lower) / bb_basis * 100.0

    # ============================================================
    # VOLUME
    # ============================================================
    volume_ratio_30 = None
    if volume is not None and avg_vol_30:
        volume_ratio_30 = volume / avg_vol_30

    volume_ratio_90 = None
    if volume is not None and avg_vol_90:
        volume_ratio_90 = volume / avg_vol_90

    # ============================================================
    # PERFORMANCE
    # ============================================================
    perf_1m = safe_number(row.get("Perf.1M"))
    perf_3m = safe_number(row.get("Perf.3M"))
    perf_6m = safe_number(row.get("Perf.6M"))
    perf_ytd = safe_number(row.get("Perf.YTD"))
    perf_1y = safe_number(row.get("Perf.Y"))
    perf_5y = safe_number(row.get("Perf.5Y"))
    perf_10y = safe_number(row.get("Perf.10Y"))
    perf_all = safe_number(row.get("Perf.All"))

    # ============================================================
    # COMPOSITE SCORES (absolute, file2)
    # ============================================================
    valuation_score_components = [
        (lower_better(pe, 10, 60), 0.25),
        (lower_better(pb, 1, 10), 0.15),
        (lower_better(ps, 0.5, 15), 0.10),
        (lower_better(ev_ebitda, 6, 30), 0.15),
        (lower_better(p_fcf, 10, 50), 0.10),
        (higher_better(earnings_yield, 0.01, 0.15), 0.10),
        (higher_better(fcf_yield, 0.0, 0.10), 0.10),
        (higher_better(div_yield_recent, 0.0, 0.05), 0.05),
    ]
    valuation_score = weighted_avg(valuation_score_components)

    roe = safe_number(row.get("return_on_equity"))
    roic = safe_number(row.get("return_on_invested_capital"))
    roce = safe_number(row.get("return_on_capital_employed"))

    quality_score_components = [
        (higher_better(roe, 0.0, 0.30), 0.20),
        (higher_better(roic, 0.0, 0.25), 0.20),
        (higher_better(roa, 0.0, 0.20), 0.15),
        (higher_better(gross_margin_fy, 0.05, 0.50), 0.10),
        (higher_better(operating_margin_fy_alt, 0.0, 0.30), 0.10),
        (higher_better(net_margin_fy, 0.0, 0.25), 0.10),
        (higher_better(cash_conversion, 0.5, 1.5), 0.10),
        (higher_better(
            extended_f_score / 9.0 if extended_f_score else None, 0.4, 1.0
        ), 0.05),
    ]
    quality_score = weighted_avg(quality_score_components)

    ebitda_growth = safe_number(row.get("ebitda_yoy_growth_fy"))
    gp_growth = gross_margin_growth
    fcf_growth = safe_number(row.get("free_cash_flow_yoy_growth_fy"))
    assets_growth = safe_number(row.get("total_assets_yoy_growth_fy"))
    eps_growth = safe_number(row.get("eps_diluted_growth_percent_fy"))

    growth_score_components = [
        (higher_better(revenue_growth, -0.10, 0.30), 0.25),
        (higher_better(earnings_growth, -0.10, 0.40), 0.25),
        (higher_better(ebitda_growth, -0.10, 0.30), 0.15),
        (higher_better(gp_growth, -0.10, 0.30), 0.10),
        (higher_better(fcf_growth, -0.20, 0.40), 0.10),
        (higher_better(eps_growth, -0.10, 0.40), 0.15),
    ]
    growth_score = weighted_avg(growth_score_components)

    financial_health_score_components = [
        (lower_better(debt_ratio, 0.0, 0.60), 0.20),
        (lower_better(net_debt_to_ebitda, 0.0, 4.0), 0.20),
        (higher_better(current_ratio, 0.8, 2.5), 0.15),
        (higher_better(quick_ratio, 0.5, 2.0), 0.10),
        (higher_better(interest_cov, 1.0, 10.0), 0.15),
        (higher_better(altman_z, 1.0, 3.0), 0.20),
    ]
    financial_health_score = weighted_avg(financial_health_score_components)

    momentum_score_components = [
        (higher_better(perf_1m, -10, 20), 0.15),
        (higher_better(perf_3m, -15, 30), 0.20),
        (higher_better(perf_6m, -20, 40), 0.20),
        (higher_better(perf_1y, -25, 60), 0.20),
        (higher_better(tech_rating_raw, -1.0, 1.0), 0.25),
    ]
    momentum_score = weighted_avg(momentum_score_components)

    overall_score = weighted_avg([
        (valuation_score, 0.20),
        (quality_score, 0.30),
        (growth_score, 0.20),
        (financial_health_score, 0.15),
        (momentum_score, 0.15),
    ])

    # ============================================================
    # SIGNALS
    # ============================================================
    signals = []
    if extended_f_score >= MIN_F_SCORE:
        signals.append("Piotroski Pass")
    if roe is not None and roe >= 0.20:
        signals.append("High ROE")
    if roic is not None and roic >= 0.15:
        signals.append("High ROIC")
    if net_margin_fy is not None and net_margin_fy >= 0.15:
        signals.append("High Net Margin")
    if cash_conversion is not None and cash_conversion >= 1.0:
        signals.append("Strong Cash Conversion")
    if fcf_yield is not None and fcf_yield >= 0.05:
        signals.append("High FCF Yield")
    if earnings_yield is not None and earnings_yield >= 0.08:
        signals.append("High Earnings Yield")
    if div_yield_recent is not None and div_yield_recent >= 0.03:
        signals.append("High Dividend Yield")
    if graham_mos is not None and graham_mos >= 0.20:
        signals.append("Graham Undervalued")
    if altman_zone == "Safe":
        signals.append("Altman Safe")
    if net_debt_to_ebitda is not None and net_debt_to_ebitda <= 1.0:
        signals.append("Low Net Debt/EBITDA")
    if revenue_growth is not None and revenue_growth >= 0.15:
        signals.append("Revenue Accelerating")
    if earnings_growth is not None and earnings_growth >= 0.20:
        signals.append("Earnings Accelerating")
    if trend_alignment == 4:
        signals.append("Full Uptrend")
    if golden_cross_20_50 == "YES" and golden_cross_50_200 == "YES":
        signals.append("Golden Cross Stack")
    if rsi_zone == "Oversold":
        signals.append("RSI Oversold")
    if rsi_zone == "Overbought":
        signals.append("RSI Overbought")
    if macd_cross == "Bullish":
        signals.append("MACD Bullish")
    if bb_position is not None and bb_position <= 5:
        signals.append("Near Lower BB")
    if bb_position is not None and bb_position >= 95:
        signals.append("Near Upper BB")
    if volume_ratio_30 is not None and volume_ratio_30 >= 2.0:
        signals.append("Volume Spike")
    if tech_rating_raw is not None and tech_rating_raw >= 0.5:
        signals.append("Tech Strong Buy")
    if tech_rating_raw is not None and tech_rating_raw <= -0.5:
        signals.append("Tech Strong Sell")
    if overall_score is not None and overall_score >= 0.75:
        signals.append("Top Composite")

    signal_str = " | ".join(signals) if signals else ""

    # ============================================================
    # OUTPUT
    # ============================================================
    return pd.Series({

        # ========== PIOTROSKI ==========
        "Piotroski_F_Score": total_score,
        "Extended_F_Score": extended_f_score,
        "Profitability_Score": f"{profitability_score}/4",
        "Leverage_Liquidity_Score": f"{leverage_score}/3 (partial - no historical deltas)",
        "Efficiency_Score": f"{efficiency_score}/2 (partial - no historical deltas)",
        "PASS": "YES" if total_score >= MIN_F_SCORE else "NO",

        # ========== PROFITABILITY ==========
        "Net_Income": net_income,
        "Total_Assets": total_assets,
        "ROA": to_pct(roa),
        "Operating_Cash_Flow": cfo,
        "CFO_GT_Net_Income": "YES" if f4 else "NO",
        "ROE": roe,
        "ROIC": roic,
        "ROCE": roce,
        "ROE_DuPont_Est": roe_dupont_est,
        "DuPont_ROE_FY": dupont_roe_fy,
        "DuPont_Leverage": dupont_leverage,

        # ========== EQUITY / LIABILITIES ESTIMATES ==========
        "Equity_Multiplier_Est": equity_multiplier_est,
        "Equity_Est": equity_est,
        "Total_Liabilities_Est": total_liabilities_est,

        # ========== MARGINS ==========
        "Gross_Margin_FY": gross_margin_fy,
        "Gross_Margin_TTM": gross_margin_ttm,
        "EBITDA_Margin": ebitda_margin,
        "Operating_Margin_FY": operating_margin_fy,
        "Operating_Margin_TTM": operating_margin_ttm,
        "Operating_Margin": operating_margin_fy_alt,
        "Net_Margin_TTM": net_margin_ttm,
        "Net_Margin_FY": net_margin_fy_befdisc,
        "Net_Margin": net_margin_fy,
        "Pretax_Margin_TTM": pretax_margin_ttm,
        "Pretax_Margin": pretax_margin_fy,
        "FCF_Margin_FY": fcf_margin_fy,
        "FCF_Margin_TTM": fcf_margin_ttm,

        # ========== LEVERAGE / LIQUIDITY / BALANCE SHEET ==========
        "Long_Term_Debt": long_term_debt,
        "Short_Term_Debt": short_term_debt,
        "Total_Debt": total_debt,
        "Net_Debt": net_debt,
        "Net_Debt_FY": net_debt_fy,
        "Net_Debt_to_EBITDA": net_debt_to_ebitda,
        "Total_Debt_to_EBITDA": total_debt_to_ebitda,
        "Debt_to_Assets_Pct": to_pct(debt_ratio),
        "Debt_to_Equity": debt_to_equity,
        "Current_Ratio": current_ratio,
        "Quick_Ratio": quick_ratio,
        "Cash_Ratio": cash_ratio,
        "Cash_and_ST_Investments": cash_sti,
        "Cash_and_Equivalents": cash_equiv,
        "Cash_and_ST_Investments_FY": cash_and_sti_fy,
        "Cash_and_Equivalents_FY": cash_and_equiv_fy,
        "Cash_to_Debt": cash_to_debt,
        "Working_Capital": working_capital,
        "Inventory": inventory,
        "Receivables": receivables,
        "Payables": payables,
        "Goodwill": goodwill,
        "Goodwill_Pct_of_Assets": goodwill_pct_assets,
        "Altman_Z_Partial_Est": altman_z_partial_est,
        "Altman_Z": altman_z,
        "Altman_Zone": altman_zone,
        "Interest_Coverage": interest_cov,

        # ========== CASH FLOW EXTRAS ==========
        "Cash_Flow_Investing": cfi,
        "Cash_Flow_Financing": cff,
        "Capital_Expenditure": capex,
        "Free_Cash_Flow_FY": fcf_fy,
        "Free_Cash_Flow_TTM": fcf_ttm,

        # ========== CASH CONVERSION / ACCRUALS ==========
        "Cash_Conversion": cash_conversion,
        "Accruals_Ratio": accruals_ratio,

        # ========== GROWTH ==========
        "Revenue_Growth_YoY_FY": revenue_growth,
        "Net_Income_Growth_YoY_FY": earnings_growth,
        "Net_Income_Growth_YoY_TTM": safe_number(row.get("net_income_yoy_growth_ttm")),
        "Net_Income_Growth_QoQ": safe_number(row.get("net_income_qoq_growth_fq")),
        "Gross_Profit_Growth_YoY_FY": gross_margin_growth,
        "Gross_Profit_Growth_YoY_TTM": safe_number(row.get("gross_profit_yoy_growth_ttm")),
        "FCF_Growth_YoY_FY": fcf_growth,
        "FCF_Growth_YoY_TTM": safe_number(row.get("free_cash_flow_yoy_growth_ttm")),
        "EBITDA": ebitda,
        "EBIT": ebit,
        "EBITDA_Growth_YoY_FY": ebitda_growth,
        "EBITDA_Growth_YoY_TTM": safe_number(row.get("ebitda_yoy_growth_ttm")),
        "EPS_Growth_YoY_FY": safe_number(row.get("earnings_per_share_diluted_yoy_growth_fy")),
        "EPS_Growth_YoY_TTM": safe_number(row.get("earnings_per_share_diluted_yoy_growth_ttm")),
        "EPS_Growth_QoQ": safe_number(row.get("earnings_per_share_diluted_qoq_growth_fq")),
        "Assets_Growth_YoY": assets_growth,

        # ========== PER-SHARE / EPS ==========
        "Shares_Outstanding": shares,
        "EPS_TTM": eps_ttm,
        "EPS_FY": eps_fy,
        "EPS_Forecast_Next_Q": eps_forecast_q,
        "EPS_Forward_FY": eps_forward_fy,
        "Forward_PE_Est": forward_pe,
        "Book_Value_Per_Share": bvps,
        "Tangible_Book_Value_Per_Share": safe_number(row.get("book_tangible_per_share_fy")),
        "Graham_Number": graham_number,
        "Graham_Upside_Pct": graham_upside_pct,
        "Graham_Margin_of_Safety": graham_mos,

        # ========== DIVIDENDS ==========
        "Dividends_Paid": dividends_paid,
        "Payout_Ratio_Pct": payout_ratio,
        "Dividend_Payout_Ratio": safe_number(row.get("dividend_payout_ratio_fy")),
        "DPS_FY": dps_fy,
        "DPS_Growth_YoY": dps_growth,
        "Dividend_Yield_Fwd": div_yield_recent,
        "Dividend_Yield_FY": safe_number(row.get("dividends_yield_fy")),

        # ========== VALUATION ==========
        "PE": pe,
        "PE_Forward": pe_fwd,
        "PB": pb,
        "PS": ps,
        "PEG": peg,
        "Price_to_FCF": p_fcf,
        "Price_to_CFO": p_cfo,
        "EV_EBITDA": ev_ebitda,
        "EV_EBIT": ev_ebit,
        "Enterprise_Value_FY": ev_fy,
        "Enterprise_Value_MRQ": ev_mrq,
        "MCap_to_EBITDA": mcap_to_ebitda,
        "Earnings_Yield": earnings_yield,
        "FCF_Yield": fcf_yield,
        "CFO_Yield": cfo_yield,
        "Book_Yield": book_yield,

        # ========== COMPANY PROFILE ==========
        "Employees": safe_number(row.get("number_of_employees")),
        "Shareholders": safe_number(row.get("number_of_shareholders")),
        "Beta_1Y": safe_number(row.get("beta_1_year")),
        "Beta_3Y": safe_number(row.get("beta_3_year")),
        "Volatility_1W": safe_number(row.get("volatility_1_week")),
        "Volatility_1M": safe_number(row.get("volatility_1_month")),
        "Volatility_1Y": safe_number(row.get("volatility_1_year")),

        # ========== REVENUE / PROFIT LEVELS ==========
        "Revenue": revenue,
        "Last_Annual_Revenue": safe_number(row.get("last_annual_revenue")),
        "Revenue_Forward": rev_forward,
        "Gross_Profit": gross_profit,

        # ========== PRICE / MARKET ==========
        "Close": close,
        "Market_Cap": market_cap,
        "Volume": volume,
        "Avg_Volume_10D": avg_vol_10,
        "Avg_Volume_30D": avg_vol_30,
        "Avg_Volume_90D": avg_vol_90,
        "Relative_Volume_10D": rel_vol_10,
        "Volume_Ratio_30D": volume_ratio_30,
        "Volume_Ratio_90D": volume_ratio_90,

        # ========== 52W / ALL-TIME ==========
        "High_52W": high_52w,
        "Low_52W": low_52w,
        "Distance_from_52W_High_%": pct_from_52w_high,
        "Distance_from_52W_Low_%": pct_from_52w_low,
        "52W_Range_Position_%": range_52w_pos,
        "All_Time_High": all_time_high,
        "All_Time_Low": all_time_low,
        "Pct_From_AllTime_High": pct_from_ath,
        "Pct_From_AllTime_Low": pct_from_atl,

        # ========== TREND / MOVING AVERAGES ==========
        "SMA20": sma20,
        "SMA50": sma50,
        "SMA100": sma100,
        "SMA200": sma200,
        "EMA20": ema20,
        "EMA50": ema50,
        "EMA200": ema200,
        "Dist_SMA20_%": dist_sma20,
        "Dist_SMA50_%": dist_sma50,
        "Dist_SMA100_%": dist_sma100,
        "Dist_SMA200_%": dist_sma200,
        "Dist_EMA20_%": dist_ema20,
        "Dist_EMA50_%": dist_ema50,
        "Dist_EMA200_%": dist_ema200,
        "Trend_Alignment_0to4": trend_alignment,
        "Trend_Label": trend_label(dist_sma200),
        "Above_SMA50": above_sma50,
        "Above_SMA200": above_sma200,
        "Golden_Cross_20_50": golden_cross_20_50,
        "Golden_Cross_50_200": golden_cross_50_200,
        "Death_Cross_20_50": death_cross_20_50,

        # ========== OSCILLATORS ==========
        "RSI": rsi,
        "RSI_Prev": rsi_prev,
        "RSI7": rsi7,
        "RSI_Zone": rsi_zone,
        "Stoch_K": stoch_k,
        "Stoch_D": stoch_d,
        "Stoch_RSI_K": stoch_rsi,
        "CCI20": cci,
        "ADX": adx,
        "ADX_Plus_DI": adx_plus,
        "ADX_Minus_DI": adx_minus,
        "ADX_Strength": adx_strength,
        "AO": ao,
        "AO_Prev": ao_prev,
        "Momentum": mom,
        "Momentum_Prev": mom_prev,
        "MACD": macd,
        "MACD_Signal": macd_sig,
        "MACD_Cross": macd_cross,
        "Williams_R": wr,
        "BBPower": bbpower,
        "Ultimate_Osc": uo,

        # ========== VOLATILITY / BANDS ==========
        "ATR": atr,
        "ATR_%": atr_pct,
        "BB_Upper": bb_upper,
        "BB_Lower": bb_lower,
        "BB_Basis": bb_basis,
        "BB_Position_%": bb_position,
        "BB_Width_%": bb_width,

        # ========== PERFORMANCE ==========
        "Perf_1M": perf_1m,
        "Perf_3M": perf_3m,
        "Perf_6M": perf_6m,
        "Perf_YTD": perf_ytd,
        "Perf_1Y": perf_1y,
        "Perf_5Y": perf_5y,
        "Perf_10Y": perf_10y,
        "Perf_All": perf_all,

        # ========== TECHNICAL RATINGS ==========
        "Tech_Rating_Value": tech_rating_raw,
        "Tech_Rating": rating_label(tech_rating_raw),
        "MA_Rating_Value": ma_rating_raw,
        "MA_Rating": rating_label(ma_rating_raw),
        "Oscillator_Rating_Value": osc_rating_raw,
        "Oscillator_Rating": rating_label(osc_rating_raw),
        "Tech_Rating_Prev": tech_rating_prev,
        "MA_Rating_Prev": ma_rating_prev,
        "Oscillator_Rating_Prev": osc_rating_prev,

        # ========== COMPOSITE SCORES (absolute) ==========
        "Valuation_Score": to_score_100(valuation_score),
        "Quality_Score": to_score_100(quality_score),
        "Growth_Score": to_score_100(growth_score),
        "Financial_Health_Score": to_score_100(financial_health_score),
        "Momentum_Score": to_score_100(momentum_score),
        "Overall_Score": to_score_100(overall_score),

        # ========== SIGNALS ==========
        "Signals": signal_str,
        "Signal_Count": len(signals),
    })


# ============================================================
# CROSS-SECTIONAL INSIGHTS (sector-relative percentile scores)
# ============================================================

# Metrics where a HIGHER raw value maps to a HIGHER "better" score.
HIGHER_IS_BETTER = [
    "ROE", "ROA", "ROIC", "Piotroski_F_Score",
    "Revenue_Growth_YoY_FY", "Net_Income_Growth_YoY_FY", "EBITDA_Growth_YoY_FY",
    "Gross_Margin_TTM", "Operating_Margin_TTM", "Net_Margin_TTM", "FCF_Margin_TTM",
    "Dividend_Yield_Fwd", "Current_Ratio", "Tech_Rating_Value",
    "Perf_1Y", "Perf_6M", "Perf_3M",
]

LOWER_IS_BETTER = [
    "PE", "PB", "PS", "EV_EBITDA", "Debt_to_Equity", "Net_Debt_to_EBITDA",
]


def add_sector_relative_scores(result: pd.DataFrame) -> pd.DataFrame:
    """
    Adds sector-relative percentile scores (0-100) for a curated set of
    fundamental / valuation / momentum metrics, then rolls them up into
    Sector_Quality / Sector_Value / Sector_Growth / Sector_Momentum and
    a Sector_Composite score per stock.
    """

    if "sector" not in result.columns:
        result["sector"] = "Unknown"

    result["sector"] = result["sector"].fillna("Unknown")

    def sector_percentile(series: pd.Series, higher_is_better: bool) -> pd.Series:
        pct = series.groupby(result["sector"]).rank(pct=True, na_option="keep") * 100.0
        if not higher_is_better:
            pct = 100.0 - pct
        return pct

    quality_cols, value_cols, growth_cols, momentum_cols = [], [], [], []

    for column in ["ROE", "ROA", "ROIC", "Piotroski_F_Score", "Current_Ratio"]:
        if column in result.columns:
            out_col = f"SectorPctile_{column}"
            result[out_col] = sector_percentile(result[column], True)
            quality_cols.append(out_col)

    for column in ["PE", "PB", "PS", "EV_EBITDA", "Debt_to_Equity", "Net_Debt_to_EBITDA"]:
        if column in result.columns:
            out_col = f"SectorPctile_{column}"
            result[out_col] = sector_percentile(result[column], False)
            value_cols.append(out_col)

    for column in ["Revenue_Growth_YoY_FY", "Net_Income_Growth_YoY_FY", "EBITDA_Growth_YoY_FY"]:
        if column in result.columns:
            out_col = f"SectorPctile_{column}"
            result[out_col] = sector_percentile(result[column], True)
            growth_cols.append(out_col)

    for column in ["Perf_1Y", "Perf_6M", "Perf_3M", "Tech_Rating_Value"]:
        if column in result.columns:
            out_col = f"SectorPctile_{column}"
            result[out_col] = sector_percentile(result[column], True)
            momentum_cols.append(out_col)

    result["Sector_Quality_Score"] = (
        result[quality_cols].mean(axis=1) if quality_cols else np.nan
    )
    result["Sector_Value_Score"] = (
        result[value_cols].mean(axis=1) if value_cols else np.nan
    )
    result["Sector_Growth_Score"] = (
        result[growth_cols].mean(axis=1) if growth_cols else np.nan
    )
    result["Sector_Momentum_Score"] = (
        result[momentum_cols].mean(axis=1) if momentum_cols else np.nan
    )

    score_frame = result[[
        "Sector_Quality_Score",
        "Sector_Value_Score",
        "Sector_Growth_Score",
        "Sector_Momentum_Score",
    ]]
    result["Sector_Composite_Score"] = score_frame.mean(axis=1)

    return result


def build_sector_summary(result: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sector-level averages for a quick market-map view."""

    agg_map = {
        "Piotroski_F_Score": "mean",
        "ROE": "mean",
        "ROA": "mean",
        "PE": "median",
        "PB": "median",
        "EV_EBITDA": "median",
        "Dividend_Yield_Fwd": "mean",
        "Revenue_Growth_YoY_FY": "mean",
        "Net_Income_Growth_YoY_FY": "mean",
        "Composite_Score": "mean",
        "Sector_Composite_Score": "mean",
        "Market_Cap": "sum",
    }
    agg_map = {k: v for k, v in agg_map.items() if k in result.columns}

    summary = (
        result.groupby("sector")
        .agg(agg_map)
        .rename(columns={"Market_Cap": "Total_Market_Cap"})
        .sort_values("Total_Market_Cap", ascending=False)
        .reset_index()
    )
    return summary


# ============================================================
# MAIN
# ============================================================

def main():

    df = download_india_stocks()

    if df.empty:
        raise RuntimeError("TradingView returned no data.")

    print("\nCalculating Piotroski metrics, valuation, quality, growth,")
    print("financial health, technicals and composite scores...")

    metrics = df.apply(calculate_metrics, axis=1)

    result = pd.concat(
        [df.reset_index(drop=True), metrics.reset_index(drop=True)],
        axis=1,
    )

    # --------------------------------------------------------
    # Remove duplicates
    # --------------------------------------------------------
    if "ticker" in result.columns:
        result = result.drop_duplicates(subset=["ticker"])

    # --------------------------------------------------------
    # Filter actual equities
    # --------------------------------------------------------
    if "type" in result.columns:
        result = result[result["type"].isin(["stock"])]

    # --------------------------------------------------------
    # Sector-relative percentile / composite scores
    # --------------------------------------------------------
    print("Computing sector-relative Quality / Value / Growth / Momentum scores...")
    result = add_sector_relative_scores(result)

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------
    result = result.sort_values(
        by=["Piotroski_F_Score", "Overall_Score", "Market_Cap"],
        ascending=[False, False, False],
        na_position="last",
    )

    # --------------------------------------------------------
    # Save full output (CSV + multi-sheet Excel)
    # --------------------------------------------------------
    result.to_csv(OUTPUT_CSV, index=False)

    passing = result[result["Piotroski_F_Score"] >= MIN_F_SCORE].copy()
    top_by_sector = (
        result.sort_values("Overall_Score", ascending=False)
        .groupby("sector")
        .head(5)
        .sort_values(["sector", "Overall_Score"], ascending=[True, False])
    )
    sector_summary = build_sector_summary(result)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        result.to_excel(writer, sheet_name="All Stocks", index=False)
        passing.to_excel(writer, sheet_name="Piotroski Pass", index=False)
        top_by_sector.to_excel(writer, sheet_name="Top 5 per Sector", index=False)
        sector_summary.to_excel(writer, sheet_name="Sector Summary", index=False)

    # --------------------------------------------------------
    # Display passing stocks
    # --------------------------------------------------------
    print("\n" + "=" * 120)
    print(f"PIOTROSKI SCREEN — SCORE >= {MIN_F_SCORE}")
    print("=" * 120)

    display_columns = [
        "ticker", "name", "sector",
        "Piotroski_F_Score", "Extended_F_Score", "Overall_Score",
        "Quality_Score", "Valuation_Score", "Growth_Score",
        "Momentum_Score", "Financial_Health_Score",
        "Sector_Composite_Score",
        "ROA", "ROE", "ROIC",
        "Net_Margin", "Cash_Conversion",
        "Revenue_Growth_YoY_FY", "Net_Income_Growth_YoY_FY",
        "Debt_to_Equity", "Net_Debt_to_EBITDA",
        "Altman_Z",
        "PE", "PB", "PS", "EV_EBITDA",
        "FCF_Yield", "Dividend_Yield_Fwd", "Payout_Ratio_Pct",
        "Graham_Upside_Pct", "Graham_Margin_of_Safety",
        "Trend_Label", "Trend_Alignment_0to4",
        "RSI", "RSI_Zone", "MACD_Cross",
        "Tech_Rating", "MA_Rating", "Oscillator_Rating",
        "Above_SMA200", "Golden_Cross_50_200",
        "Signal_Count", "Signals",
        "Close", "Market_Cap",
    ]
    display_columns = [c for c in display_columns if c in passing.columns]

    with pd.option_context(
        "display.max_columns", None,
        "display.width", 320,
        "display.max_colwidth", 80,
    ):
        print(passing[display_columns].to_string(index=False))

    # --------------------------------------------------------
    # Sector summary
    # --------------------------------------------------------
    print("\n" + "-" * 120)
    print("SECTOR SUMMARY (top 10 by total market cap)")
    print("-" * 120)
    with pd.option_context(
        "display.max_columns", None,
        "display.width", 320,
    ):
        print(sector_summary.head(10).to_string(index=False))

    # --------------------------------------------------------
    # Top composite (overall)
    # --------------------------------------------------------
    print("\n" + "=" * 120)
    print("TOP 25 BY OVERALL COMPOSITE SCORE")
    print("=" * 120)

    top_overall = result.dropna(subset=["Overall_Score"]).head(25)
    top_cols = [
        "ticker", "name", "sector",
        "Overall_Score", "Quality_Score", "Valuation_Score",
        "Growth_Score", "Momentum_Score", "Financial_Health_Score",
        "Piotroski_F_Score", "PE", "PB", "ROE", "ROIC", "Market_Cap",
    ]
    top_cols = [c for c in top_cols if c in top_overall.columns]
    with pd.option_context(
        "display.max_columns", None,
        "display.width", 320,
        "display.max_colwidth", 60,
    ):
        print(top_overall[top_cols].to_string(index=False))

    # --------------------------------------------------------
    # Top by individual composite category
    # --------------------------------------------------------
    for score_col, title in [
        ("Valuation_Score", "TOP 15 — VALUATION"),
        ("Quality_Score", "TOP 15 — QUALITY"),
        ("Growth_Score", "TOP 15 — GROWTH"),
        ("Momentum_Score", "TOP 15 — MOMENTUM"),
        ("Financial_Health_Score", "TOP 15 — FINANCIAL HEALTH"),
    ]:
        if score_col not in result.columns:
            continue
        print("\n" + "=" * 120)
        print(title)
        print("=" * 120)
        cols = [
            "ticker", "name", "sector", score_col,
            "Piotroski_F_Score", "Overall_Score",
            "PE", "PB", "ROE", "ROIC", "Market_Cap",
        ]
        cols = [c for c in cols if c in result.columns]
        with pd.option_context(
            "display.max_columns", None,
            "display.width", 320,
            "display.max_colwidth", 60,
        ):
            print(
                result.dropna(subset=[score_col])
                .sort_values(by=score_col, ascending=False)
                .head(15)[cols]
                .to_string(index=False)
            )

    # --------------------------------------------------------
    # Signal frequency
    # --------------------------------------------------------
    print("\n" + "=" * 120)
    print("SIGNAL FREQUENCY ACROSS UNIVERSE")
    print("=" * 120)

    counter = Counter()
    for sig in result["Signals"].fillna(""):
        for s in sig.split(" | "):
            if s.strip():
                counter[s.strip()] += 1

    for sig, cnt in counter.most_common(50):
        print(f"{sig:35s} : {cnt}")

    # --------------------------------------------------------
    # Sector breakdown of passing stocks
    # --------------------------------------------------------
    if "sector" in passing.columns and not passing.empty:
        print("\n" + "=" * 120)
        print("PASSING STOCKS — SECTOR BREAKDOWN")
        print("=" * 120)
        sector_counts = passing["sector"].value_counts()
        for sector, cnt in sector_counts.items():
            print(f"{str(sector):35s} : {cnt}")

    # --------------------------------------------------------
    # Final summary
    # --------------------------------------------------------
    print("\n")
    print(f"Total Indian stocks scanned         : {len(result)}")
    print(f"Stocks passing Piotroski >= {MIN_F_SCORE}     : {len(passing)}")
    print(f"Stocks with Overall_Score >= 75     : "
          f"{(result['Overall_Score'] >= 75).sum()}")
    print(f"Stocks with Piotroski F == 9        : "
          f"{(result['Piotroski_F_Score'] == 9).sum()}")
    print(f"Stocks with Altman Safe zone        : "
          f"{(result['Altman_Zone'] == 'Safe').sum()}")
    print(f"Stocks in Full Uptrend (4/4)        : "
          f"{(result['Trend_Alignment_0to4'] == 4).sum()}")
    print(f"Stocks with RSI Oversold            : "
          f"{(result['RSI_Zone'] == 'Oversold').sum()}")
    print(f"Stocks with RSI Overbought          : "
          f"{(result['RSI_Zone'] == 'Overbought').sum()}")

    print(f"\nCSV saved to   : {Path(OUTPUT_CSV).resolve()}")
    print(f"Excel saved to : {Path(OUTPUT_XLSX).resolve()} "
          f"(sheets: All Stocks, Piotroski Pass, Top 5 per Sector, Sector Summary)")
    df_out.to_csv(OUTPUT_CSV, index=False)
    df_out.to_excel(OUTPUT_XLSX, index=False, engine="openpyxl")


if __name__ == "__main__":
    main()
