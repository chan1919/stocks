# -*- coding: utf-8 -*-
"""估值表数据采集与计算管道.
A 股: 新浪源 (stock_financial_abstract / stock_zh_a_daily) - 本地可用, 已验证.
港股/美股: 东方财富源 - 本地可能被封, GitHub Actions 上正常.
输出: data/valuation.json
"""
import json
import time
import traceback
from datetime import datetime

import akshare as ak
import pandas as pd

from config import STOCKS


# ---------- 工具函数 ----------
def clean_number(val):
    """清洗数值: 去百分号/逗号, 转 float."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("%", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def find_first_row_index(index_list, names):
    """在指标索引列表中, 按候选名称顺序找第一次出现的位置."""
    for name in names:
        for i, x in enumerate(index_list):
            if x == name:
                return i
    return None


def pick_annual_cols(cols, n=3):
    """从列名中挑出年报(以 12-31 / 1231 结尾), 降序取最近 n 个."""
    annual = [c for c in cols if str(c).replace("-", "").endswith("1231")]
    annual = sorted(annual, reverse=True)[:n]
    return annual


# ---------- A 股 ----------
def get_a_financial(code):
    """A 股近 3 年年报归母净利润(亿元)与 ROE(%).
    stock_financial_abstract 返回: 行=指标(含重复), 列=日期字符串.
    iloc[0] = 归母净利润, iloc[11] = 净资产收益率(ROE).
    """
    df = ak.stock_financial_abstract(symbol=code)
    idx_list = df["指标"].tolist()
    r_gm = find_first_row_index(idx_list, ["归母净利润"])
    r_roe = find_first_row_index(idx_list, ["净资产收益率(ROE)", "净资产收益率"])
    if r_gm is None or r_roe is None:
        return [], []
    annual_cols = pick_annual_cols([c for c in df.columns if c != "指标"], 3)
    if len(annual_cols) < 3:
        return [], []
    profits = [clean_number(df.iloc[r_gm][c]) / 1e8 for c in annual_cols]  # 元 -> 亿元
    roes = [clean_number(df.iloc[r_roe][c]) for c in annual_cols]
    return roes, profits


def get_a_price(code):
    """A 股最新真实收盘价(不复权)."""
    prefix = "sh" if code.startswith("6") else "sz"
    df = ak.stock_zh_a_daily(symbol=prefix + code, adjust="")
    return float(df["close"].iloc[-1])


# ---------- 港股 ----------
def get_hk_price(code):
    df = ak.stock_hk_hist(symbol=code, period="daily", adjust="")
    close_col = "收盘" if "收盘" in df.columns else "close"
    return float(df[close_col].iloc[-1])


def get_hk_financial(code):
    """港股近 3 年年报归母净利润与 ROE. 列名自适应."""
    df = ak.stock_financial_hk_analysis_indicator_em(symbol=code, indicator="年度")
    # 自适应: 找日期列与指标列
    # 可能结构: 行=年份, 列=指标名
    profit_cols = ["归母净利润", "归属母公司净利润", "净利润", "归属母公司股东净利润"]
    roe_cols = ["净资产收益率", "净资产收益率(ROE)", "ROE", "加权净资产收益率"]
    date_col = None
    for c in df.columns:
        if any(k in str(c) for k in ["日期", "报告期", "年份", "date", "year", "DATE", "YEAR"]):
            date_col = c
            break
    if date_col is None and df.index.name and any(k in str(df.index.name) for k in ["日期", "报告期", "年份", "date", "year"]):
        df = df.reset_index()
        date_col = df.columns[0]
    if date_col is None:
        # 退而求其次: 取 object 类型第一列
        for c in df.columns:
            if df[c].dtype == object:
                date_col = c
                break
    if date_col is None:
        return [], []
    df = df.sort_values(date_col, ascending=False).head(3)
    pcol = next((c for c in profit_cols if c in df.columns), None)
    rcol = next((c for c in roe_cols if c in df.columns), None)
    if pcol is None or rcol is None:
        return [], []
    profits = [clean_number(v) / 1e8 for v in df[pcol].values]  # 假设单位为元
    roes = [clean_number(v) for v in df[rcol].values]
    return roes, profits


# ---------- 美股 ----------
def get_us_price(code):
    sym = "105." + code  # 105 = NASDAQ 前缀; AAPL/NVDA/PDD 均在 NASDAQ
    df = ak.stock_us_hist(symbol=sym, period="daily", adjust="")
    close_col = "收盘" if "收盘" in df.columns else "close"
    return float(df[close_col].iloc[-1])


def get_us_financial(code):
    """美股近 3 年年报归母净利润与 ROE. 列名自适应."""
    df = ak.stock_financial_us_analysis_indicator_em(symbol=code)
    profit_cols = ["归母净利润", "归属母公司净利润", "净利润", "归属母公司股东净利润"]
    roe_cols = ["净资产收益率", "净资产收益率(ROE)", "ROE", "加权净资产收益率"]
    date_col = None
    for c in df.columns:
        if any(k in str(c) for k in ["日期", "报告期", "年份", "date", "year", "DATE", "YEAR"]):
            date_col = c
            break
    if date_col is None:
        for c in df.columns:
            if df[c].dtype == object:
                date_col = c
                break
    if date_col is None:
        return [], []
    df = df.sort_values(date_col, ascending=False).head(3)
    pcol = next((c for c in profit_cols if c in df.columns), None)
    rcol = next((c for c in roe_cols if c in df.columns), None)
    if pcol is None or rcol is None:
        return [], []
    profits = [clean_number(v) / 1e8 for v in df[pcol].values]
    roes = [clean_number(v) for v in df[rcol].values]
    return roes, profits


# ---------- 主流程 ----------
def process_one(stock):
    code, market, name, shares_yi = stock
    roes, profits, price = [], [], 0.0

    if market == "A":
        roes, profits = get_a_financial(code)
        price = get_a_price(code)
    elif market == "HK":
        roes, profits = get_hk_financial(code)
        price = get_hk_price(code)
    elif market == "US":
        roes, profits = get_us_financial(code)
        price = get_us_price(code)

    if len(roes) < 3 or len(profits) < 3 or price <= 0:
        return None

    roe1, roe2, roe3 = roes[0], roes[1], roes[2]
    p1, p2, p3 = profits[0], profits[1], profits[2]
    mean_roe = round((roe1 + roe2 + roe3) / 3, 2)
    mean_profit = round((p1 + p2 + p3) / 3, 2)
    market_cap = round(price * shares_yi, 2)  # 价格 × 总股本(亿股) = 市值(亿元)
    mean_pe = round(market_cap / mean_profit, 2) if mean_profit != 0 else 0
    valuation_ratio = round(mean_pe / mean_roe, 2) if mean_roe != 0 else 0

    return {
        "证券简称": name,
        "市场": market,
        "股价": round(price, 2),
        "股本": shares_yi,
        "市值": market_cap,
        "ROE": round(roe1, 2),
        "ROE2": round(roe2, 2),
        "ROE3": round(roe3, 2),
        "均值ROE": mean_roe,
        "利润": round(p1, 2),
        "利润2": round(p2, 2),
        "利润3": round(p3, 2),
        "均值利润": mean_profit,
        "均值PE": mean_pe,
        "估值比": valuation_ratio,
    }


def main():
    results = []
    failed = []
    for i, stock in enumerate(STOCKS, 1):
        code, market, name, _ = stock
        print(f"[{i}/{len(STOCKS)}] {name} ({market}:{code}) ...", flush=True)
        try:
            rec = process_one(stock)
            if rec is None:
                print(f"  SKIP: 数据不足", flush=True)
                failed.append(name)
                continue
            results.append(rec)
            print(f"  OK  PE={rec['均值PE']} 估值比={rec['估值比']} 均值ROE={rec['均值ROE']}%", flush=True)
        except Exception as e:
            print(f"  FAIL: {type(e).__name__}: {e}", flush=True)
            failed.append(name)
            traceback.print_exc()
        time.sleep(0.4)  # 轻微限速, 避免被封

    output = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(results),
        "failed": failed,
        "data": results,
    }
    with open("data/valuation.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nDONE: 成功 {len(results)} / 失败 {len(failed)}", flush=True)
    if failed:
        print("失败列表:", ", ".join(failed), flush=True)


if __name__ == "__main__":
    main()
