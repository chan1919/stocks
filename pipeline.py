# -*- coding: utf-8 -*-
"""估值表数据采集与计算管道.
股票池由 README.md 维护 (parse_readme), 每行 `代码 中文名 [股本]`.
A 股: 新浪源 (stock_financial_abstract / stock_zh_a_daily) + 东方财富个股信息查总股本.
港股: 东方财富财务(英文列) + 网易行情(stock_hk_daily); 股本由 README 手填(无自动接口).
美股: 东方财富财务(英文列) + 网易行情(stock_us_daily); 股本由 README 手填.
跨市场币种: 港股价 HKD、美股股价 USD; 财务利润按报告币种(中概股多为 CNY).
市值与利润统一按 fx_spot_quote 当日汇率折算为人民币(亿), 保证 PE 可比.
股本缓存 data/shares_cache.json 为 A 股自动查兜底.
输出: data/valuation.json
"""
import json
import os
import time
import traceback
from datetime import datetime
import threading

import akshare as ak
import pandas as pd
import requests


# ---------- 超时包装: akshare 接口无原生 timeout, 用线程硬卡 ----------
def timed(fn, secs=60):
    """运行 fn, 最多 secs 秒, 超时抛 TimeoutError. 防接口卡死拖垮整个 pipeline."""
    box = {}
    def run():
        try:
            box["ok"] = fn()
        except Exception as e:
            box["err"] = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(secs)
    if t.is_alive():
        raise TimeoutError(f">{secs}s")
    if "err" in box:
        raise box["err"]
    return box.get("ok")


# ---------- 股票池解析 (README 驱动) ----------
README_PATH = "README.md"
CACHE_PATH = "data/shares_cache.json"

# 分组标题 -> 市场代码
SECTION_MARKET = {"a股": "A", "港股": "HK", "美股": "US"}


def parse_readme(path=README_PATH):
    """解析 README.md 的股票池, 返回 [(code, market, name, shares_opt), ...].
    每行格式: `代码 中文名 [股本]`, 股本可省略(亿股). 按 ## 分组识别市场.
    """
    stocks = []
    market = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            if s.startswith("##"):
                head = s.lstrip("#").strip().lower()
                market = SECTION_MARKET.get(head)
                continue
            if market is None:
                continue  # 说明区(标题/表格/列表/分割线)跳过
            s = s.lstrip("-*").strip()  # 去除列表前缀
            parts = s.split()
            if len(parts) < 2:
                continue
            code, name = parts[0], parts[1]
            shares = None
            if len(parts) >= 3:
                try:
                    shares = float(parts[2])
                except ValueError:
                    shares = None
            stocks.append((code, market, name, shares))
    return stocks


def load_cache(path=CACHE_PATH):
    """读取股本缓存 {code: shares_yi}."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(cache, path=CACHE_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


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


def with_retry(fn, attempts=3, delay=1.5, secs=60):
    """重试 + 超时, 应对接口偶发超时/卡死. secs 控制单次最长耗时."""
    last = None
    for _ in range(attempts):
        try:
            return timed(fn, secs)
        except Exception as e:
            last = e
            time.sleep(delay)
    raise last


# 汇率缓存: 各货币 -> 人民币 (CNY)
_FX = None
def fx_rates():
    """返回 {货币: 折算CNY汇率}, 失败时用近似硬编码值兜底."""
    global _FX
    if _FX is not None:
        return _FX
    rates = {"CNY": 1.0, "USD": 7.10, "HKD": 0.87}
    try:
        df = with_retry(lambda: ak.fx_spot_quote())
        for row in df.itertuples(index=False):
            pair, bid, ask = str(row[0]), float(row[1]), float(row[2])
            mid = (bid + ask) / 2.0
            if pair == "USD/CNY":
                rates["USD"] = mid
            elif pair == "HKD/CNY":
                rates["HKD"] = mid
    except Exception:
        pass
    _FX = rates
    return rates


def get_shares_a(code, fallback=None):
    """A 股总股本(亿股), 多源兜底:
    1) push2 实时报价 f84 (与价格同 host, GitHub 上稳定)
    2) stock_individual_info_em (东方财富个股页, 备用)
    3) fallback (README 手填或缓存值)
    """
    secid = _a_secid(code)
    s = _em_quote_shares(secid)
    if s and s > 0:
        return s
    try:
        df = with_retry(lambda: ak.stock_individual_info_em(symbol=code))
        row = df[df["item"] == "总股本"]
        if not row.empty:
            v = clean_number(row["value"].iloc[0]) / 1e8  # 股 -> 亿股
            if v > 0:
                return v
    except Exception:
        pass
    return fallback


# ---------- A 股 ----------
def get_a_financial(code):
    """A 股近 3 年年报归母净利润(亿元)与 ROE(%).
    stock_financial_abstract 返回: 行=指标(含重复), 列=日期字符串.
    iloc[0] = 归母净利润, iloc[11] = 净资产收益率(ROE).
    """
    df = with_retry(lambda: ak.stock_financial_abstract(symbol=code))
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
    """A 股最新价: 优先东方财富实时报价, 失败回退新浪日报昨收."""
    p = _em_quote_price(_a_secid(code))
    if p:
        return p
    prefix = "sh" if code.startswith("6") else "sz"
    df = with_retry(lambda: ak.stock_zh_a_daily(symbol=prefix + code, adjust=""))
    return float(df["close"].iloc[-1])


# ---------- 实时行情/股本 (东方财富 push2, 轻量带超时, GitHub 可用) ----------
def _a_secid(code):
    """A 股 secid: 沪市 1.代码, 深市 0.代码."""
    return f"1.{code}" if code.startswith("6") else f"0.{code}"


def _em_quote(secid, fields="f43", timeout=8):
    """东方财富 push2 单只报价, 返回 {字段: 值} dict 或 {}.
    fields: f43=最新价, f84=总股本, f58=名称, f85=流通股 ...
    """
    try:
        r = requests.get(
            "https://push2.eastmoney.com/api/qt/stock/get",
            params={"secid": secid, "fields": fields, "fltt": "2"},
            timeout=timeout,
        )
        d = r.json().get("data") or {}
        return d
    except Exception:
        return {}


def _em_quote_price(secid, timeout=8):
    """东方财富 push2 单只实时报价, 返回最新价或 None."""
    d = _em_quote(secid, fields="f43", timeout=timeout)
    p = clean_number(d.get("f43"))
    return p if p > 0 else None


def _em_quote_shares(secid, timeout=8):
    """东方财富 push2 查总股本, 返回亿股或 None."""
    d = _em_quote(secid, fields="f84", timeout=timeout)
    s = clean_number(d.get("f84"))
    return s / 1e8 if s > 0 else None


def _us_secid_candidates(code):
    """美股 secid 候选: 105 NASDAQ, 106 NYSE, 107 AMEX."""
    return [f"105.{code}", f"106.{code}", f"107.{code}"]


# ---------- 港股 ----------
def get_hk_price(code, name=None):
    """港股最新价: 优先东方财富实时报价, 失败回退网易日报昨收."""
    p = _em_quote_price(f"116.{code}")
    if p:
        return p
    df = with_retry(lambda: ak.stock_hk_daily(symbol=code, adjust=""))
    return float(df["close"].iloc[-1])


def get_hk_financial(code):
    """港股近 3 年年报归母净利润(亿元)与 ROE(%).
    东方财富接口返回英文列: REPORT_DATE / HOLDER_PROFIT / ROE_AVG.
    """
    df = with_retry(lambda: ak.stock_financial_hk_analysis_indicator_em(symbol=code, indicator="年度"))
    date_col, profit_col, roe_col = "REPORT_DATE", "HOLDER_PROFIT", "ROE_AVG"
    if not all(c in df.columns for c in (date_col, profit_col, roe_col)):
        return [], []
    df = df.sort_values(date_col, ascending=False).head(3)
    profits = [clean_number(v) / 1e8 for v in df[profit_col].values]  # 元 -> 亿元
    roes = [clean_number(v) for v in df[roe_col].values]
    return roes, profits


# ---------- 美股 ----------
def get_us_price(code, name=None):
    """美股最新价: 优先东方财富实时报价(尝试 NASDAQ/NYSE/AMEX), 失败回退网易日报昨收."""
    for secid in _us_secid_candidates(code):
        p = _em_quote_price(secid)
        if p:
            return p
    df = with_retry(lambda: ak.stock_us_daily(symbol=code, adjust=""))
    return float(df["close"].iloc[-1])


def get_us_financial(code):
    """美股近 3 年年报归母净利润(亿元)与 ROE(%), 并返回报告币种.
    东方财富接口返回英文列: REPORT_DATE / PARENT_HOLDER_NETPROFIT / ROE_AVG / CURRENCY_ABBR.
    中概股(如 PDD)报告币种为 CNY, 美股本土(如 AAPL)为 USD.
    """
    df = with_retry(lambda: ak.stock_financial_us_analysis_indicator_em(symbol=code))
    date_col, profit_col, roe_col, ccy_col = "REPORT_DATE", "PARENT_HOLDER_NETPROFIT", "ROE_AVG", "CURRENCY_ABBR"
    if not all(c in df.columns for c in (date_col, profit_col, roe_col)):
        return [], [], "USD"
    df = df.sort_values(date_col, ascending=False).head(3)
    profits = [clean_number(v) / 1e8 for v in df[profit_col].values]  # 元 -> 亿元(报告币种)
    roes = [clean_number(v) for v in df[roe_col].values]
    ccy = str(df[ccy_col].iloc[0]).strip().upper() if ccy_col in df.columns and len(df) else "USD"
    return roes, profits, ccy


# ---------- 主流程 ----------
PRICE_CCY = {"A": "CNY", "HK": "HKD", "US": "USD"}

def process_one(stock, prev=None):
    code, market, name, shares_yi = stock
    roes, profits, price = [], [], 0.0
    profit_ccy = "CNY"

    try:
        if market == "A":
            roes, profits = get_a_financial(code)
            price = get_a_price(code)
        elif market == "HK":
            roes, profits = get_hk_financial(code)
            price = get_hk_price(code, name)
        elif market == "US":
            roes, profits, profit_ccy = get_us_financial(code)
            price = get_us_price(code, name)
    except Exception as e:
        # 网络全挂时回退旧记录保底(宁可旧数据也不丢股票)
        if prev:
            print(f"  NET FAIL, 用上次数据保底: {type(e).__name__}", flush=True)
            return prev
        raise

    # 价格拿不到 -> 用旧价格保底
    if price <= 0 and prev and prev.get("股价", 0) > 0:
        print(f"  价格缺失, 用上次股价 {prev['股价']} 保底", flush=True)
        price = prev["股价"]
    # 财务拿不到 -> 用旧 ROE/利润保底
    if (len(roes) < 3 or len(profits) < 3) and prev and prev.get("均值ROE", 0) > 0:
        print(f"  财务缺失, 用上次财务保底", flush=True)
        return prev

    if len(roes) < 3 or len(profits) < 3 or price <= 0:
        return None

    rates = fx_rates()
    fx_p = rates[PRICE_CCY[market]]   # 价格币种 -> CNY
    fx_r = rates.get(profit_ccy, 1.0) # 利润币种 -> CNY

    roe1, roe2, roe3 = roes[0], roes[1], roes[2]
    p1 = profits[0] * fx_r
    p2 = profits[1] * fx_r
    p3 = profits[2] * fx_r
    mean_roe = round((roe1 + roe2 + roe3) / 3, 2)
    mean_profit = round((p1 + p2 + p3) / 3, 2)
    market_cap = round(price * shares_yi * fx_p, 2)  # 市值(亿人民币)
    mean_pe = round(market_cap / mean_profit, 2) if mean_profit != 0 else 0
    valuation_ratio = round(mean_pe / mean_roe, 2) if mean_roe != 0 else 0

    return {
        "证券简称": name,
        "代码": code,
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


def load_prev(path="data/valuation.json"):
    """读取上次 valuation.json, 返回 (by_code, by_name) 两个 dict 供保底."""
    if not os.path.exists(path):
        return {}, {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f).get("data") or []
        by_code = {r["代码"]: r for r in data if "代码" in r}
        by_name = {r["证券简称"]: r for r in data}
        return by_code, by_name
    except Exception:
        return {}, {}


def main():
    stocks = parse_readme()
    if not stocks:
        print("README.md 未解析到任何股票, 请检查股票池格式.", flush=True)
        return
    cache = load_cache()
    prev_code, prev_name = load_prev()
    results = []
    failed = []
    for i, (code, market, name, shares_readme) in enumerate(stocks, 1):
        print(f"[{i}/{len(stocks)}] {name} ({market}:{code}) ...", flush=True)
        try:
            # 确定股本(亿股): A 股自动查 -> README 手填 -> 缓存 -> 跳过
            if market == "A":
                fallback = shares_readme if shares_readme else cache.get(code)
                shares_yi = get_shares_a(code, fallback=fallback)
            else:
                shares_yi = shares_readme if shares_readme else cache.get(code)
            if not shares_yi or shares_yi <= 0:
                print(f"  SKIP: 缺少股本(A股自动查失败且无缓存/手填值)", flush=True)
                failed.append(name)
                continue

            prev = prev_code.get(code) or prev_name.get(name)
            rec = process_one((code, market, name, shares_yi), prev)
            if rec is None:
                # 最后兜底: 直接用上次记录
                if prev:
                    print(f"  SKIP: 数据不足, 用上次数据保底", flush=True)
                    results.append(prev)
                else:
                    print(f"  SKIP: 数据不足且无历史数据", flush=True)
                    failed.append(name)
                continue
            results.append(rec)
            cache[code] = shares_yi  # 更新缓存
            print(f"  OK  PE={rec['均值PE']} 估值比={rec['估值比']} 均值ROE={rec['均值ROE']}%", flush=True)
        except Exception as e:
            print(f"  FAIL: {type(e).__name__}: {e}", flush=True)
            failed.append(name)
            traceback.print_exc()
        time.sleep(0.4)  # 轻微限速, 避免被封

    save_cache(cache)
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
