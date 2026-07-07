# -*- coding: utf-8 -*-
"""估值表数据采集与计算管道.
股票池由 README.md 维护 (parse_readme), 每行 `代码 中文名 [股本]`.
A 股: 新浪源 (stock_financial_abstract / stock_zh_a_daily) + 东方财富个股信息查总股本.
港股: 东方财富财务(英文列) + 网易行情(stock_hk_daily); 股本由 README 手填(无自动接口).
美股: 东方财富财务(英文列) + 网易行情(stock_us_daily); 股本由 README 手填.
市值 = 股价 × 股本, 自然在股价币种; 利润保留原始报告币种, 不额外折算.
PE 内部统一到价格币种计算, 确保准确.
输出: data/valuation.json
"""
import json
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
import threading

import akshare as ak
import pandas as pd
import requests


CN_TZ = timezone(timedelta(hours=8))
AK_TIMEOUT = 30
REQUEST_TIMEOUT = 8
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/",
}
HTTP = requests.Session()
HTTP.headers.update(HTTP_HEADERS)


# ---------- 超时包装: akshare 接口无原生 timeout, 用线程硬卡 ----------
def timed(fn, secs=AK_TIMEOUT):
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
    同一代码重复出现时保留首次 (避免 README 手误重复行导致 results 双行).
    """
    stocks = []
    seen = set()
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
            if code in seen:
                continue  # 去重: 同代码只保留首次
            seen.add(code)
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
    """清洗数值: 去百分号/逗号, 转 float.
    支持中文量词后缀: '1.47亿' -> 147000000, '2.16万' -> 21600 (ths 源净利润列格式).
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("%", "").replace(",", "")
    mult = 1.0
    if s and s[-1] in "亿万":
        unit = s[-1]
        s = s[:-1]
        mult = 1e8 if unit == "亿" else 1e4
    if s in ("False", "True", "--", "", "nan", "NaN"):
        return 0.0
    try:
        return float(s) * mult
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


def with_retry(fn, attempts=2, delay=1.0, secs=AK_TIMEOUT):
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
            if mid != mid:  # nan 检查: nan != nan 为 True, 跳过脏数据保硬编码值
                continue
            if pair == "USD/CNY":
                rates["USD"] = mid
            elif pair == "HKD/CNY":
                rates["HKD"] = mid
    except Exception:
        pass
    _FX = rates
    return rates


# ---------- A 股行情快照: 一次拉全市场, 避免逐只请求被限流 ----------
_A_SPOT = None
_A_SPOT_TRIES = 0
_A_SPOT_MAX_TRIES = 3
def a_spot_map():
    """返回 {code: {price, shares_yi, market_cap_yi}}.
    东方财富 A 股全市场快照含最新价与总市值; 股本 = 总市值 / 最新价。
    失败时返回空 dict, 后续会用逐只接口/缓存兜底。
    关键: 失败不缓存空结果, 允许后续股票重试(单次运行最多 _A_SPOT_MAX_TRIES 次),
    应对瞬时网络中断(RemoteDisconnected 等)——否则一次抖动会让整轮 99 只全走逐只兜底被限流。
    """
    global _A_SPOT, _A_SPOT_TRIES
    if _A_SPOT is not None:
        return _A_SPOT
    if _A_SPOT_TRIES >= _A_SPOT_MAX_TRIES:
        return {}
    _A_SPOT_TRIES += 1
    out = {}
    try:
        df = with_retry(lambda: ak.stock_zh_a_spot_em(), attempts=2, secs=45)
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).zfill(6)
            price = clean_number(row.get("最新价"))
            market_cap_yuan = clean_number(row.get("总市值"))
            if code and price > 0 and market_cap_yuan > 0:
                out[code] = {
                    "price": price,
                    "shares_yi": market_cap_yuan / price / 1e8,
                    "market_cap_yi": market_cap_yuan / 1e8,
                }
        _A_SPOT = out  # 仅成功时缓存, 失败不缓存以便后续股票重试
        if _A_SPOT_TRIES > 1:
            print(f"A股快照第 {_A_SPOT_TRIES} 次尝试恢复成功: {len(out)} 只", flush=True)
    except Exception as e:
        print(f"A股快照不可用(尝试 {_A_SPOT_TRIES}/{_A_SPOT_MAX_TRIES}), 将走逐只/缓存兜底: {type(e).__name__}: {e}", flush=True)
    return out


def get_shares_a(code, fallback=None):
    """A 股总股本(亿股), 多源兜底:
    1) A 股全市场快照 (总市值/最新价, 避免逐只限流)
    2) push2 实时报价 f84
    3) stock_individual_info_em
    4) fallback (README 手填或缓存值)
    """
    spot = a_spot_map().get(code)
    if spot and spot.get("shares_yi", 0) > 0:
        return spot["shares_yi"]
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
def _get_a_financial_sina(code):
    """A 股近 3 年年报归母净利润(亿元)与 ROE(%) - 新浪源 (主源).
    stock_financial_abstract 返回: 行=指标(含重复), 列=日期字符串.
    iloc[0] = 归母净利润, iloc[11] = 净资产收益率(ROE).
    返回 (roes, profits) 或 ([], []) 表示取数失败.
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


def _get_a_financial_em(code):
    """A 股近 3 年年报归母净利润(亿元)与 ROE(%) - 东方财富 datacenter (兜底).
    直接 HTTP 请求 datacenter 接口, 不依赖 akshare wrapper (规避 *_by_yearly_em 的 hidctype bug).
    字段: REPORTDATE / WEIGHTAVG_ROE(加权ROE%) / PARENT_NETPROFIT(归母净利,元).
    SECUCODE: 沪市 6开头.SH, 深市 .SZ.
    返回 (roes, profits) 或 ([], []) 表示取数失败.
    """
    secucode = code + ".SH" if code.startswith("6") else code + ".SZ"
    try:
        r = HTTP.get(
            "https://datacenter.eastmoney.com/securities/api/data/v1/get",
            params={
                "reportName": "RPT_LICO_FN_CPD",
                "columns": "REPORTDATE,WEIGHTAVG_ROE,PARENT_NETPROFIT",
                "filter": '(SECUCODE="%s")' % secucode,
                "pageNumber": 1, "pageSize": 50,
                "sortColumns": "REPORTDATE", "sortTypes": "-1",
            },
            timeout=REQUEST_TIMEOUT,
        )
        rows = (r.json().get("result") or {}).get("data") or []
    except Exception:
        return [], []
    # 筛年报(12-31), 已按 REPORTDATE 降序, 取最近 3 年
    annual = [x for x in rows if str(x.get("REPORTDATE", "")).endswith("12-31")][:3]
    if len(annual) < 3:
        return [], []
    roes = [clean_number(x.get("WEIGHTAVG_ROE")) for x in annual]
    profits = [clean_number(x.get("PARENT_NETPROFIT")) / 1e8 for x in annual]  # 元 -> 亿元
    return roes, profits


def get_a_financial(code):
    """A 股近 3 年年报净利润(亿元)与 ROE(%), 双源: 新浪 -> 东方财富兜底."""
    try:
        roes, profits = _get_a_financial_sina(code)
        if len(roes) >= 3 and len(profits) >= 3:
            return roes, profits
    except Exception as e:
        print(f"  新浪财务失败({type(e).__name__}), 转东方财富兜底", flush=True)
    try:
        roes, profits = _get_a_financial_em(code)
        if len(roes) >= 3 and len(profits) >= 3:
            print(f"  东方财富兜底成功", flush=True)
            return roes, profits
    except Exception as e:
        print(f"  东方财富兜底也失败({type(e).__name__})", flush=True)
    return [], []


def get_a_price(code):
    """A 股最新价: 优先东方财富实时报价, 失败回退新浪日报昨收."""
    spot = a_spot_map().get(code)
    if spot and spot.get("price", 0) > 0:
        return spot["price"]
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


def _em_quote(secid, fields="f43", timeout=REQUEST_TIMEOUT):
    """东方财富 push2 单只报价, 返回 {字段: 值} dict 或 {}.
    fields: f43=最新价, f84=总股本, f58=名称, f85=流通股 ...
    """
    try:
        r = HTTP.get(
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
FIN_CACHE_PATH = "data/financials.json"
FIN_REFRESH_DAYS = 30  # 财务缓存超过 30 天才重新拉新鲜, 否则直接用缓存


def _parse_iso_dt(s):
    """解析 ISO 格式时间字符串, 失败返回 None."""
    try:
        return datetime.fromisoformat(str(s))
    except Exception:
        return None


def load_financials(path=FIN_CACHE_PATH):
    """读取财务缓存 {code: {roes, profits, ccy, fetched_at}}."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_financials(cache, path=FIN_CACHE_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _fetch_fresh_financial(code, market):
    """拉新鲜财务, 返回 dict {roes, profits, ccy} 或 None.
    三市场统一入口, 失败返回 None (不抛异常, 由调用方决定兜底).
    """
    try:
        if market == "A":
            roes, profits = get_a_financial(code)
            ccy = "CNY"
        elif market == "HK":
            roes, profits = get_hk_financial(code)
            ccy = "CNY"
        elif market == "US":
            roes, profits, ccy = get_us_financial(code)
        else:
            return None
        if len(roes) >= 3 and len(profits) >= 3:
            return {"roes": roes, "profits": profits, "ccy": ccy}
    except Exception as e:
        print(f"  财务拉取异常: {type(e).__name__}: {e}", flush=True)
    return None


def process_one(stock, prev=None, fin_cache=None, refresh_days=FIN_REFRESH_DAYS):
    """处理单只股票, 返回 (record, fin_fresh_or_cached) 或 (None, None).
    财务: 30 天内复用缓存, 超期或无缓存才拉新鲜; 拉不到用缓存兜底; 缓存也无 -> 裸奔失败.
    价格: 总是拉新鲜, 失败用 prev["股价"] 兜底.
    fin_cache: {code: {roes, profits, ccy, fetched_at}}, 由 main 传入并统一持久化.
    返回 fin 用于 main 决定是否更新缓存(新鲜成功才更新).
    """
    code, market, name, shares_yi = stock
    fin_cache = fin_cache if fin_cache is not None else {}
    cached = fin_cache.get(code)
    now = datetime.now(CN_TZ)

    # 决定是否需要拉新鲜财务: 无缓存, 或缓存超 refresh_days
    need_fresh = True
    if cached:
        fetched_at = _parse_iso_dt(cached.get("fetched_at"))
        if fetched_at and (now - fetched_at).days < refresh_days:
            need_fresh = False

    fin = None
    fin_is_fresh = False
    if need_fresh:
        fin = _fetch_fresh_financial(code, market)
        if fin:
            fin_is_fresh = True
        elif cached:
            print(f"  财务拉取失败, 用缓存兜底( fetched_at={cached.get('fetched_at')})", flush=True)
            fin = cached
    else:
        fin = cached

    # 财务彻底没有 (新股首跑失败, 且无缓存) -> 裸奔, 本次缺席, 下次再试
    if not fin or len(fin.get("roes", [])) < 3 or len(fin.get("profits", [])) < 3:
        return None, None

    roes = fin["roes"]
    profits = fin["profits"]
    profit_ccy = fin.get("ccy", "CNY")

    # 价格: 总是拉新鲜
    price = 0.0
    try:
        if market == "A":
            price = get_a_price(code)
        elif market == "HK":
            price = get_hk_price(code, name)
        elif market == "US":
            price = get_us_price(code, name)
    except Exception as e:
        print(f"  价格拉取异常: {type(e).__name__}: {e}", flush=True)

    # 价格拿不到 -> 用旧价格保底
    if price <= 0 and prev and prev.get("股价", 0) > 0:
        print(f"  价格缺失, 用上次股价 {prev['股价']} 保底", flush=True)
        price = prev["股价"]
    # 价格仍拿不到 -> 用上次整条记录保底(财务已是缓存, 极少走到这)
    if price <= 0 and prev:
        print(f"  价格仍缺失, 用上次整条记录保底", flush=True)
        return prev, None
    if price <= 0:
        return None, None

    rates = fx_rates()
    price_ccy = PRICE_CCY[market]
    # 利润折算到价格币种, 用于算 PE (市值与利润同币种)
    fx_profit_to_price = rates.get(profit_ccy, 1.0) / rates[price_ccy]

    roe1, roe2, roe3 = roes[0], roes[1], roes[2]
    # 利润显示: 保留原始报告币种 (来源是什么就写什么)
    p1 = profits[0]
    p2 = profits[1]
    p3 = profits[2]
    mean_roe = round((roe1 + roe2 + roe3) / 3, 2)
    mean_profit = round((p1 + p2 + p3) / 3, 2)
    # 市值: 股价 × 股本, 自然在股价币种, 不额外折算
    market_cap = round(price * shares_yi, 2)
    # PE: 市值与利润统一到价格币种再算
    mean_profit_in_price = mean_profit * fx_profit_to_price
    mean_pe = round(market_cap / mean_profit_in_price, 2) if mean_profit_in_price != 0 else 0
    valuation_ratio = round(mean_pe / mean_roe, 2) if mean_roe != 0 else 0

    rec = {
        "证券简称": name,
        "代码": code,
        "市场": market,
        "股价": round(price, 2),
        "股本": round(shares_yi, 2),
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
    # 返回的 fin 仅含数据字段(无 fetched_at), 由 main 补 fetched_at 后写缓存
    return rec, ({"roes": roes, "profits": profits, "ccy": profit_ccy} if fin_is_fresh else None)


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
    fin_cache = load_financials()

    # 新股优先: 无 prev 记录(从未成功输出过)的股票排最前, 趁 API 新鲜时先跑(限流随运行累积).
    # 有 prev 的老股票放后面, 即使被限流也有 prev/缓存兜底.
    new_codes = {s[0] for s in stocks if s[0] not in prev_code}
    ordered = [s for s in stocks if s[0] in new_codes] + [s for s in stocks if s[0] not in new_codes]
    print(f"股票池 {len(stocks)} 只: 新股(无prev) {len(new_codes)} 只优先, 老股 {len(stocks)-len(new_codes)} 只", flush=True)

    results = []
    failed = []
    for i, (code, market, name, shares_readme) in enumerate(ordered, 1):
        tag = "新" if code in new_codes else "老"
        print(f"[{i}/{len(ordered)}] [{tag}] {name} ({market}:{code}) ...", flush=True)
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
            rec, fin_fresh = process_one((code, market, name, shares_yi), prev, fin_cache)
            # 即使最终 rec=None(股本/价格失败), 已拿到的新鲜财务也写入缓存, 避免下次重拉
            if fin_fresh:
                fin_fresh["fetched_at"] = datetime.now(CN_TZ).isoformat()
                fin_cache[code] = fin_fresh
            if rec is None:
                # 财务与价格都拿不到, 且无 prev -> 真正失败, 下次再试
                print(f"  SKIP: 财务/价格均缺失且无历史数据, 下次再试", flush=True)
                failed.append(name)
                continue
            results.append(rec)
            if fin_fresh:
                print(f"  OK  PE={rec['均值PE']} 估值比={rec['估值比']} 均值ROE={rec['均值ROE']}% (财务已更新缓存)", flush=True)
            else:
                src = "缓存" if fin_cache.get(code) else "prev"
                print(f"  OK  PE={rec['均值PE']} 估值比={rec['估值比']} 均值ROE={rec['均值ROE']}% (财务用{src})", flush=True)
            cache[code] = shares_yi  # 更新股本缓存
        except Exception as e:
            print(f"  FAIL: {type(e).__name__}: {e}", flush=True)
            failed.append(name)
            traceback.print_exc()
        time.sleep(0.4)  # 轻微限速, 避免被封

    save_cache(cache)
    save_financials(fin_cache)  # 财务缓存持久化, 下次运行/新股下次就能兜底
    output = {
        "update_time": datetime.now(CN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "total": len(stocks),
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
