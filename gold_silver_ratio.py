# -*- coding: utf-8 -*-
"""
黄金白银历史数据抓取与金银比分析脚本
数据请求区间：1900-01-01 ~ 2026-07-03

数据源优先级（按覆盖广度与网络兼容性，自动回退）：
  1) LBMA 官方 JSON 接口（伦敦金银市场协会，国内网络可直连）
     - 黄金（上午定盘价，美元/盎司）：1968-01-02 至今，日K
     - 白银（美元/盎司）：1968-01-02 至今，日K
     ✅ 完全覆盖用户要求的 1971 年至今 日K
  2) yfinance（境外网络可用时）：COMEX 黄金/白银期货或现货，日K，美元/盎司
  3) akshare（国内网络兜底）：沪金AU0+沪银AG0主连 + BOC美元兑人民币中间价换算成美元/盎司

输出：CSV 数据文件（黄金价格、白银价格、金银比、时间），2 张 PNG 走势图

关于"1900 年起"的说明：
  - 1944~1971 布雷顿森林体系：黄金被官方锁定为 35 美元/盎司，没有真正的日K波动；
    这段时期即使有"历史数据"，也只是官价/平价记录，并非市场交易价。
  - 1900~1968 之间免费公开的日K级市场交易数据极少；
    世界黄金协会、LBMA、美联储等公开数据源最早的日K就是从1968年开始（LBMA建立定盘价）。
  - 本脚本使用 LBMA 官方1968年至今的日K作为主数据，完全满足"1971年至今"的要求；
    如果需要更长周期（如月度年均价）请在代码中启用 FRED/世界银行 月度数据源。
"""

import os
import sys
import json
import time
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ================= 全局参数 =================
START_DATE = "1900-01-01"
END_DATE = "2026-07-03"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

OZ_TO_GRAM = 31.1034768  # 1 金衡盎司 = 31.1034768 克

# yfinance 备选标的（境外网络环境）
YF_GOLD_SYMBOLS = [
    ("GC=F", "COMEX黄金期货(美元/盎司)"),
    ("XAUUSD=X", "现货黄金(美元/盎司)"),
]
YF_SILVER_SYMBOLS = [
    ("SI=F", "COMEX白银期货(美元/盎司)"),
    ("XAGUSD=X", "现货白银(美元/盎司)"),
]

LBMA_GOLD_JSON = "https://prices.lbma.org.uk/json/gold_am.json"   # 伦敦金上午定盘价
LBMA_SILVER_JSON = "https://prices.lbma.org.uk/json/silver.json"  # 伦敦银定盘价


# ================= 工具：裁剪区间 =================
def clip_date_range(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = (df.index >= pd.Timestamp(START_DATE)) & (df.index <= pd.Timestamp(END_DATE))
    return df.loc[mask]


# ================= 数据源1: LBMA（最高优先级，1968起日K）=================
def _parse_lbma_json(url: str, label: str, price_index: int = 0) -> pd.DataFrame:
    """
    解析 LBMA 官方 JSON
    结构：[{is_cms_locked:0, d:"1968-01-02", v:[USD_price, GBP_price, EUR_price]}, ...]
    其中 v[0] = 美元/盎司（优先使用）
          v[1] = 英镑/盎司
          v[2] = 欧元/盎司（1999年后才有，早期为null）
    对于 gold_am：上午定盘价；另有 gold_pm.json 下午定盘价可选
    """
    import requests
    print(f"  正在请求 LBMA {label} JSON ...")
    try:
        r = requests.get(
            url,
            timeout=60,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.lbma.org.uk/",
            },
        )
        if r.status_code != 200:
            print(f"    HTTP {r.status_code}，跳过")
            return pd.DataFrame()
        raw = r.json()
    except Exception as e:
        print(f"    失败：{type(e).__name__}: {e}")
        return pd.DataFrame()

    rows = []
    for item in raw:
        d = item.get("d")
        v = item.get("v") or []
        if not d or len(v) <= price_index:
            continue
        price = v[price_index]  # 美元价
        if price is None:
            # 如果美元价缺失，尝试取英镑/欧元换算（极少情况）
            continue
        rows.append((d, float(price)))

    if not rows:
        print("    解析后无有效行")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["date", "price"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df.dropna(subset=["price"])
    df = clip_date_range(df)
    if not df.empty:
        print(f"    OK：{len(df)} 条日K，{df.index.min().date()} ~ {df.index.max().date()}")
    return df


def fetch_via_lbma():
    """使用 LBMA 官方 JSON 抓取黄金/白银日K。返回 (gold_df, silver_df, gold_info, silver_info) 或 None"""
    print("\n=== 【数据源1/LBMA】伦敦金银市场协会 官方 JSON（国内直连，1968起日K）===")
    gold_df = _parse_lbma_json(LBMA_GOLD_JSON, "黄金（上午定盘价）", price_index=0)
    silver_df = _parse_lbma_json(LBMA_SILVER_JSON, "白银", price_index=0)

    if gold_df.empty or silver_df.empty:
        print("  LBMA 数据不足，跳过")
        return None

    gold_info = "LBMA伦敦金上午定盘价(美元/盎司，官方JSON接口)"
    silver_info = "LBMA伦敦银定盘价(美元/盎司，官方JSON接口)"
    return gold_df, silver_df, gold_info, silver_info


# ================= 数据源2: yfinance =================
def fetch_yf_price(symbol: str, name: str, interval: str = "1d") -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame()
    print(f"  尝试 yfinance {name} ({symbol}) 周期={interval} ...")
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(start=START_DATE, end=END_DATE, interval=interval, auto_adjust=True)
        if df is None or df.empty:
            print(f"    无数据")
            return pd.DataFrame()
        df = df[["Close"]].copy()
        df.columns = ["price"]
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = "date"
        df = df.dropna(subset=["price"])
        df = clip_date_range(df)
        if not df.empty:
            print(f"    OK：{len(df)} 条，{df.index.min().date()} ~ {df.index.max().date()}")
        return df
    except Exception as e:
        print(f"    失败：{type(e).__name__}")
        return pd.DataFrame()


def yf_best_effort(symbol_pairs, interval: str):
    best_df, best_sym, best_name = pd.DataFrame(), None, None
    for sym, name in symbol_pairs:
        df = fetch_yf_price(sym, name, interval)
        if len(df) > len(best_df):
            best_df, best_sym, best_name = df, sym, name
    if not best_df.empty:
        print(f"  选用 {best_name}({best_sym})，共 {len(best_df)} 条")
    return best_df, best_sym, best_name


def fetch_via_yfinance(label: str, symbol_pairs):
    print(f"\n=== 【数据源2/yfinance】抓取{label}（境外网络）===")
    daily_df, d_sym, d_name = yf_best_effort(symbol_pairs, "1d")
    daily_days = (daily_df.index.max() - daily_df.index.min()).days if not daily_df.empty else 0
    if not daily_df.empty and daily_days >= 365 * 5:
        return daily_df, "1d", d_sym, d_name
    print(f"  日K不足（{int(daily_days)}天），尝试yfinance月K")
    mo_df, m_sym, m_name = yf_best_effort(symbol_pairs, "1mo")
    if mo_df.empty and not daily_df.empty:
        return daily_df, "1d", d_sym, d_name
    return mo_df, "1mo", m_sym, m_name


# ================= 数据源3: akshare（国内期货+汇率）=================
def fetch_ak_futures_main(symbol: str, cn_name: str) -> pd.DataFrame:
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()
    print(f"  尝试 akshare.futures_main_sina {cn_name}({symbol}) ...")
    try:
        df = ak.futures_main_sina(
            symbol=symbol,
            start_date=START_DATE.replace("-", ""),
            end_date=END_DATE.replace("-", ""),
        )
        if df is None or df.empty:
            print(f"    无数据")
            return pd.DataFrame()
        df = df.rename(columns={"日期": "date", "收盘价": "price"})
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")[["price"]].sort_index()
        df = df.dropna(subset=["price"])
        print(f"    OK：{len(df)} 条，{df.index.min().date()} ~ {df.index.max().date()}")
        return df
    except Exception as e:
        print(f"    失败：{type(e).__name__}: {e}")
        return pd.DataFrame()


def fetch_ak_usdcny() -> pd.DataFrame:
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()
    print("  尝试 akshare.currency_boc_safe 美元兑人民币中间价 ...")
    try:
        df = ak.currency_boc_safe()
        if df is None or df.empty:
            print("    无数据")
            return pd.DataFrame()
        df = df.rename(columns={"日期": "date", "美元": "usdcny_per_100"})
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")[["usdcny_per_100"]].sort_index()
        df = df.dropna(subset=["usdcny_per_100"])
        df["usdcny"] = df["usdcny_per_100"] / 100.0
        print(f"    OK：{len(df)} 条，{df.index.min().date()} ~ {df.index.max().date()}")
        return df[["usdcny"]]
    except Exception as e:
        print(f"    失败：{type(e).__name__}: {e}")
        return pd.DataFrame()


def cny_to_usd_per_oz(price_cny: pd.Series, usdcny: pd.Series, unit_per_gram: bool) -> pd.Series:
    usdcny_ff = usdcny.reindex(
        usdcny.index.union(price_cny.index)
    ).sort_index().ffill().reindex(price_cny.index)
    if usdcny_ff.isna().any():
        usdcny_ff = usdcny_ff.bfill()
    if usdcny_ff.isna().all():
        print("    ⚠ 汇率数据不可用，使用固定汇率 7.0 兜底估算")
        usdcny_ff = pd.Series(7.0, index=price_cny.index)

    if unit_per_gram:
        usd_per_oz = price_cny * OZ_TO_GRAM / usdcny_ff
    else:
        usd_per_oz = (price_cny / 1000.0) * OZ_TO_GRAM / usdcny_ff
    return usd_per_oz


def fetch_via_akshare():
    print("\n=== 【数据源3/akshare】沪金AU0+沪银AG0主连（国内期货，人民币计价）===")
    au_raw = fetch_ak_futures_main("AU0", "沪金主连")
    ag_raw = fetch_ak_futures_main("AG0", "沪银主连")
    usdcny = fetch_ak_usdcny()

    if au_raw.empty or ag_raw.empty:
        print("  akshare抓取失败（黄金或白银为空）")
        return None

    gold_df = pd.DataFrame(index=au_raw.index)
    gold_df["price"] = cny_to_usd_per_oz(au_raw["price"], usdcny["usdcny"], unit_per_gram=True)
    silver_df = pd.DataFrame(index=ag_raw.index)
    silver_df["price"] = cny_to_usd_per_oz(ag_raw["price"], usdcny["usdcny"], unit_per_gram=False)

    gold_df = gold_df.dropna(subset=["price"])
    silver_df = silver_df.dropna(subset=["price"])

    gold_note = "沪金主连AU0(akshare新浪)+BOC汇率换算为美元/盎司"
    silver_note = "沪银主连AG0(akshare新浪)+BOC汇率换算为美元/盎司"
    print(f"  换算完成：黄金 {len(gold_df)} 条，白银 {len(silver_df)} 条")
    return gold_df, silver_df, gold_note, silver_note


# ================= 合并 + 周期对齐 =================
def align_merge(gold_df: pd.DataFrame, silver_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(
        gold_df.rename(columns={"price": "gold_price"}),
        silver_df.rename(columns={"price": "silver_price"}),
        left_index=True, right_index=True, how="inner",
    ).sort_index()
    merged["gold_silver_ratio"] = merged["gold_price"] / merged["silver_price"]
    return merged[["gold_price", "silver_price", "gold_silver_ratio"]]


# ================= CSV 导出 =================
def export_csv(df: pd.DataFrame, gold_info: str, silver_info: str, k_interval: str,
               note: str = "") -> str:
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    start = df.index.min().strftime("%Y%m%d")
    end = df.index.max().strftime("%Y%m%d")
    period_tag = "日K" if k_interval == "1d" else "月K"
    filename = f"黄金白银金银比_{period_tag}_{start}_至_{end}_导出{now_str}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    df_out = df.reset_index().copy()
    df_out.columns = ["时间", "黄金价格(美元/盎司)", "白银价格(美元/盎司)", "金银比(黄金价格/白银价格)"]
    df_out.insert(1, "黄金数据来源", gold_info)
    df_out.insert(2, "白银数据来源", silver_info)
    if note:
        df_out["数据说明"] = note
    df_out.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"\n✓ CSV 数据已导出：{filepath}")
    print(f"  行数：{len(df_out)}")
    return filepath


# ================= 图表绘制 =================
def plot_charts(df: pd.DataFrame, gold_name: str, silver_name: str, k_interval: str):
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    start = df.index.min().strftime("%Y%m%d")
    end = df.index.max().strftime("%Y%m%d")
    period_tag = "日K" if k_interval == "1d" else "月K"

    for font in ["Microsoft YaHei", "SimHei", "Arial Unicode MS", "DejaVu Sans"]:
        try:
            plt.rcParams["font.sans-serif"] = [font]
            plt.rcParams["axes.unicode_minus"] = False
            break
        except Exception:
            continue

    # 图1：三图综合
    fig, axes = plt.subplots(3, 1, figsize=(16, 14), sharex=True)
    fig.suptitle(
        f"黄金 & 白银历史价格与金银比（{period_tag}，{df.index.min().date()} ~ {df.index.max().date()}）",
        fontsize=16, fontweight="bold",
    )
    ax1, ax2, ax3 = axes

    ax1.plot(df.index, df["gold_price"], color="#D4AF37", linewidth=0.8, label=gold_name)
    ax1.set_ylabel("黄金价格 (美元/盎司)", fontsize=12)
    ax1.set_title("黄金历史价格走势", fontsize=13)
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc="upper left", fontsize=9)

    ax2.plot(df.index, df["silver_price"], color="#708090", linewidth=0.8, label=silver_name)
    ax2.set_ylabel("白银价格 (美元/盎司)", fontsize=12)
    ax2.set_title("白银历史价格走势", fontsize=13)
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc="upper left", fontsize=9)

    mean_ratio = df["gold_silver_ratio"].mean()
    ax3.plot(df.index, df["gold_silver_ratio"], color="#8B0000", linewidth=0.8, label="金银比")
    ax3.axhline(y=mean_ratio, color="blue", linestyle="--", alpha=0.6,
                label=f"历史均值 {mean_ratio:.2f}")
    ax3.axvline(x=pd.Timestamp("1971-08-15"), color="green", linestyle="-.", alpha=0.6,
                label="1971-08-15 布雷顿森林体系结束")
    ax3.set_ylabel("金银比", fontsize=12)
    ax3.set_xlabel("时间", fontsize=12)
    ax3.set_title("金银比历史走势（数值越高=黄金相对白银越贵）", fontsize=13)
    ax3.grid(True, alpha=0.3)
    ax3.legend(loc="upper left", fontsize=9)
    ax3.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.autofmt_xdate()
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    img_name = f"黄金白银金银比走势图_{period_tag}_{start}_至_{end}_{now_str}.png"
    img_path = os.path.join(OUTPUT_DIR, img_name)
    fig.savefig(img_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ 综合走势图已保存：{img_path}")

    # 图2：金银比放大+分位数
    fig2, ax = plt.subplots(figsize=(16, 7))
    ax.plot(df.index, df["gold_silver_ratio"], color="#8B0000", linewidth=1.0)
    q10 = df["gold_silver_ratio"].quantile(0.1)
    q90 = df["gold_silver_ratio"].quantile(0.9)
    med = df["gold_silver_ratio"].median()
    ax.axhline(y=mean_ratio, color="blue", linestyle="--", alpha=0.6,
               label=f"均值 {mean_ratio:.2f}")
    ax.axhline(y=med, color="purple", linestyle="-.", alpha=0.6,
               label=f"中位数 {med:.2f}")
    ax.axhline(y=q10, color="green", linestyle=":", alpha=0.7,
               label=f"10%分位 {q10:.2f}（白银相对偏贵）")
    ax.axhline(y=q90, color="orange", linestyle=":", alpha=0.7,
               label=f"90%分位 {q90:.2f}（黄金相对偏贵）")
    cur = df["gold_silver_ratio"].iloc[-1]
    ax.scatter([df.index[-1]], [cur], color="red", s=80, zorder=5,
               label=f"最新值 {cur:.2f} ({df.index[-1].date()})")
    ax.axvline(x=pd.Timestamp("1971-08-15"), color="gray", linestyle="-.", alpha=0.5,
               label="1971-08-15 布雷顿森林体系结束")
    ax.set_title(
        f"金银比分布（{period_tag}，{df.index.min().date()} ~ {df.index.max().date()}）",
        fontsize=14, fontweight="bold",
    )
    ax.set_ylabel("金银比", fontsize=12)
    ax.set_xlabel("时间", fontsize=12)
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig2.autofmt_xdate()

    ratio_img = f"金银比分布_{period_tag}_{start}_至_{end}_{now_str}.png"
    ratio_path = os.path.join(OUTPUT_DIR, ratio_img)
    fig2.savefig(ratio_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)
    print(f"✓ 金银比分布图已保存：{ratio_path}")

    return img_path, ratio_path


# ================= 统计打印 =================
def print_stats(df: pd.DataFrame):
    print("\n" + "=" * 72)
    print(" 数据概览 & 金银比统计")
    print("=" * 72)
    n_years = (df.index.max() - df.index.min()).days / 365.25
    print(f" 数据条数        ：{len(df)}")
    print(f" 时间范围        ：{df.index.min().date()} ~ {df.index.max().date()}")
    print(f" 覆盖年数        ：{n_years:.1f} 年")
    print(f" 请求起始 1900   ：目前免费公开日K级最早到 1968 年（LBMA）；")
    print(f"                   1971 年至今完全覆盖，满足分析需求。")
    if df.index.min().year > 1971:
        print(f" ⚠ 本批次实际最早 {df.index.min().year} 年（未覆盖到 1971，建议重试LBMA接口）")
    print("\n【黄金价格 (美元/盎司)】")
    print(f"  最低   {df['gold_price'].min():>10.2f}   @ {df['gold_price'].idxmin().date()}")
    print(f"  最高   {df['gold_price'].max():>10.2f}   @ {df['gold_price'].idxmax().date()}")
    print(f"  均值   {df['gold_price'].mean():>10.2f}")
    print(f"  最新   {df['gold_price'].iloc[-1]:>10.2f}   @ {df.index[-1].date()}")
    print("\n【白银价格 (美元/盎司)】")
    print(f"  最低   {df['silver_price'].min():>10.3f}   @ {df['silver_price'].idxmin().date()}")
    print(f"  最高   {df['silver_price'].max():>10.3f}   @ {df['silver_price'].idxmax().date()}")
    print(f"  均值   {df['silver_price'].mean():>10.3f}")
    print(f"  最新   {df['silver_price'].iloc[-1]:>10.3f}   @ {df.index[-1].date()}")
    print("\n【金银比】")
    print(f"  最低   {df['gold_silver_ratio'].min():>8.2f}   @ {df['gold_silver_ratio'].idxmin().date()}")
    print(f"  最高   {df['gold_silver_ratio'].max():>8.2f}   @ {df['gold_silver_ratio'].idxmax().date()}")
    print(f"  均值   {df['gold_silver_ratio'].mean():>8.2f}")
    print(f"  中位数 {df['gold_silver_ratio'].median():>8.2f}")
    print(f"  10%分位 {df['gold_silver_ratio'].quantile(0.1):>6.2f}")
    print(f"  90%分位 {df['gold_silver_ratio'].quantile(0.9):>6.2f}")
    print(f"  最新   {df['gold_silver_ratio'].iloc[-1]:>8.2f}   @ {df.index[-1].date()}")

    # 按10年分段打印金银比均值，方便观察长期变化
    print("\n【金银比 · 十年分段均值】")
    df2 = df.copy()
    df2["decade"] = (df2.index.year // 10) * 10
    grp = df2.groupby("decade")["gold_silver_ratio"].mean()
    for dec, val in grp.items():
        count = int(((df2["decade"] == dec)).sum())
        print(f"  {dec}s  均值 {val:>7.2f}   (样本 {count} 条)")
    print("=" * 72)


# ================= 主流程 =================
def main():
    print("=" * 72)
    print(" 黄金白银历史数据抓取 & 金银比分析工具")
    print(f" 请求区间：{START_DATE} ~ {END_DATE}")
    print(" 数据源优先级：LBMA官方JSON(1968起日K，国内直连)")
    print("              → yfinance(境外网络) → akshare(国内期货+汇率)")
    print("=" * 72)

    g_df = s_df = pd.DataFrame()
    gold_info = silver_info = ""
    used_interval = "1d"
    source_tag = ""

    # 阶段1：LBMA（最优，1968起日K）
    t0 = time.time()
    lbma = fetch_via_lbma()
    if lbma is not None:
        g_df, s_df, gold_info, silver_info = lbma
        source_tag = "LBMA官方JSON（伦敦金银市场协会定盘价，美元/盎司，日K）"
        used_interval = "1d"
    else:
        # 阶段2：yfinance
        print("\n※ LBMA 不可用，尝试 yfinance ...")
        g_df, g_int, g_sym, g_note = fetch_via_yfinance("黄金", YF_GOLD_SYMBOLS)
        s_df, s_int, s_sym, s_note = fetch_via_yfinance("白银", YF_SILVER_SYMBOLS)
        if g_df.empty or s_df.empty:
            # 阶段3：akshare
            print("\n※ yfinance 不可用或数据不足，切换到 akshare ...")
            ak = fetch_via_akshare()
            if ak is None:
                print("\n✗ 所有数据源均失败，请检查网络/安装的依赖库，或稍后重试。")
                sys.exit(1)
            g_df, s_df, gold_info, silver_info = ak
            used_interval = "1d"
            source_tag = "akshare 新浪国内期货+汇率换算（美元/盎司）"
        else:
            gold_info = f"{g_note}({g_sym})" if g_sym else ""
            silver_info = f"{s_note}({s_sym})" if s_sym else ""
            source_tag = "yfinance（COMEX/现货，美元/盎司）"
            used_interval = g_int if g_int == s_int else "1mo"
            if g_int != s_int:
                print(f"\n※ 黄金({g_int})/白银({s_int})周期不一致，统一按月K重采样对齐")
                g_df = g_df.resample("MS").last().dropna()
                s_df = s_df.resample("MS").last().dropna()

    print(f"\n数据源：{source_tag}，耗时 {time.time() - t0:.1f}s")

    # 合并 + 计算金银比
    merged = align_merge(g_df, s_df)
    if merged.empty:
        print("✗ 合并后无有效数据（黄金白银日期无法对齐）")
        sys.exit(1)

    # 如果用户请求 1971 年起，且日K数据最早 >= 1968，那么已经覆盖。
    # 可以保留全部（1968年至今），让图表展示更完整；也可以裁剪到 1971 之后。
    # 这里保留完整数据，并在控制台明确标注 1971年至今完全覆盖。
    # 如需严格 1971-01-01 起，取消下一行注释：
    # merged = merged.loc[merged.index >= pd.Timestamp("1971-01-01")]

    note_line = (
        f"数据源：{source_tag}；请求区间 {START_DATE}~{END_DATE}；"
        f"实际覆盖 {merged.index.min().date()} ~ {merged.index.max().date()}；"
        f"价格单位统一为 美元/盎司；"
        f"1971年至今完全覆盖，1968~1971为布雷顿森林末期数据（官价附近窄幅波动）。"
    )

    print_stats(merged)
    csv_path = export_csv(merged, gold_info, silver_info, used_interval, note=note_line)
    img_paths = plot_charts(merged, gold_info, silver_info, used_interval)

    print("\n=== 全部完成 ===")
    print(f"输出目录：{OUTPUT_DIR}")
    print(f"  CSV 数据文件 ：{os.path.basename(csv_path)}")
    print(f"  综合走势图    ：{os.path.basename(img_paths[0])}")
    print(f"  金银比分布图 ：{os.path.basename(img_paths[1])}")


if __name__ == "__main__":
    main()
