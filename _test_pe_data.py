# -*- coding: utf-8 -*-
"""排查 000895 个股市盈率(TTM)和深证PE历史数据是否为空"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web_app import _uv_fetch_stock_pe_ttm, _uv_load_shenzhen_pe_history
import pandas as pd

print("=" * 80)
print("📌 [000895 双汇发展] 查询个股市盈率(TTM)历史")
print("=" * 80)
pe = _uv_fetch_stock_pe_ttm("000895")
print(f"  返回长度: {len(pe)} 条")
if len(pe) > 0:
    print(f"  日期范围: {pe.index.min().date()} ~ {pe.index.max().date()}")
    print(f"  前5条数据:")
    for i, (d, v) in enumerate(pe.head().items()):
        print(f"    {d.date()} : PE={v:.2f}")
    print(f"  后5条数据:")
    for i, (d, v) in enumerate(pe.tail().items()):
        print(f"    {d.date()} : PE={v:.2f}")
    # 检查 2023-10-24 附近有没有数据
    d0 = pd.Timestamp("2023-10-24")
    near = pe.iloc[(pe.index - d0).abs().argsort()[:5]]
    print(f"  2023-10-24 附近的 PE:")
    for d, v in near.items():
        print(f"    {d.date()} : PE={v:.2f}")
else:
    print("  ❌ 完全没有数据！")

print()
print("=" * 80)
print("📌 深证指数 PE(TTM) 历史（本地DB）")
print("=" * 80)
sz = _uv_load_shenzhen_pe_history()
print(f"  返回长度: {len(sz)} 条")
if len(sz) > 0:
    print(f"  日期范围: {sz.index.min().date()} ~ {sz.index.max().date()}")
    print(f"  数据类型: {type(sz.index)}")
    # 检查 2023-10-24 附近有没有数据
    d0 = pd.Timestamp("2023-10-24")
    if isinstance(sz.index, pd.DatetimeIndex):
        if d0 in sz.index:
            print(f"  2023-10-24 的深证PE: {sz.loc[d0]:.2f}")
        else:
            near = sz.iloc[(sz.index - d0).abs().argsort()[:3]]
            print(f"  2023-10-24 最近的3条:")
            for d, v in near.items():
                print(f"    {d.date()} : PE={v:.2f}")
    # 检查 2024-04 ~ 2026-04（你截图里分红的日期）有没有 PE 数据
    check_dates = ["2024-04-26", "2024-09-11", "2025-04-30", "2025-08-22", "2026-04-29", "2026-07-08"]
    print(f"  检查分红/持有日的深证PE:")
    for d_str in check_dates:
        d = pd.Timestamp(d_str)
        if isinstance(sz.index, pd.DatetimeIndex):
            if d in sz.index:
                print(f"    {d_str}: {sz.loc[d]:.2f}")
            else:
                # 找最近的前一条
                prev = sz[sz.index <= d]
                if len(prev) > 0:
                    print(f"    {d_str}: 最近前值 {prev.index[-1].date()} = {prev.iloc[-1]:.2f}（非交易日，需forward-fill！）")
                else:
                    print(f"    {d_str}: ❌ 无数据")
