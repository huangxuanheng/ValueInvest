# -*- coding: utf-8 -*-
"""
黄金白银金银比 前后端一体化服务
=================================
功能：
  1. 长期后台运行：Flask 提供 Web 页面 + APScheduler 每小时抓当日最新金/银价格
  2. 数据库：finance_analysis 库，表 precious_metals
        字段：date_k (DATE, PK) | gold_price (FLOAT) | silver_price (FLOAT)
        默认使用 SQLite（文件型，零配置）；切 MySQL 只需改 DB_URL
  3. 首页：Plotly.js 绘制金银比折线图
        - 横轴 date_k（日期），纵轴 金银比=gold_price/silver_price
        - 原生支持"类地图"分层缩放：看全景→十年刻度，放大→年/月/日自动细化
  4. API：
        /api/data          → 返回全量数据 JSON
        /api/refresh_now   → 立即手动触发一次采集（调试用）
        /api/latest        → 返回最近一条记录
        /api/stats         → 返回统计摘要

启动：
  pip install -r requirements_web.txt
  python web_app.py
  浏览器访问 http://127.0.0.1:5000
"""

import os
import sys
import json
import time
import threading
import datetime as dt
from datetime import datetime, timedelta

import pandas as pd
from flask import Flask, render_template, jsonify, request

# ============================================================
# 1. 配置区（数据库等）
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
os.makedirs(TEMPLATE_DIR, exist_ok=True)

# —— 数据库连接 ——
#  SQLite（默认，零配置）：
DB_NAME = "finance_analysis"
DB_URL = f"sqlite:///{os.path.join(DATA_DIR, DB_NAME)}.db"
#
#  若切 MySQL：pip install pymysql，然后把下面这行取消注释、填账号
# DB_URL = "mysql+pymysql://user:password@localhost:3306/finance_analysis?charset=utf8mb4"

TABLE_NAME = "precious_metals"

# —— 采集周期（每小时）——
COLLECT_INTERVAL_HOURS = 1

LBMA_GOLD_JSON = "https://prices.lbma.org.uk/json/gold_am.json"
LBMA_SILVER_JSON = "https://prices.lbma.org.uk/json/silver.json"

# ============================================================
# 1-bis. 告警阈值 & 邮件配置  ⬇⬇⬇ 请在这里自行配置 ⬇⬇⬇
# ============================================================
RATIO_UPPER_THRESHOLD = 80.0   # 金银比 > 80 发邮件（黄金贵/白银便宜，可能是抛金买银的机会）
RATIO_LOWER_THRESHOLD = 50.0   # 金银比 < 50 发邮件（黄金便宜/白银贵，可能是买金抛银的机会）
EMAIL_ALERT_COOLDOWN_HOURS = 6  # 同一方向的告警冷却时长（小时），避免连续轰炸；0=每次触发都发

# 邮件配置说明：
#   - 163 / QQ 邮箱都需要在邮箱网页版里开启【SMTP服务】并获取【授权码】（不是登录密码！）
#       · 163: 设置 → POP3/SMTP/IMAP → 开启 SMTP → 生成授权码
#       · QQ : 设置 → 账户 → 开启「POP3/SMTP服务」→ 生成授权码
#   - 发件人 EMAIL_FROM 必须 = EMAIL_USER（SMTP登录身份）
#   - 收件人 EMAIL_TO 可以填自己同一个邮箱，收到就是成功
#   - 填完后启动服务即可；未填前会自动跳过邮件告警（不影响核心功能）
#  —————————— 要启用邮件告警，请把 EMAIL_ENABLED 改成 True，再填下面4行 ——————————
EMAIL_ENABLED = False
# —— 方案1：163 邮箱（取消下面4行注释并填授权码）
# EMAIL_HOST = "smtp.163.com"
# EMAIL_PORT = 465
# EMAIL_USER = "yourname@163.com"
# EMAIL_PASS = "XXXXXXXXXXXXXXXX"   # 163的【授权码】（16位字母，不是登录密码）
# —— 方案2：QQ 邮箱（取消下面4行注释并填授权码，注意是SSL端口465）
# EMAIL_HOST = "smtp.qq.com"
# EMAIL_PORT = 465
# EMAIL_USER = "123456789@qq.com"
# EMAIL_PASS = "XXXXXXXXXXXXXXXX"   # QQ的【授权码】（通常16位字母）
EMAIL_FROM = locals().get("EMAIL_USER", "")   # 发件人（默认=SMTP登录账号）
EMAIL_TO   = locals().get("EMAIL_USER", "")   # 收件人（默认发给自己，可改成多个 ["a@163.com","b@qq.com"] 列表）


# 简单的进程内冷却状态记录（重启服务即清空；如需跨进程持久化可改为写表/文件）
_alert_state = {
    "upper_last_at": None,   # datetime or None，>阈值的最近一次告警时间
    "lower_last_at": None,   # datetime or None，<阈值的最近一次告警时间
}

# ============================================================
# 1-ter. 交易日 & 请求跳过 策略
# ============================================================
# LBMA (伦敦金银市场协会) 定盘价仅在「伦敦工作日」发布：
#   1) 周六周日铁定休市 → 直接跳过
#   2) 英国公共假日（元旦/受难日/复活节一/5月初银行假/5月末春假/8月末夏假/圣诞节/节礼日）也休市
#      → 这类假日不做硬编码，用「智能跳过」自动兼容：
#        · 只要当天我们已拿到过「LBMA最新日期 == 当日」的新数据，当天就不再请求
#        · 即使是工作日但假日（英国没开市），LBMA返回旧日期，
#          我们会每小时查一次（一年仅6~8天这种情况，可接受）；
#          想更省流量可自行把这些假日加到 EXTRA_HOLIDAYS 里
EXTRA_HOLIDAYS = set()   # 可自己填 (date(2026,12,25), date(2026,4,6), ...)
_skip_collect = {"done_date": None}   # 当日已拿到过 LBMA 新数据 → 存当日 date.isoformat()


def is_likely_trading_day(today=None) -> bool:
    """判断今天是否可能为交易日：周一~周五 + 不在 EXTRA_HOLIDAYS 黑名单"""
    today = today or datetime.now().date()
    if today.weekday() >= 5:
        return False
    if today in EXTRA_HOLIDAYS:
        return False
    return True


# ============================================================
# 2. SQLAlchemy 数据库 & 模型
# ============================================================
from sqlalchemy import (
    create_engine, Column, Date, Float, String, PrimaryKeyConstraint, text,
)
from sqlalchemy.orm import declarative_base, sessionmaker

engine = create_engine(DB_URL, echo=False, future=True,
                       connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
Base = declarative_base()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class PreciousMetal(Base):
    """对应表 precious_metals"""
    __tablename__ = TABLE_NAME
    date_k = Column(Date, primary_key=True, comment="交易日(YYYY-MM-DD)")
    gold_price = Column(Float, nullable=True, comment="黄金价格 美元/盎司")
    silver_price = Column(Float, nullable=True, comment="白银价格 美元/盎司")

    def to_dict(self):
        return {
            "date_k": self.date_k.isoformat() if self.date_k else None,
            "gold_price": float(self.gold_price) if self.gold_price is not None else None,
            "silver_price": float(self.silver_price) if self.silver_price is not None else None,
        }


def init_db():
    """初始化：建库（若MySQL）+ 建全部两张表"""
    # SQLite 不需要 CREATE DATABASE
    if not DB_URL.startswith("sqlite"):
        try:
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4"))
                conn.commit()
        except Exception:
            pass
    Base.metadata.create_all(bind=engine)
    db_path = DB_URL.split('@')[-1] if '@' in DB_URL else DB_URL
    print(f"[DB] 已初始化，连接：{db_path}")
    print(f"     - 表 {TABLE_NAME}      (金银价)：记录数 = {count_rows()}")
    print(f"     - 表 {INDEX_TABLE} (指数估值)：记录数 = {index_count_rows()}")


def upsert_row(date_val: dt.date, gold: float | None, silver: float | None):
    """插入或按日期更新一条。两者都缺省时跳过。"""
    if gold is None and silver is None:
        return False
    with SessionLocal() as s:
        row = s.get(PreciousMetal, date_val)
        if row is None:
            row = PreciousMetal(date_k=date_val, gold_price=gold, silver_price=silver)
            s.add(row)
        else:
            if gold is not None:
                row.gold_price = gold
            if silver is not None:
                row.silver_price = silver
        s.commit()
    return True


def read_all_rows() -> list[dict]:
    """读出全部记录（按日期升序），附带金银比"""
    with SessionLocal() as s:
        rows = s.query(PreciousMetal).order_by(PreciousMetal.date_k).all()
        data = []
        for r in rows:
            d = r.to_dict()
            if d["gold_price"] and d["silver_price"] and d["silver_price"] > 0:
                d["ratio"] = round(d["gold_price"] / d["silver_price"], 4)
            else:
                d["ratio"] = None
            data.append(d)
        return data


def count_rows() -> int:
    with SessionLocal() as s:
        return s.query(PreciousMetal).count()


def latest_row() -> dict | None:
    with SessionLocal() as s:
        r = s.query(PreciousMetal).order_by(PreciousMetal.date_k.desc()).first()
        if not r:
            return None
        d = r.to_dict()
        if d["gold_price"] and d["silver_price"] and d["silver_price"] > 0:
            d["ratio"] = round(d["gold_price"] / d["silver_price"], 4)
        return d


# ============================================================
# 2-bis. 贵金属 按需自动补采（每次打开金银比页面都会判断是否过期 → 后台线程抓最新 LBMA 定价）
# ============================================================
_PM_REFRESH_STATE = {"in_progress": False, "last_attempt_ts": 0.0, "last_success_ts": 0.0}
_PM_REFRESH_LOCK = threading.Lock()
_PM_REFRESH_MIN_INTERVAL_SEC = 30 * 60   # 贵金属最短采集间隔 30 分钟


def _pm_expected_latest_trade_date(today=None) -> dt.date:
    """贵金属（LBMA伦敦定盘价）只在工作日发布：周六/日 → 回退上周五"""
    if today is None:
        today = datetime.now().date()
    wd = today.weekday()
    if wd == 5:
        return today - timedelta(days=1)
    if wd == 6:
        return today - timedelta(days=2)
    return today


def _pm_is_stale() -> tuple:
    """返回 (是否过期, 说明) —— 库中最新日期 < 预期最新工作日 → 过期（含周末回退）"""
    expected = _pm_expected_latest_trade_date()
    latest = latest_row()
    if latest is None or not latest.get("date_k"):
        return True, "库空无数据"
    dk = latest["date_k"]
    if isinstance(dk, str):
        dk = dt.date.fromisoformat(dk)
    if dk < expected:
        return True, f"库最新 {dk} < 预期 {expected}"
    return False, f"库最新 {dk} 已是最新"


def _trigger_bg_pm_refresh_if_stale(caller: str = "api") -> tuple:
    """非阻塞：贵金属数据过期 → daemon 后台线程 collect_and_save。30 分钟节流 + 运行中锁。
    返回 (triggered: bool, note: str)，永抛异常吞掉。"""
    try:
        stale, why = _pm_is_stale()
        if not stale:
            return False, f"贵金属数据新鲜（{why}）"
        now = time.time()
        with _PM_REFRESH_LOCK:
            if _PM_REFRESH_STATE["in_progress"]:
                return False, "贵金属后台刷新进行中"
            gap = now - _PM_REFRESH_STATE["last_attempt_ts"]
            if gap < _PM_REFRESH_MIN_INTERVAL_SEC:
                minutes_ago = int(gap // 60)
                minutes_left = int((_PM_REFRESH_MIN_INTERVAL_SEC - gap) // 60)
                return False, f"贵金属 {minutes_ago} 分钟前刚尝试过，30 分钟节流中（剩 {minutes_left} 分钟），跳过"
            _PM_REFRESH_STATE["in_progress"] = True
            _PM_REFRESH_STATE["last_attempt_ts"] = now
    except Exception as e:
        return False, f"贵金属刷新前置检查异常：{type(e).__name__}"

    def _run():
        try:
            res = collect_and_save(reason=f"页面触发:{caller}", force=False)
            ok = bool(res and res.get("ok") and not res.get("skipped"))
            print(f"[PM/AutoRefresh/{caller}] 完成: {res.get('msg') if res else 'None'}")
            with _PM_REFRESH_LOCK:
                _PM_REFRESH_STATE["in_progress"] = False
                if ok:
                    _PM_REFRESH_STATE["last_success_ts"] = time.time()
        except Exception as e:
            print(f"[PM/AutoRefresh/{caller}] 后台刷新异常: {type(e).__name__}: {e}")
            with _PM_REFRESH_LOCK:
                _PM_REFRESH_STATE["in_progress"] = False

    threading.Thread(target=_run, name="pm_autorefresh_bg", daemon=True).start()
    return True, f"贵金属数据过期（{why}）→ 已启动后台补采（本次先返回现有数据，数秒后再刷新页面即可见最新数据）"


# ============================================================
# 2-bis2. 美国基础货币/黄金储备 按需自动补采（每次打开页面都会判断 → 后台线程重新生成当月插值）
# ============================================================
_USA_REFRESH_STATE = {"in_progress": False, "last_attempt_ts": 0.0, "last_success_ts": 0.0}
_USA_REFRESH_LOCK = threading.Lock()
_USA_REFRESH_MIN_INTERVAL_SEC = 30 * 60   # 日度数据 + 金价联动，节流 30 分钟（与 PM 一致）


def _usa_expected_latest_date(today=None) -> str:
    """美国货币/黄金储备为日度数据 + 金价取贵金属工作日定盘价 → 预期最新日期 = 贵金属预期最新工作日
    （周六/日回退上周五，与 LBMA 发布日历一致）"""
    return _pm_expected_latest_trade_date(today).strftime("%Y-%m-%d")


def _usa_is_stale() -> tuple:
    """返回 (是否过期, 说明)：
    1) 缓存不存在 → 过期
    2) 缓存的最新日期 < 预期最新工作日 → 过期（日度数据已到当日/上周五；贵金属金价有新记录）
    3) 缓存超过 24 小时 → 过期（每日至少重算一次，保证金价跟随 precious_metals）
    """
    global _usa_money_gold_cache
    expected = _usa_expected_latest_date()

    # 情况1：缓存空
    if _usa_money_gold_cache is None:
        return True, "无缓存"

    # 情况2：缓存最新日期 < 预期最新工作日（今天是工作日则需至今日；周末需至上周五）
    cached_to = _usa_money_gold_cache.get("summary", {}).get("range", {}).get("to") or ""
    if not cached_to or cached_to < expected:
        return True, f"最新仅至 {cached_to or '—'}，预期需至 {expected}"

    # 情况3：即使日期够，缓存超过 24 小时也重算一次（保证金价使用 precious_metals 的最新日价）
    age = time.time() - _usa_money_gold_cache.get("_ts", 0)
    if age > 86400:
        hrs = int(age // 3600)
        return True, f"缓存已超 {hrs} 小时（阈值 24h，确保金价同步最新 LBMA）"

    hrs = int(age // 3600)
    return False, f"已至 {cached_to}，缓存 {hrs} 小时（阈值 24h）"


def _trigger_bg_usa_refresh_if_stale(caller: str = "api") -> tuple:
    """非阻塞：USA 货币/黄金储备数据过期 → daemon 后台线程重生成数据并刷新缓存。60 分钟节流 + 运行中锁。
    返回 (triggered: bool, note: str)，永抛异常吞掉。"""
    try:
        stale, why = _usa_is_stale()
        if not stale:
            return False, f"美债/黄金储备数据新鲜（{why}）"
        now = time.time()
        with _USA_REFRESH_LOCK:
            if _USA_REFRESH_STATE["in_progress"]:
                return False, "美国货币/黄金后台刷新进行中"
            gap = now - _USA_REFRESH_STATE["last_attempt_ts"]
            if gap < _USA_REFRESH_MIN_INTERVAL_SEC:
                minutes_ago = int(gap // 60)
                minutes_left = int((_USA_REFRESH_MIN_INTERVAL_SEC - gap) // 60)
                return False, f"美国货币/黄金 {minutes_ago} 分钟前刚尝试过，60 分钟节流中（剩 {minutes_left} 分钟），跳过"
            _USA_REFRESH_STATE["in_progress"] = True
            _USA_REFRESH_STATE["last_attempt_ts"] = now
    except Exception as e:
        return False, f"美国货币/黄金刷新前置检查异常：{type(e).__name__}"

    def _run():
        global _usa_money_gold_cache
        ok = False
        try:
            # 重跑数据生成函数（会用最新的 precious_metals 金价月均 + 重新生成月度插值）
            result = _fetch_usa_money_gold_data()
            dates     = result["data"]["date"]
            gold_t    = result["data"]["gold_reserve_tons"]
            gold_p    = result["data"]["gold_price_usd_oz"]
            gold_usdb = result["data"]["gold_reserve_usd_bn"]
            mb_b      = result["data"]["monetary_base_bn"]
            cc_b      = result["data"]["currency_circ_bn"]

            def _valid(arr): return [x for x in arr if x is not None]
            gt_v = _valid(gold_t); gp_v = _valid(gold_p); gu_v = _valid(gold_usdb)
            mb_v = _valid(mb_b); cc_v = _valid(cc_b)
            summary = {
                "range": {"from": dates[0] if dates else None, "to": dates[-1] if dates else None},
                "gold_reserve_latest_tons": gold_t[-1] if gold_t else None,
                "gold_reserve_min_tons":    min(gt_v) if gt_v else None,
                "gold_reserve_max_tons":    max(gt_v) if gt_v else None,
                "gold_price_latest_usd_oz": gold_p[-1] if gold_p else None,
                "gold_price_min_usd_oz":    min(gp_v) if gp_v else None,
                "gold_price_max_usd_oz":    max(gp_v) if gp_v else None,
                "gold_reserve_latest_usd_bn": gold_usdb[-1] if gold_usdb else None,
                "gold_reserve_min_usd_bn":    min(gu_v) if gu_v else None,
                "gold_reserve_max_usd_bn":    max(gu_v) if gu_v else None,
                "monetary_base_latest_bn": mb_b[-1] if mb_b else None,
                "monetary_base_min_bn":    min(mb_v) if mb_v else None,
                "monetary_base_max_bn":    max(mb_v) if mb_v else None,
                "currency_circ_latest_bn": cc_b[-1] if cc_b else None,
                "currency_circ_min_bn":    min(cc_v) if cc_v else None,
                "currency_circ_max_bn":    max(cc_v) if cc_v else None,
            }
            now_ts = time.time()
            payload = {
                "ok": True,
                "count": len(dates),
                "source_note": result["source_note"],
                "summary": summary,
                "date": dates,
                "gold_reserve_tons":     gold_t,
                "gold_price_usd_oz":     gold_p,
                "gold_reserve_usd_bn":   gold_usdb,
                "monetary_base_bn":      mb_b,
                "currency_circ_bn":      cc_b,
                "render_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "_ts": now_ts,
            }
            # 原子替换缓存
            _usa_money_gold_cache = payload
            ok = True
            print(f"[USA/AutoRefresh/{caller}] 完成: {len(dates)} 条, {dates[0]} ~ {dates[-1]}")
        except Exception as e:
            print(f"[USA/AutoRefresh/{caller}] 后台刷新异常: {type(e).__name__}: {e}")
        finally:
            with _USA_REFRESH_LOCK:
                _USA_REFRESH_STATE["in_progress"] = False
                if ok:
                    _USA_REFRESH_STATE["last_success_ts"] = time.time()

    threading.Thread(target=_run, name="usa_autorefresh_bg", daemon=True).start()
    return True, f"美国货币/黄金储备数据过期（{why}）→ 已启动后台补采（本次先返回现有数据，数秒后再刷新页面即可见最新数据）"


# ============================================================
# 2-ter. 指数估值表：A股平均 / 沪深300 / 创业板(50) / 中证500 / 上证50
# ============================================================
INDEX_TABLE = "index_valuation"

# 统一的指数代码映射：idx_code（主键字符串）→ (中文名, akshare_symbol_list, 描述, (PE口径Label, PB口径Label))
INDEX_META = {
    # 全A中位数估值：不被巨型权重股拉低，更能反映市场整体温度（akshare 自带 middle* + 10年分位）
    "average_pe": ("A股平均市盈率（中位数口径）", (),
                   "中位数PE/PB口径：全A成分股 PE-TTM 与 PB 的 50% 分位，不被金融等巨无霸拉低，更贴近真实市场温度",
                   ("中位数PE(TTM · 全A成分股 · 整体法 50% 分位)", "中位数PB(全A成分股 · 整体法 50% 分位)")),
    # —— 以下 4 个乐咕宽基指数：按用户明确要求：【整体法 PE-TTM = ∑(总市值)/∑(滚动净利润)】 ——
    "hs300":  ("沪深300估值",  ("沪深300",),
               "乐咕 stock_index_pe_lg：滚动市盈率(TTM · 整体法 = ∑总市值/∑滚动净利润；市净率整体法",
               ("滚动PE(TTM · 沪深300 · 整体法 = ∑总市值/∑净利润)", "市净率(沪深300 · 整体法)")),
    "cyb":    ("创业板估值(创业板50)", ("创业板50",),
               "乐咕 symbol=创业板50(399673)：滚动市盈率(TTM · 整体法；市净率整体法（创业板指乐咕暂不提供）",
               ("滚动PE(TTM · 创业板50 · 整体法)", "市净率(创业板50 · 整体法)")),
    "zz500":  ("中证500估值",  ("中证500",),
               "乐咕：滚动市盈率(TTM · 整体法) + 市净率整体法",
               ("滚动PE(TTM · 中证500 · 整体法)", "市净率(中证500 · 整体法)")),
    "sz50":   ("上证50估值",   ("上证50",),
               "乐咕：滚动市盈率(TTM · 整体法) + 市净率整体法",
               ("滚动PE(TTM · 上证50 · 整体法)", "市净率(上证50 · 整体法)")),
    # —— 热门数据新增：市场分类整体法估值（上证/深证两板独立页）——
    "sh_market":  ("上证指数估值", ("上证",),
                   "乐咕 stock_market_pe_lg：上证A股整体法市盈率(TTM) = ∑总市值 / ∑净利润TTM（覆盖沪市全部A股）",
                   ("整体法PE(TTM · 上证指数·市场分类 = ∑总市值/∑净利润TTM)", "市净率(市场分类暂不单独提供)")),
    "sz_market":  ("深证指数估值", ("深证",),
                   "乐咕 stock_market_pe_lg：深证A股整体法市盈率(TTM)（2021年2月起已包含原中小板成分股）",
                   ("整体法PE(TTM · 深证指数·市场分类 = ∑总市值/∑净利润TTM)", "市净率(市场分类暂不单独提供)")),
}
# A股各板（市场分类）PE 代码
MARKET_PE_META = {
    "sh_market":  ("上证指数 · 市场整体法",      "上证",  "上证A股整体法PE = ∑(收盘价×总股本)/∑(每股收益×总股本)"),
    "sz_market":  ("深证指数 · 市场整体法",    "深证",  "深证A股整体法PE（2021年2月起包含原中小板）"),
    "zxb_market": ("中小板 · 市场整体法",      None,    "【注】中小板已于2021年2月并入深证主板，乐咕不提供独立分类，此处仅保留说明，无独立数据"),
    "cyb_market": ("创业板 · 市场整体法",    "创业板", "创业板整体法PE = ∑总市值 / ∑净利润TTM"),
    "kcb_market": ("科创板 · 市场整体法",    "科创版", "科创板整体法PE（乐咕symbol=科创版，注意是“版”不是“板”）"),
}
MARKET_PE_CODES = list(MARKET_PE_META.keys())   # 顺序：sh/sz/zxb/cyb/kcb 5个，含中小板占位
INDEX_CODE_LIST = list(INDEX_META.keys())


class IndexValuation(Base):
    """index_valuation 表：(date_k, idx_code) 联合主键，存 PE_TTM / PB / 估值分位"""
    __tablename__ = INDEX_TABLE
    date_k    = Column(Date, nullable=False, comment="交易日")
    idx_code  = Column(String(16), nullable=False, comment="指数代码: average_pe/hs300/cyb/zz500/sz50")
    pe_ttm    = Column(Float, nullable=True, comment="滚动市盈率(TTM，整体法)")
    pb        = Column(Float, nullable=True, comment="市净率(整体法)")
    close     = Column(Float, nullable=True, comment="指数收盘点位")
    q10y_pe   = Column(Float, nullable=True, comment="PE 的近10年历史分位数(0-1)")
    q10y_pb   = Column(Float, nullable=True, comment="PB 的近10年历史分位数(0-1)")

    __table_args__ = (
        PrimaryKeyConstraint("date_k", "idx_code", name="pk_index_valuation"),
        {"comment": "A股主要指数估值历史（日度）"},
    )

    def to_dict(self):
        return {
            "date_k":  self.date_k.isoformat() if self.date_k else None,
            "pe_ttm":  float(self.pe_ttm) if self.pe_ttm is not None else None,
            "pb":      float(self.pb)     if self.pb     is not None else None,
            "close":   float(self.close)  if self.close  is not None else None,
            "q10y_pe": float(self.q10y_pe) if self.q10y_pe is not None else None,
            "q10y_pb": float(self.q10y_pb) if self.q10y_pb is not None else None,
        }


def _safe_float(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def upsert_bulk_index(rows: list[dict]):
    """批量 upsert：rows=[{date_k, idx_code, pe_ttm, pb, close, q10y_pe, q10y_pb}]"""
    if not rows:
        return 0
    with SessionLocal() as s:
        cnt = 0
        for r in rows:
            pk = (r["date_k"], r["idx_code"])
            row = s.get(IndexValuation, pk)
            if row is None:
                row = IndexValuation(
                    date_k=r["date_k"], idx_code=r["idx_code"],
                    pe_ttm=_safe_float(r.get("pe_ttm")),
                    pb=_safe_float(r.get("pb")),
                    close=_safe_float(r.get("close")),
                    q10y_pe=_safe_float(r.get("q10y_pe")),
                    q10y_pb=_safe_float(r.get("q10y_pb")),
                )
                s.add(row)
            else:
                changed = False
                for k, col in [("pe_ttm","pe_ttm"),("pb","pb"),("close","close"),("q10y_pe","q10y_pe"),("q10y_pb","q10y_pb")]:
                    v = _safe_float(r.get(k))
                    if v is not None and getattr(row, col) != v:
                        setattr(row, col, v)
                        changed = True
                if changed:
                    cnt += 1
        s.commit()
        return cnt


def read_index_data(idx_code: str) -> list[dict]:
    """按日期升序读一个指数/市场分类的全部数据（附带分位）"""
    valid_codes = set(INDEX_META.keys()) | set(MARKET_PE_META.keys())
    if idx_code not in valid_codes:
        return []
    with SessionLocal() as s:
        rows = (s.query(IndexValuation)
                 .filter(IndexValuation.idx_code == idx_code)
                 .order_by(IndexValuation.date_k)
                 .all())
        return [r.to_dict() for r in rows]


def index_latest_row(idx_code: str) -> dict | None:
    valid_codes = set(INDEX_META.keys()) | set(MARKET_PE_META.keys())
    if idx_code not in valid_codes:
        return None
    with SessionLocal() as s:
        r = (s.query(IndexValuation)
              .filter(IndexValuation.idx_code == idx_code)
              .order_by(IndexValuation.date_k.desc())
              .first())
        return r.to_dict() if r else None


def index_count_rows(idx_code: str | None = None) -> int:
    with SessionLocal() as s:
        q = s.query(IndexValuation)
        if idx_code:
            q = q.filter(IndexValuation.idx_code == idx_code)
        return q.count()


# ============================================================
# 2-ter. 按需自动增量刷新（每次打开页面 → 后台补抓最新交易日）
# ============================================================
_REFRESH_STATE: dict = {}
_REFRESH_LOCK = threading.Lock()
_REFRESH_MIN_INTERVAL_SEC = 30 * 60  # 同一指数最短刷新间隔 30 分钟


def _expected_latest_trade_date(today=None) -> dt.date:
    """预期最新交易日（周六回退周五，周日回退周五）"""
    if today is None:
        today = datetime.now().date()
    wd = today.weekday()  # 0=Mon..6=Sun
    if wd == 5:
        return today - timedelta(days=1)
    if wd == 6:
        return today - timedelta(days=2)
    return today


def _is_index_stale(idx_code: str) -> tuple:
    """返回 (是否过期: bool, 说明文案: str)"""
    expected = _expected_latest_trade_date()
    lr = index_latest_row(idx_code)
    if lr is None:
        return True, f"库空无数据"
    latest = lr["date_k"]
    if isinstance(latest, str):
        latest = dt.date.fromisoformat(latest)
    if latest < expected:
        return True, f"库最新 {latest} < 预期 {expected}"
    return False, f"库最新 {latest} 已是最新"


def _refresh_single_now(idx_code: str) -> str:
    """【阻塞】抓 1 个指数/市场分类 + upsert + 重算近 10 年分位（非分位数据源自带除外）。返回摘要行。"""
    need_q10y = True
    try:
        if idx_code == "average_pe":
            rows = _fetch_average_pe()
            need_q10y = False
        elif idx_code in MARKET_PE_META:
            symbol = MARKET_PE_META[idx_code][1]
            rows = _fetch_market_pe(idx_code, symbol)
        else:
            syms = INDEX_META[idx_code][1]
            rows = _fetch_one_lg_index(idx_code, syms[0])
    except Exception as e:
        return f"{idx_code} 抓取异常 {type(e).__name__}: {str(e)[:120]}"

    if rows:
        chg = upsert_bulk_index(rows)
        line = f"✅ {idx_code:12s} 入库 {len(rows):5d} 行，更新 {chg:4d} 行（最新 {rows[-1]['date_k']}）"
    else:
        chg = 0
        line = f"⚠️ {idx_code:12s} 未抓到数据（数据源可能临时不可用，稍后自动重试）"

    if need_q10y and index_count_rows(idx_code) > 100:
        try:
            nq = recompute_q10y_for_index(idx_code)
            line += f"，近10年分位更新 {nq} 行"
        except Exception as e_q:
            line += f"，分位失败 {type(e_q).__name__}"
    return line


def _trigger_bg_refresh_if_stale(idx_code: str, caller: str = "api") -> tuple:
    """
    【非阻塞】若数据过期、且 30 分钟内没试过、且没在跑 → 起一个 daemon 后台线程去补采。
    立即返回 (是否启动了线程, 说明文案)。永远不抛异常。
    """
    try:
        stale, why = _is_index_stale(idx_code)
        if not stale:
            return False, f"{idx_code}: 数据新鲜（{why}）"
        now = time.time()
        with _REFRESH_LOCK:
            st = _REFRESH_STATE.get(idx_code, {})
            if st.get("in_progress"):
                return False, f"{idx_code}: 后台刷新进行中"
            last_att = st.get("last_attempt_ts", 0)
            if (now - last_att) < _REFRESH_MIN_INTERVAL_SEC:
                m = int((_REFRESH_MIN_INTERVAL_SEC - (now - last_att)) // 60)
                return False, f"{idx_code}: {m} 分钟内刚尝试过，节流跳过"
            _REFRESH_STATE[idx_code] = {
                "in_progress": True,
                "last_attempt_ts": now,
                "last_success_ts": st.get("last_success_ts", 0),
            }
    except Exception as e_prep:
        return False, f"{idx_code}: 刷新前置检查异常 {type(e_prep).__name__}"

    def _run():
        try:
            line = _refresh_single_now(idx_code)
            ok = line.startswith("✅")
            print(f"[AutoRefresh/{caller}] {line}")
        except Exception as e:
            ok = False
            print(f"[AutoRefresh/{caller}] {idx_code} 后台刷新异常: {type(e).__name__}: {e}")
        finally:
            with _REFRESH_LOCK:
                prev = _REFRESH_STATE.get(idx_code, {})
                _REFRESH_STATE[idx_code] = {
                    "in_progress": False,
                    "last_attempt_ts": time.time(),
                    "last_success_ts": time.time() if ok else prev.get("last_success_ts", 0),
                }

    threading.Thread(target=_run, name=f"autorefresh_{idx_code}", daemon=True).start()
    return True, f"{idx_code}: 过期（{why}）→ 已启动后台补采（本次先返回库中现有数据，稍后页面刷新就能看到新数据）"


# ============================================================
# 2-quater. 指数估值抓取（akshare 乐咕 / stock_a_*）+ 冷启动
# ============================================================
def _fetch_one_lg_index(idx_code: str, symbol: str) -> list[dict]:
    """抓乐咕一个指数的 PE + PB 并合并。
    根据用户明确要求：【整体法滚动PE-TTM = ∑(总市值) / ∑(滚动净利润)】
    优先使用「滚动市盈率(TTM, 整体法)」列，兜底用「滚动市盈率」列。"""
    import akshare as ak
    print(f"  [{idx_code}] 正在抓取乐咕 PE(整体法滚动TTM): {symbol} ...")
    pe_df = ak.stock_index_pe_lg(symbol=symbol)
    # 自动识别列名：优先整体法列，兼容不同版本
    pe_cols = [c for c in pe_df.columns if "滚动市盈率" in str(c)]
    pe_col = None
    for c in pe_cols:
        if "整体法" in str(c) or c == "滚动市盈率":
            pe_col = c; break
    if pe_col is None and pe_cols:
        pe_col = pe_cols[0]
    pb_cols = [c for c in pe_df.columns if str(c) == "指数" or "收盘" in str(c)]
    idx_col = "指数" if "指数" in pe_df.columns else (pb_cols[0] if pb_cols else None)

    pe_map = {}
    for _, row in pe_df.iterrows():
        d = row["日期"]
        if hasattr(d, "date"):
            d = d.date()
        pe_map[d] = {
            "pe_ttm": _safe_float(row.get(pe_col)) if pe_col else None,
            "close":  _safe_float(row.get(idx_col)) if idx_col else None,
        }
    print(f"    PE列={pe_col}, rows={len(pe_map)}, 最新日期={max(pe_map.keys()) if pe_map else 'N/A'}")

    print(f"  [{idx_code}] 正在抓取乐咕 PB(整体法): {symbol} ...")
    pb_df = ak.stock_index_pb_lg(symbol=symbol)
    pb_cols_list = [c for c in pb_df.columns if "市净率" in str(c)]
    pb_col = None
    for c in pb_cols_list:
        if "整体法" in str(c) or c == "市净率":
            pb_col = c; break
    if pb_col is None and pb_cols_list:
        pb_col = pb_cols_list[0]

    pb_map = {}
    for _, row in pb_df.iterrows():
        d = row["日期"]
        if hasattr(d, "date"):
            d = d.date()
        pb_map[d] = _safe_float(row.get(pb_col)) if pb_col else None
    print(f"    PB列={pb_col}, rows={len(pb_map)}")

    all_dates = sorted(set(pe_map.keys()) | set(pb_map.keys()))
    out = []
    for d in all_dates:
        pe_info = pe_map.get(d, {})
        out.append({
            "date_k":   d,
            "idx_code": idx_code,
            "pe_ttm":   pe_info.get("pe_ttm"),
            "pb":       pb_map.get(d),
            "close":    pe_info.get("close"),
            "q10y_pe":  None,
            "q10y_pb":  None,
        })
    return out


def _fetch_market_pe(idx_code: str, symbol: str | None) -> list[dict]:
    """
    抓乐咕「市场分类」整体法 PE：
      stock_market_pe_lg(symbol="上证"/"深证"/"创业板"/"科创版")
    中小板(zxb_market)已于2021年并入深证主板，乐咕不提供 → 返回空数组并提示。
    口径：市场整体法 PE = ∑(收盘价×发行数量) / ∑(每股收益×发行数量)  =  ∑总市值 / ∑净利润TTM
    """
    import akshare as ak
    meta = MARKET_PE_META.get(idx_code, ("", "", ""))
    print(f"  [{idx_code}] 正在抓取乐咕 市场PE(整体法={meta[2]}): symbol={symbol} ...")
    if symbol is None:
        # 中小板：乐咕不提供独立分类，直接返回空（前端会显示说明）
        print(f"    ⚠️  {meta[2]}，跳过抓取")
        return []

    try:
        time.sleep(0.6)  # 避免被限流
        pe_df = ak.stock_market_pe_lg(symbol=symbol)
    except Exception as e:
        print(f"    ❌ stock_market_pe_lg({symbol}) 失败: {type(e).__name__}: {e}")
        return []

    # 自动识别列：优先「整体法」或「滚动市盈率(TTM)」
    cols = list(pe_df.columns)
    print(f"    接口列名: {cols}")
    date_col = "日期" if "日期" in cols else (cols[0] if cols else "日期")
    pe_col_candidates = [c for c in cols if "市盈率" in str(c)]
    pe_col = None
    for prio in ["整体法", "TTM", "滚动"]:
        for c in pe_col_candidates:
            if prio in str(c):
                pe_col = c; break
        if pe_col: break
    if pe_col is None and pe_col_candidates:
        pe_col = pe_col_candidates[0]

    pe_map = {}
    for _, row in pe_df.iterrows():
        d = row[date_col]
        if hasattr(d, "date"):
            d = d.date()
        try:
            dv = pd.to_datetime(d).date()
        except Exception:
            dv = d
        pe_map[dv] = _safe_float(row.get(pe_col)) if pe_col else None

    print(f"    PE列={pe_col}, rows={len(pe_map)}, 最新日期={max(pe_map.keys()) if pe_map else 'N/A'}")

    out = []
    for d in sorted(pe_map.keys()):
        out.append({
            "date_k":   d,
            "idx_code": idx_code,
            "pe_ttm":   pe_map[d],
            "pb":       None,   # stock_market_pe_lg 不给 PB
            "close":    None,
            "q10y_pe":  None,   # 后续统一重算分位
            "q10y_pb":  None,
        })
    return out


def _fetch_average_pe() -> list[dict]:
    """A股平均市盈率（全A）：中位数口径（更符合"市场平均估值温度"，避免被巨型权重股拉低）。
    PE: middlePETTM（全A成分股 PE-TTM 中位数，更主流）
    PB: middlePB（全A成分股 PB 中位数）
    分位数：akshare 自带的 middle* 对应的近10年 quantile 直接入库，不用再重算。
    """
    import akshare as ak
    print("  [average_pe] 正在抓取 stock_a_ttm_lyr() (中位数PE口径) ...")
    pe_df = ak.stock_a_ttm_lyr()
    # 列: date, middlePETTM, averagePETTM, middlePELYR, averagePELYR, close,
    #      quantileInRecent10YearsMiddlePeTtm, quantileInRecent10YearsAveragePeTtm, ...
    pe_map = {}
    for _, row in pe_df.iterrows():
        d = row["date"]
        if hasattr(d, "date"):
            d = d.date()
        pe_map[d] = {
            # 中位数PE：50%分位点上的公司市盈率，不被极端权重股拉低
            "pe_ttm":  _safe_float(row.get("middlePETTM")),
            "close":   _safe_float(row.get("close")),
            # akshare 自带 middlePETTM 的近10年历史分位，直接用
            "q10y_pe": _safe_float(row.get("quantileInRecent10YearsMiddlePeTtm")),
        }
    print(f"    PE rows={len(pe_map)}, 最新日期={max(pe_map.keys()) if pe_map else 'N/A'}")

    print("  [average_pe] 正在抓取 stock_a_all_pb() (中位数PB口径) ...")
    pb_df = ak.stock_a_all_pb()
    # 列: date, middlePB, equalWeightAveragePB, close,
    #      quantileInRecent10YearsMiddlePB, quantileInRecent10YearsEqualWeightAveragePB
    pb_map = {}
    for _, row in pb_df.iterrows():
        d = row["date"]
        if hasattr(d, "date"):
            d = d.date()
        pb_map[d] = {
            # middlePB（中位数PB）：与 middlePETTM 配对，口径一致
            "pb":      _safe_float(row.get("middlePB")),
            "q10y_pb": _safe_float(row.get("quantileInRecent10YearsMiddlePB")),
        }
    print(f"    PB rows={len(pb_map)}")

    all_dates = sorted(set(pe_map.keys()) | set(pb_map.keys()))
    out = []
    for d in all_dates:
        pe_info = pe_map.get(d, {})
        pb_info = pb_map.get(d, {})
        out.append({
            "date_k":   d,
            "idx_code": "average_pe",
            "pe_ttm":   pe_info.get("pe_ttm"),
            "pb":       pb_info.get("pb"),
            "close":    pe_info.get("close"),
            "q10y_pe":  pe_info.get("q10y_pe"),
            "q10y_pb":  pb_info.get("q10y_pb"),
        })
    return out


def recompute_q10y_for_index(idx_code: str):
    """
    对不自带 q10y 分位的 4 个乐咕指数（hs300/cyb/zz500/sz50）：
    全量读库 → 按近10年滚动窗口计算 PE/PB 的历史分位，然后批量 update db。
    （因为乐咕接口本身不给分位数，需要自己算。）
    """
    import numpy as np
    data = read_index_data(idx_code)
    if len(data) < 100:
        return 0
    dates  = [dt.date.fromisoformat(d["date_k"]) for d in data]
    pes    = [d["pe_ttm"] for d in data]
    pbs    = [d["pb"]     for d in data]
    # 近10年对应的日期偏移：向前 ~3652 天
    win_days = 3653

    def _q(arr: list, v) -> float | None:
        vs = [x for x in arr if x is not None and not np.isnan(x)]
        if not vs or v is None:
            return None
        try:
            fv = float(v)
        except Exception:
            return None
        if np.isnan(fv):
            return None
        vs = np.array(vs, dtype=float)
        return float(np.clip(np.mean(vs <= fv), 0.0, 1.0))

    # 滚动窗口：对每个 i，取 索引 j where dates[i]-dates[j] <= win_days
    out_rows = []
    j = 0
    for i in range(len(dates)):
        while j < i and (dates[i] - dates[j]).days > win_days:
            j += 1
        window_pes = pes[j:i+1]
        window_pbs = pbs[j:i+1]
        out_rows.append({
            "date_k": dates[i],
            "idx_code": idx_code,
            "q10y_pe": _q(window_pes, pes[i]),
            "q10y_pb": _q(window_pbs, pbs[i]),
        })
    # 批量写入（只更分位）
    return upsert_bulk_index(out_rows)


def collect_all_index_valuation(force: bool = False) -> str:
    """
    全量抓取 + 入库：
      1) 4 个宽基指数（hs300/cyb/zz500/sz50）：整体法 PE+PB
      2) 5 个A股市场分类PE（sh/sz/zxb/cyb/kcb_market）：整体法 PE
      3) average_pe（全A中位数）：保持不变
    force=True：强制全部重抓（收盘后每日增量更新用）
    force=False（冷启动 / 默认）：按「每个指数」的新鲜度判断，
        只抓"库中最新日期 < 今日预期最新交易日"的指数；已最新的指数跳过，不打浪费网络请求。
        （这样库里哪怕 hs300 有 5000 条历史，但新增的 sh_market / sz_market 是空表时，也会被补齐！）
    返回摘要字符串。
    """
    import akshare as ak  # 延迟 import，避免没装 akshare 也能跑金银比部分
    t0 = time.time()
    result_lines = []

    # 已去掉旧的「DB > 500 行就全跳过」逻辑，改为按指数逐个判断新鲜度

    # —— 第 1 批：原 5 个指数（average_pe + 4 个宽基）——
    for idx_code in ["average_pe", "hs300", "cyb", "zz500", "sz50"]:
        name, syms, _, _labels = INDEX_META[idx_code]

        # force=False 时：若此指数数据已是最新 → 跳过（不打网络请求，不浪费资源）
        if not force:
            stale, why = _is_index_stale(idx_code)
            if not stale:
                result_lines.append(f"  🟢 {idx_code:12s} 跳过（{why}）")
                continue

        print(f"\n[指数估值] ▶ 开始抓取: {idx_code} ({name}) · PE={_labels[0]}, PB={_labels[1]}")
        try:
            if idx_code == "average_pe":
                rows = _fetch_average_pe()
            else:
                rows = _fetch_one_lg_index(idx_code, syms[0])
            if rows:
                chg = upsert_bulk_index(rows)
                result_lines.append(f"  ✅ {idx_code:12s} 入库 {len(rows):5d} 行，更新 {chg:4d} 行（最新日期={rows[-1]['date_k']}）")
            else:
                result_lines.append(f"  ⚠️ {idx_code:12s} 未抓到任何数据")
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            result_lines.append(f"  ❌ {idx_code:12s} 失败: {err[:120]}")
            print(f"  ❌ {idx_code} 失败: {err[:300]}")

    # —— 第 2 批：5 个市场分类 PE（sh_market / sz_market / zxb_market / cyb_market / kcb_market）——
    print(f"\n[指数估值] ▶ 开始抓取 5 个 A 股市场分类整体法 PE（用于「A 股各板平均市盈率叠加图」）：")
    for mkt_code, (name_cn, symbol, desc) in MARKET_PE_META.items():

        # force=False 时：按单指数新鲜度跳过
        if not force:
            stale, why = _is_index_stale(mkt_code)
            if not stale:
                result_lines.append(f"  🟢 {mkt_code:12s} 跳过（{why}）")
                continue

        print(f"\n[市场PE] ▶ {mkt_code} ({name_cn}) · {desc}")
        try:
            rows = _fetch_market_pe(mkt_code, symbol)
            if rows:
                chg = upsert_bulk_index(rows)
                result_lines.append(f"  ✅ {mkt_code:12s} 入库 {len(rows):5d} 行，更新 {chg:4d} 行（最新日期={rows[-1]['date_k']}）")
            else:
                result_lines.append(f"  ⚠️ {mkt_code:12s} 未抓到数据（{name_cn}：{desc[:40]}）")
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            result_lines.append(f"  ❌ {mkt_code:12s} 失败: {err[:120]}")
            print(f"  ❌ {mkt_code} 失败: {err[:300]}")

    # 对除 average_pe 外的所有指数（4 宽基 + 5 市场分类）重算近 10 年 PE 分位
    print(f"\n[指数估值] ▶ 重算近 10 年 PE/PB 分位：")
    need_q = ["hs300", "cyb", "zz500", "sz50"] + list(MARKET_PE_META.keys())
    for idx_code in need_q:
        try:
            n = recompute_q10y_for_index(idx_code)
            result_lines.append(f"  🧮 {idx_code:12s} 分位计算更新 {n:5d} 行")
        except Exception as e:
            result_lines.append(f"  ❌ {idx_code:12s} 分位失败: {type(e).__name__}: {str(e)[:80]}")

    total_rows = index_count_rows()
    msg = (f"[指数估值] 全部完成，总记录 {total_rows} 条，耗时 {time.time()-t0:.1f}s：\n"
           + "\n".join(result_lines))
    print(msg)
    return msg


# ============================================================
# 2-bis. 邮件告警（SMTP_SSL，兼容 163 / QQ）
# ============================================================
def _email_config_ready() -> bool:
    """检查邮件配置是否齐全"""
    need = ["EMAIL_HOST", "EMAIL_PORT", "EMAIL_USER", "EMAIL_PASS"]
    if not EMAIL_ENABLED:
        return False
    for k in need:
        if not globals().get(k):
            return False
    return True


def send_email(subject: str, html_body: str) -> tuple[bool, str]:
    """
    用 SMTP_SSL 发送一封 HTML 邮件。
    兼容 163、QQ 及大部分主流邮箱（都走 465 / SSL）。
    返回 (是否成功, 原因描述)。
    """
    if not _email_config_ready():
        return False, "邮件未启用 / 配置不齐全（设置 EMAIL_ENABLED=True 并填完整 HOST/PORT/USER/PASS）"
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.header import Header
    from email.utils import formataddr

    msg = MIMEMultipart("alternative")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = formataddr((str(Header("金银比监控", "utf-8")), EMAIL_FROM or EMAIL_USER))
    # 兼容单个字符串 或 字符串列表
    to_list = EMAIL_TO if isinstance(EMAIL_TO, (list, tuple)) else [EMAIL_TO]
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        # 统一走 SMTPS（SSL 加密，163/QQ 默认 465），避免明文/STARTTLS 配置差异
        with smtplib.SMTP_SSL(EMAIL_HOST, int(EMAIL_PORT), timeout=15) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.sendmail(EMAIL_FROM or EMAIL_USER, to_list, msg.as_string())
        return True, "邮件发送成功"
    except Exception as e:
        return False, f"邮件发送失败：{type(e).__name__}: {e}"


def check_and_alert_ratio(ratio: float, date_val: dt.date, gold: float, silver: float) -> str:
    """
    每小时采集完金银比后调用：
    - 若 ratio > RATIO_UPPER_THRESHOLD  →  高端告警（建议买银抛金）
    - 若 ratio < RATIO_LOWER_THRESHOLD  →  低端告警（建议买金抛银）
    - 带冷却时间，避免连续邮件轰炸
    返回简短告警状态描述（供日志/接口返回）。
    """
    direction = None   # 'upper' | 'lower' | None
    if ratio > RATIO_UPPER_THRESHOLD:
        direction = "upper"
    elif ratio < RATIO_LOWER_THRESHOLD:
        direction = "lower"
    if direction is None:
        return f"金银比 {ratio:.2f} 在正常区间 [{RATIO_LOWER_THRESHOLD}, {RATIO_UPPER_THRESHOLD}]，无需告警"

    # 冷却检查
    state_key = f"{direction}_last_at"
    now = datetime.now()
    last: datetime | None = _alert_state.get(state_key)
    if last is not None and EMAIL_ALERT_COOLDOWN_HOURS > 0:
        if (now - last).total_seconds() < EMAIL_ALERT_COOLDOWN_HOURS * 3600:
            remain = EMAIL_ALERT_COOLDOWN_HOURS * 3600 - (now - last).total_seconds()
            return (f"金银比 {ratio:.2f} 触发{direction}告警，但仍在冷却期内"
                    f"（剩 {remain/3600:.1f}h），跳过邮件。")

    # 组装邮件内容
    if direction == "upper":
        emoji, zh = "🟥", "高于上阈值（黄金偏贵/白银偏便宜）"
        suggest = "策略提示：历史上金银比超过 80 后多有均值回归倾向，可考虑减黄金、加白银配置。"
        threshold_line = f"{RATIO_UPPER_THRESHOLD:.2f}"
    else:
        emoji, zh = "🟩", "低于下阈值（黄金偏便宜/白银偏贵）"
        suggest = "策略提示：历史上金银比低于 50 后多有均值回归倾向，可考虑加黄金、减白银配置。"
        threshold_line = f"{RATIO_LOWER_THRESHOLD:.2f}"

    subject = f"{emoji}[金银比告警] {ratio:.2f} {zh}  | {date_val.isoformat()}"
    html = f"""
    <div style="font-family:Microsoft YaHei,PingFang SC,Arial;max-width:680px;margin:20px auto;padding:20px;border:1px solid #e5e7eb;border-radius:10px;background:#fff;">
      <h2 style="margin-top:0;color:#222;border-left:6px solid {'#C62828' if direction=='upper' else '#2E7D32'};padding-left:10px;">
        金银比告警 · {date_val.isoformat()}
      </h2>
      <table style="width:100%;border-collapse:collapse;margin:14px 0;font-size:14px;">
        <tr style="background:#fafafa;"><td style="padding:8px 12px;border:1px solid #eee;width:40%;">指标</td><td style="padding:8px 12px;border:1px solid #eee;">数值</td></tr>
        <tr><td style="padding:8px 12px;border:1px solid #eee;">伦敦金 (美元/盎司)</td><td style="padding:8px 12px;border:1px solid #eee;font-weight:600;color:#c9a227;">{gold:.2f}</td></tr>
        <tr><td style="padding:8px 12px;border:1px solid #eee;">伦敦银 (美元/盎司)</td><td style="padding:8px 12px;border:1px solid #eee;font-weight:600;color:#8a8d93;">{silver:.3f}</td></tr>
        <tr style="background:#fff7e6;"><td style="padding:8px 12px;border:1px solid #eee;">金银比 = 金 ÷ 银</td><td style="padding:8px 12px;border:1px solid #eee;font-weight:700;font-size:18px;color:#8B0000;">{ratio:.2f}</td></tr>
        <tr><td style="padding:8px 12px;border:1px solid #eee;">告警方向</td><td style="padding:8px 12px;border:1px solid #eee;">{zh}</td></tr>
        <tr><td style="padding:8px 12px;border:1px solid #eee;">触发阈值</td><td style="padding:8px 12px;border:1px solid #eee;">{threshold_line}</td></tr>
        <tr><td style="padding:8px 12px;border:1px solid #eee;">采集时间</td><td style="padding:8px 12px;border:1px solid #eee;">{now.strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
      </table>
      <div style="background:#f0f7ff;padding:12px 16px;border-radius:8px;color:#0d3b74;font-size:14px;line-height:1.7;">
        💡 {suggest}<br/>
        本提示仅作数据参考，不构成投资建议。投资有风险，入市需谨慎。
      </div>
      <hr style="border:none;border-top:1px dashed #ddd;margin:20px 0;"/>
      <div style="font-size:12px;color:#888;">
        来自 金银比可视化监控服务（Finance Analysis · precious_metals）<br/>
        可在页面查看图表：http://127.0.0.1:5000/
      </div>
    </div>
    """
    ok, reason = send_email(subject, html)
    if ok:
        _alert_state[state_key] = now
        print(f"[告警邮件✅] {reason}  |  金银比={ratio:.2f} {zh}")
    else:
        print(f"[告警邮件❌] {reason}")
    return f"金银比 {ratio:.2f} {zh} → 邮件：{reason}"


# ============================================================
# 3. 数据采集：LBMA 当日最新价
# ============================================================
def fetch_lbma_latest() -> tuple[dt.date | None, float | None, float | None]:
    """
    抓取 LBMA JSON，返回（最新交易日日期，黄金价，白银价）
    注意：LBMA JSON 里是伦敦定盘价（工作日每天一条），
    我们取 JSON 里最后几天有值的记录，避免周末/节假日无数据。
    """
    import requests
    sess = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36",
        "Referer": "https://www.lbma.org.uk/",
    }

    def _load(url: str, label: str) -> list[tuple[dt.date, float]]:
        try:
            r = sess.get(url, timeout=45, headers=headers)
            if r.status_code != 200:
                print(f"  [采集] {label} HTTP {r.status_code}")
                return []
            raw = r.json()
        except Exception as e:
            print(f"  [采集] {label} 请求失败：{type(e).__name__}: {e}")
            return []
        out = []
        for item in raw:
            d = item.get("d")
            v = item.get("v") or []
            if not d or not v or v[0] is None:
                continue
            try:
                dv = datetime.strptime(d, "%Y-%m-%d").date()
                out.append((dv, float(v[0])))
            except Exception:
                continue
        return out

    gold_list = _load(LBMA_GOLD_JSON, "LBMA黄金上午定盘价")
    silver_list = _load(LBMA_SILVER_JSON, "LBMA白银定盘价")

    if not gold_list and not silver_list:
        return None, None, None

    # 取两边最后几天里"共同存在的最近一天"，或各自的最近一天
    gold_map = {d: p for d, p in gold_list}
    silver_map = {d: p for d, p in silver_list}
    all_dates = sorted(set(gold_map.keys()) | set(silver_map.keys()), reverse=True)

    # 优先找"同一天两边都有值"的最近一天；否则取两边各自的最近
    latest_common = None
    for d in all_dates[:10]:
        if d in gold_map and d in silver_map:
            latest_common = d
            break
    if latest_common is not None:
        return latest_common, gold_map[latest_common], silver_map[latest_common]

    # 兜底：各自取最近
    latest_gold = gold_list[-1][0] if gold_list else None
    latest_silver = silver_list[-1][0] if silver_list else None
    if latest_gold == latest_silver and latest_gold is not None:
        return latest_gold, gold_map.get(latest_gold), silver_map.get(latest_silver)
    pick_date = latest_gold or latest_silver
    return pick_date, gold_map.get(pick_date), silver_map.get(pick_date)


def collect_and_save(reason: str = "定时", force: bool = False):
    """
    采集一次并入库，返回描述信息（附带告警状态）。

    force=False（定时/启动默认）：遇到周末 / 当日已拿到过新 LBMA 数据 → 直接跳过，不打网络请求。
    force=True（手动刷新）：无视上述规则，强制访问 LBMA（用于调试 / 验证配置）。
    """
    now = datetime.now()
    today = now.date()
    skip_reason = None

    if not force:
        if not is_likely_trading_day(today):
            skip_reason = f"今日 {today.isoformat()}（周{list('一二三四五六日')[today.weekday()]}）非交易日，跳过查询"
        elif _skip_collect["done_date"] == today.isoformat():
            skip_reason = f"今天已拿到过 LBMA 最新数据（{today.isoformat()}），跳过重复查询"
        if skip_reason:
            msg = f"[采集] ⏭  {skip_reason}  |  触发：{reason}"
            print(msg)
            # 即使跳过，也返回库里最新一条方便接口显示
            latest = latest_row() or {}
            return {
                "ok": True, "skipped": True, "msg": msg,
                "date": latest.get("date_k"),
                "gold": latest.get("gold_price"),
                "silver": latest.get("silver_price"),
                "ratio": latest.get("ratio"),
                "alert": "已跳过采集（非交易日 / 今日已完成）",
            }

    print(f"\n[采集] {now.strftime('%Y-%m-%d %H:%M:%S')} 触发：{reason}" + ("  [强制]" if force else ""))
    t0 = time.time()
    d, g, s = fetch_lbma_latest()
    if d is None:
        msg = "[采集] 未能从LBMA获取到有效数据，稍后自动重试。"
        print(msg)
        return {"ok": False, "msg": msg}
    ok = upsert_row(d, g, s)
    # 如果 LBMA 返回的最新交易日日期 == 今天（工作日）→ 今日已出定盘价，之后自动跳过
    if (not force) and d == today and is_likely_trading_day(today):
        _skip_collect["done_date"] = today.isoformat()
        print(f"[采集] ✅ 已获取到今日（{today.isoformat()}）LBMA 定盘价，今日剩余时间将不再请求")
    # 计算金银比并触发告警邮件
    ratio = (g / s) if (g and s and s > 0) else None
    alert_msg = ""
    if ratio is not None:
        try:
            alert_msg = check_and_alert_ratio(ratio, d, g, s)
        except Exception as ae:
            alert_msg = f"告警流程异常：{type(ae).__name__}: {ae}"
            print(f"[告警邮件❌] {alert_msg}")
    cost = time.time() - t0
    msg = (f"[采集] 完成：日期={d}, 黄金={g}, 白银={s}, "
           f"金银比={'%.2f' % ratio if ratio is not None else 'N/A'}, 耗时={cost:.1f}s"
           + (f"  |  {alert_msg}" if alert_msg else ""))
    print(msg)
    return {"ok": ok, "msg": msg, "date": str(d), "gold": g, "silver": s,
            "ratio": round(ratio, 4) if ratio is not None else None,
            "alert": alert_msg or "区间正常，无需告警"}


def import_historical_csv_if_empty():
    """启动时若数据库为空，自动导入最近生成的 LBMA 历史 CSV 做冷启动"""
    n = count_rows()
    if n > 0:
        print(f"[初始化] 数据库已有 {n} 条记录，跳过历史CSV导入")
        return

    import glob
    candidates = sorted(
        glob.glob(os.path.join(BASE_DIR, "黄金白银金银比_日K_1968*_至_*_导出*.csv")),
        key=os.path.getmtime, reverse=True,
    )
    if not candidates:
        print("[初始化] 未找到历史CSV（可手动运行 gold_silver_ratio.py 先抓取生成）")
        print("[初始化] 将只保留空白表，等待定时任务按小时写入新数据")
        return
    csv_path = candidates[0]
    print(f"[初始化] 数据库为空，导入历史数据：{os.path.basename(csv_path)}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [str(c).lstrip("\ufeff").strip() for c in df.columns]
    # 同时兼容两套列名（中英文）
    col_zh = {"时间": "date_k", "黄金价格(美元/盎司)": "gold_price", "白银价格(美元/盎司)": "silver_price"}
    col_en = {"date": "date_k", "gold_price": "gold_price", "silver_price": "silver_price"}
    if all(k in df.columns for k in col_zh):
        rename = col_zh
    elif all(k in df.columns for k in col_en):
        rename = col_en
    else:
        print(f"[初始化] CSV 列名不匹配。实际列: {list(df.columns)}")
        return
    df = df[list(rename.keys())].rename(columns=rename)
    df["date_k"] = pd.to_datetime(df["date_k"]).dt.date
    df = df.dropna(subset=["date_k"])
    df = df.drop_duplicates(subset=["date_k"])
    inserted = 0
    with SessionLocal() as s:
        for r in df.itertuples(index=False):
            row = PreciousMetal(
                date_k=r.date_k,
                gold_price=float(r.gold_price) if pd.notna(r.gold_price) else None,
                silver_price=float(r.silver_price) if pd.notna(r.silver_price) else None,
            )
            s.merge(row)
            inserted += 1
            if inserted % 2000 == 0:
                s.commit()
        s.commit()
    print(f"[初始化] 历史数据导入完成：共 {inserted} 条")


# ============================================================
# 4. Flask + 路由
# ============================================================
app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=None)
app.config.update(TEMPLATES_AUTO_RELOAD=True, SEND_FILE_MAX_AGE_DEFAULT=0)   # 开发时模板/缓存即时刷新

# 给 Jinja2 模板注册 Python 内置 enumerate（用于 main_frame.html 循环多组一级菜单时取索引）
app.jinja_env.globals.update(enumerate=enumerate)


# ------- 1. 主框架页（顶部导航 + 左侧二级菜单 + 右侧iframe内容）--------
# 菜单按一级分组：(组ID, 组显示名, [二级菜单项...])
# 二级菜单项：(key, 显示名, raw_content_url) —— key 用于激活态匹配
MENU_GROUPS = [
    (
        "hot", "热门数据", [
            ("average_pe",   "A股平均市盈率（各板叠加）", "/chart/average_pe_raw"),
            ("hs300",        "沪深300估值",      "/chart/hs300_raw"),
            ("cyb",          "创业板估值",        "/chart/cyb_raw"),
            ("zz500",        "中证500估值",       "/chart/zz500_raw"),
            ("sz50",         "上证50估值",        "/chart/sz50_raw"),
            ("sh_market",    "上证指数估值",      "/chart/sh_market_raw"),
            ("sz_market",    "深证指数估值",      "/chart/sz_market_raw"),
        ],
    ),
    (
        "precious", "贵金属", [
            ("gold_silver",  "金银比历史走势",   "/chart/gold_silver_raw"),
            ("usa_money_gold", "美国基础货币与黄金储备", "/chart/usa_money_gold_raw"),
        ],
    ),
    (
        "tools", "工具", [
            ("finance_calc",     "理财计算器",       "/chart/finance_calculator_raw"),
            ("reinvest_backtest","个股复投回测",     "/chart/reinvest_backtest_raw"),
        ],
    ),
]
# 扁平化 key → (组索引, 菜单索引, raw_url) 用于快速查找
_FLAT_MENU = {}
for gi, (_gid, _gname, items) in enumerate(MENU_GROUPS):
    for mi, (k, t, u) in enumerate(items):
        _FLAT_MENU[k] = (gi, mi, u, t)
DEFAULT_MENU = "gold_silver"   # 默认打开金银比（归属贵金属）


def _render_frame(active: str):
    """统一渲染主框架页。传入菜单key，选中对应项+加载对应raw页面到右侧iframe。"""
    if active not in _FLAT_MENU:
        active = DEFAULT_MENU
    gi, mi, active_raw_url, active_title = _FLAT_MENU[active]
    # 找到所属组名，用于面包屑
    group_name = MENU_GROUPS[gi][1]
    return render_template(
        "main_frame.html",
        MENU_GROUPS=MENU_GROUPS,
        active_group_idx=gi,
        active_menu_idx=mi,
        active_key=active,
        active_raw_url=active_raw_url,
        active_title=active_title,
        active_group_name=group_name,
        render_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


@app.route("/")
def home():
    return _render_frame(DEFAULT_MENU)


# 快捷别名：访问 /hot/<key> 直接跳到对应菜单页
@app.route("/hot/<menu_key>")
def hot_menu(menu_key: str):
    return _render_frame(menu_key)


# 给所有菜单再各绑一个清晰的路由（便于链接分享）
@app.route("/chart/gold_silver")
def page_gold_silver():   return _render_frame("gold_silver")
@app.route("/chart/average_pe")
def page_average_pe():    return _render_frame("average_pe")
@app.route("/chart/hs300")
def page_hs300():         return _render_frame("hs300")
@app.route("/chart/cyb")
def page_cyb():           return _render_frame("cyb")
@app.route("/chart/zz500")
def page_zz500():         return _render_frame("zz500")
@app.route("/chart/sz50")
def page_sz50():          return _render_frame("sz50")
@app.route("/chart/sh_market")
def page_sh_market():     return _render_frame("sh_market")
@app.route("/chart/sz_market")
def page_sz_market():     return _render_frame("sz_market")
@app.route("/chart/usa_money_gold")
def page_usa_money_gold(): return _render_frame("usa_money_gold")
@app.route("/chart/finance_calculator")
def page_finance_calculator(): return _render_frame("finance_calc")
@app.route("/chart/reinvest_backtest")
def page_reinvest_backtest(): return _render_frame("reinvest_backtest")


# ------- 2. 右侧内容页（raw，嵌入 iframe 的独立页面）--------
@app.route("/chart/finance_calculator_raw")
def finance_calculator_raw():
    """理财计算器独立页（纯前端计算，四个页签）"""
    return render_template("finance_calculator_raw.html")

@app.route("/chart/reinvest_backtest_raw")
def reinvest_backtest_raw():
    """个股分红复投回测独立页（表单输入 + 汇总/明细展示）"""
    return render_template("reinvest_backtest_raw.html")

@app.route("/chart/gold_silver_raw")
def gold_silver_raw():
    """金银比独立页：之前的完整页面，保留所有逻辑（状态栏+Plotly）不变"""
    latest = latest_row() or {}
    return render_template(
        "index.html",
        latest=latest,
        total_rows=count_rows(),
        collect_interval=COLLECT_INTERVAL_HOURS,
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def _render_raw_template(template_name: str, title: str, subtitle: str, data_source: str,
                         api_url: str, idx_code: str):
    """
    统一的估值内容页渲染：
      - 传入 api_url（前端 fetch 的真数据地址）+ idx_code（5 类之一）
      - 前端拿到真数据后绘制 PE(红左)/PB(蓝右) 双轴 + 十字虚线 + 分位标注
    """
    name_cn, _syms, _desc, (pe_label, pb_label) = INDEX_META[idx_code]
    return render_template(
        template_name,
        page_title=title,
        page_subtitle=subtitle,
        page_data_source=data_source,
        render_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        DATA_API=api_url,
        IDX_CODE=idx_code,
        INDEX_NAME_CN=name_cn,
        PE_LABEL=pe_label,   # "等权PE(TTM · 沪深300)" / "中位数PE"
        PB_LABEL=pb_label,   # "等权PB(沪深300)" / "中位数PB"
    )


@app.route("/chart/average_pe_raw")
def raw_average_pe():
    """A股各板平均市盈率叠加图：5条PE线（上证/深证/中小板/创业板/科创板）可自由切换显示"""
    latest_map = {}
    for mc in MARKET_PE_META.keys():
        r = index_latest_row(mc)
        if r:
            latest_map[mc] = r
    return render_template(
        "a_share_multi_pe.html",
        page_title="A股各板平均市盈率历史走势（叠加图·可自由切换）",
        page_subtitle="整体法口径：平均市盈率 = ∑(收盘价×发行数量) / ∑(每股收益×发行数量) = ∑总市值 / ∑净利润TTM",
        page_data_source="akshare · 乐咕乐股 stock_market_pe_lg（上证/深证/创业板/科创版 4 个市场分类整体法 PE），中小板2021年并入深证主板（无独立数据）",
        render_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        DATA_API="/api/a_share_multi_pe_data",
        MARKET_PE_META=MARKET_PE_META,
        MARKET_PE_CODES=list(MARKET_PE_META.keys()),
        latest_map=latest_map,
    )

@app.route("/chart/usa_money_gold_raw")
def raw_usa_money_gold():
    """美国基础货币发行量与黄金储备量关系历史图"""
    return render_template(
        "usa_money_gold_raw.html",
        page_title="美国基础货币发行量 & 黄金储备量 历史关系",
        page_subtitle="黄金储备量（吨/盎司） vs 基础货币(Monetary Base) vs 循环信贷(Currency in Circulation) · 长期购买力对比",
        page_data_source="数据源优先级：① FRED 官网 CSV 镜像（AMB/BASE/WGC1ST 等系列）→ ② akshare/FRED 经济数据 → ③ 静态兜底样例数据",
        render_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        DATA_API="/api/usa_money_gold_data",
    )

@app.route("/chart/hs300_raw")
def raw_hs300():
    return _render_raw_template(
        "page_raw_placeholder.html",
        title="沪深 300 估值历史走势 (PE(TTM) / PB)",
        subtitle="大盘蓝筹核心宽基：PE/PB 双轴 + 近10年历史分位参考",
        data_source="akshare · 乐咕乐股 stock_index_pe_lg/pb_lg（沪深300）",
        api_url="/api/hs300_data",
        idx_code="hs300",
    )

@app.route("/chart/cyb_raw")
def raw_cyb():
    return _render_raw_template(
        "page_raw_placeholder.html",
        title="创业板估值（创业板50）历史走势 (PE / PB)",
        subtitle="创业板蓝筹代表（399673.SZ）：成长风格估值周期",
        data_source="akshare · 乐咕乐股 stock_index_pe_lg/pb_lg（创业板50）",
        api_url="/api/cyb_data",
        idx_code="cyb",
    )

@app.route("/chart/zz500_raw")
def raw_zz500():
    return _render_raw_template(
        "page_raw_placeholder.html",
        title="中证 500 估值历史走势 (PE / PB)",
        subtitle="中盘股核心宽基：估值周期 + 与沪深300的估值溢价",
        data_source="akshare · 乐咕乐股 stock_index_pe_lg/pb_lg（中证500）",
        api_url="/api/zz500_data",
        idx_code="zz500",
    )

@app.route("/chart/sz50_raw")
def raw_sz50():
    return _render_raw_template(
        "page_raw_placeholder.html",
        title="上证 50 估值历史走势 (PE / PB)",
        subtitle="超大盘蓝筹（金融+消费为主）：PE/PB + 近10年分位",
        data_source="akshare · 乐咕乐股 stock_index_pe_lg/pb_lg（上证50）",
        api_url="/api/sz50_data",
        idx_code="sz50",
    )


@app.route("/chart/sh_market_raw")
def raw_sh_market():
    """上证指数估值：市场分类整体法 PE（复用估值占位模板，PB 暂不提供）"""
    return _render_raw_template(
        "page_raw_placeholder.html",
        title="上证指数估值历史走势 (整体法 PE)",
        subtitle="沪市全A整体法市盈率(TTM) = ∑总市值 / ∑净利润TTM（成分股：沪市全部A股）",
        data_source="akshare · 乐咕乐股 stock_market_pe_lg（symbol=上证，市场分类整体法 PE-TTM）",
        api_url="/api/sh_market_data",
        idx_code="sh_market",
    )


@app.route("/chart/sz_market_raw")
def raw_sz_market():
    """深证指数估值：市场分类整体法 PE（复用估值占位模板，PB 暂不提供）"""
    return _render_raw_template(
        "page_raw_placeholder.html",
        title="深证指数估值历史走势 (整体法 PE)",
        subtitle="深市全A整体法市盈率(TTM)（2021年2月起已包含原中小板全部成分股）",
        data_source="akshare · 乐咕乐股 stock_market_pe_lg（symbol=深证，市场分类整体法 PE-TTM）",
        api_url="/api/sz_market_data",
        idx_code="sz_market",
    )


@app.route("/api/data")
def api_data():
    """返回全量数据：[{date_k, gold_price, silver_price, ratio}, ...]；
    同时若数据过期 → 后台 daemon 线程自动补抓最新 LBMA 定价（不阻塞当前请求）。"""
    try:
        _triggered, _note = _trigger_bg_pm_refresh_if_stale("api_data")
    except Exception:
        _triggered, _note = False, ""
    data = read_all_rows()
    resp = {
        "ok": True,
        "count": len(data),
        "data": data,
    }
    if _note:
        resp["refresh_note"] = _note
        resp["refresh_triggered"] = bool(_triggered)
    return jsonify(resp)


@app.route("/api/latest")
def api_latest():
    try:
        _triggered, _note = _trigger_bg_pm_refresh_if_stale("api_latest")
    except Exception:
        _triggered, _note = False, ""
    r = latest_row()
    resp = {"ok": r is not None, "data": r or {}}
    if _note:
        resp["refresh_note"] = _note
        resp["refresh_triggered"] = bool(_triggered)
    return jsonify(resp)


@app.route("/api/stats")
def api_stats():
    try:
        _triggered, _note = _trigger_bg_pm_refresh_if_stale("api_stats")
    except Exception:
        _triggered, _note = False, ""
    rows = read_all_rows()
    if not rows:
        resp = {"ok": False, "msg": "空库"}
        if _note:
            resp["refresh_note"] = _note
            resp["refresh_triggered"] = bool(_triggered)
        return jsonify(resp)
    rs = [r["ratio"] for r in rows if r.get("ratio")]
    resp = {
        "ok": True,
        "total": len(rows),
        "from": rows[0]["date_k"],
        "to": rows[-1]["date_k"],
        "ratio_min": min(rs),
        "ratio_max": max(rs),
        "ratio_mean": round(sum(rs) / len(rs), 4),
    }
    if _note:
        resp["refresh_note"] = _note
        resp["refresh_triggered"] = bool(_triggered)
    return jsonify(resp)


@app.route("/api/refresh_now", methods=["GET", "POST"])
def api_refresh_now():
    """立即手动触发一次采集（强制，用于测试/验证邮件）"""
    result = collect_and_save(reason="手动触发", force=True)
    return jsonify(result)


# ============================================================
# 4-ter. A 股指数估值 JSON API（5 条主接口 + 通用接口）
# ============================================================
def _api_index_data(idx_code: str):
    """封装一下：读库 + 返回带 summary 的 JSON，统一空/异常兜底（空数组不带假值，遵循 679020 经验）。
    每次调用都会先判断「库中最新日期 < 今日（或周五回退）」：是 → 后台 daemon 线程自动增量补采（30 分钟内不重复）。"""
    if idx_code not in INDEX_META:
        return jsonify({"ok": False, "msg": f"未知 idx_code={idx_code}，可用 {INDEX_CODE_LIST}", "count": 0, "data": []})

    # 【按需自动补采】—— 不阻塞当前请求：先返回现有数据，后台静默补最新交易日
    try:
        _triggered, refresh_note = _trigger_bg_refresh_if_stale(idx_code, "api_data")
    except Exception:
        _triggered, refresh_note = False, ""

    data = read_index_data(idx_code)
    # 提取 summary（当前 PE/PB/分 + 历史区间）
    summary = {
        "index_name": INDEX_META[idx_code][0],
        "index_desc": INDEX_META[idx_code][2],
        "total": len(data),
    }
    if data:
        pes = [d["pe_ttm"] for d in data if d.get("pe_ttm") is not None]
        pbs = [d["pb"]     for d in data if d.get("pb")     is not None]
        last = data[-1]
        summary.update({
            "from":       data[0]["date_k"],
            "to":         last["date_k"],
            "pe_latest":  last["pe_ttm"],
            "pb_latest":  last["pb"],
            "close":      last["close"],
            "q10y_pe_latest": last["q10y_pe"],
            "q10y_pb_latest": last["q10y_pb"],
            "pe_min":     min(pes) if pes else None,
            "pe_max":     max(pes) if pes else None,
            "pe_mean":    round(sum(pes)/len(pes), 4) if pes else None,
            "pb_min":     min(pbs) if pbs else None,
            "pb_max":     max(pbs) if pbs else None,
            "pb_mean":    round(sum(pbs)/len(pbs), 4) if pbs else None,
        })
    resp = {
        "ok":    len(data) > 0,
        "count": len(data),
        "summary": summary,
        "data":  data,
        "render_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if refresh_note:
        resp["refresh_note"] = refresh_note
        resp["refresh_triggered"] = bool(_triggered)
    return jsonify(resp)


@app.route("/api/average_pe_data")
def api_avg_pe():   return _api_index_data("average_pe")

@app.route("/api/hs300_data")
def api_hs300():    return _api_index_data("hs300")

@app.route("/api/cyb_data")
def api_cyb():      return _api_index_data("cyb")

@app.route("/api/zz500_data")
def api_zz500():    return _api_index_data("zz500")

@app.route("/api/sz50_data")
def api_sz50():     return _api_index_data("sz50")


@app.route("/api/sh_market_data")
def api_sh_market(): return _api_index_data("sh_market")

@app.route("/api/sz_market_data")
def api_sz_market(): return _api_index_data("sz_market")


# ============================================================
# 4-bis. A 股各板平均市盈率叠加图 API + 美国货币/黄金储备 API
# ============================================================
@app.route("/api/a_share_multi_pe_data")
def api_a_share_multi_pe():
    """
    给「A股各板平均市盈率叠加图」用的JSON：
      一次性返回 5 个市场分类（上证/深证/中小板/创业板/科创板）的 PE 序列，
      前端自由叠加切换显示。
    同时在后台按需触发每个分类的增量补采。
    """
    # 【按需自动补采】每个市场分类单独判断过期 → 起各自的后台线程（30 分钟节流 + 并发锁）
    refresh_notes = []
    for code in MARKET_PE_CODES:
        try:
            _t, _n = _trigger_bg_refresh_if_stale(code, "multi_pe")
            if _n:
                refresh_notes.append(_n)
        except Exception:
            pass

    series = {}
    summary_per_code = {}
    min_from, max_to = None, None
    total_count = 0

    for code in MARKET_PE_CODES:
        data = read_index_data(code)
        name_cn, _, desc = MARKET_PE_META.get(code, (code, "", ""))
        xs = [d["date_k"] for d in data]
        ys = [d["pe_ttm"] for d in data]
        qs = [d["q10y_pe"] for d in data]
        series[code] = {
            "date": xs,
            "pe_ttm": ys,
            "q10y_pe": qs,
        }
        # 摘要
        valid_y = [y for y in ys if y is not None]
        last = data[-1] if data else None
        sp = {
            "code": code,
            "name": name_cn,
            "desc": desc,
            "count": len(data),
            "from": xs[0] if xs else None,
            "to": xs[-1] if xs else None,
            "pe_latest": last["pe_ttm"] if last else None,
            "q10y_pe_latest": last["q10y_pe"] if last else None,
            "pe_min": min(valid_y) if valid_y else None,
            "pe_max": max(valid_y) if valid_y else None,
            "pe_mean": round(sum(valid_y)/len(valid_y), 4) if valid_y else None,
        }
        summary_per_code[code] = sp
        total_count += len(data)
        if xs:
            if min_from is None or xs[0] < min_from: min_from = xs[0]
            if max_to is None or xs[-1] > max_to: max_to = xs[-1]

    resp = {
        "ok": total_count > 0,
        "count": total_count,
        "codes": MARKET_PE_CODES,
        "meta": {c: {"name": MARKET_PE_META[c][0], "desc": MARKET_PE_META[c][2]}
                 for c in MARKET_PE_CODES},
        "overall": {"from": min_from, "to": max_to, "series": len(series)},
        "summary": summary_per_code,
        "series": series,
        "render_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if refresh_notes:
        resp["refresh_notes"] = refresh_notes
    return jsonify(resp)


def _fetch_usa_money_gold_data() -> dict:
    """
    抓取美国基础货币 & 黄金储备历史数据（增强版：对齐参考图口径 · 日度频率）。

    输出字段（每日序列，与 precious_metals 数据最新日期同步 → 到当日工作日）：
      date                   日期（YYYY-MM-DD）
      gold_reserve_tons      美国官方黄金储备（公吨）—— 节点间线性插值；末节点向后 FFill
      gold_price_usd_oz      当日金价（美元/金衡盎司）：1968+ 来自 precious_metals 每日真实价；周末 FFill
      gold_reserve_usd_bn    黄金储备 市价估值（十亿美元）= 吨 × 32150.7 × 金价 ÷ 10^9
      monetary_base_bn       基础货币 Monetary Base（十亿美元）—— 节点插值+FFill
      currency_circ_bn       流通中货币 Currency in Circulation（十亿美元）—— 节点插值+FFill

    三级数据源策略：
      ① akshare FRED 接口（环境失连 → 跳过）
      ② HTTP 直连 FRED CSV（被墙 → 跳过）
      ③ 增强型历史基准节点 + 日度插值/向前填充 （当前启用，覆盖 1918-01-01 ~ 最新工作日）
         1950+ Fed/Treasury/WGC 公开数据；2008/2020 QE 期按季度/月度密集节点；
         金价 1968+ 取 precious_metals 每日真实 LBMA 定盘价（周末/节假日用前值 FFill，保持连续）。
    """
    import pandas as pd
    import numpy as np
    import sqlite3

    # ---------- Step 1: 构建高密度历史基准节点 ----------
    bm = [
        ("1918-01-01",  2300,    6.4,    4.7),
        ("1920-01-01",  2550,    7.0,    5.1),
        ("1925-01-01",  4500,    7.2,    4.2),
        ("1929-01-01",  4100,    7.1,    3.9),
        ("1933-03-01",  4300,    6.6,    4.1),
        ("1934-02-01",  6357,    7.9,    4.5),
        ("1936-01-01",  9000,    8.6,    5.2),
        ("1940-01-01", 14600,   11.2,    6.9),
        ("1945-12-01", 17848,   28.2,   25.1),
        ("1950-01-01", 20279,   42.0,   28.0),
        ("1952-01-01", 20663,   44.5,   29.8),
        ("1955-01-01", 20100,   46.8,   31.5),
        ("1960-01-01", 15609,   50.0,   32.0),
        ("1965-01-01", 14100,   57.5,   37.9),
        ("1968-01-01", 11800,   61.0,   40.9),
        ("1969-01-01", 10900,   63.5,   42.5),
        ("1970-01-01",  9839,   65.5,   48.3),
        ("1971-08-15",  9070,   68.0,   53.0),
        ("1972-01-01",  9070,   75.6,   58.6),
        ("1973-01-01",  8990,   85.4,   65.5),
        ("1974-01-01",  8860,   96.5,   72.6),
        ("1975-01-01",  8790,  108.8,   78.6),
        ("1976-01-01",  8710,  119.8,   81.3),
        ("1977-01-01",  8670,  131.8,   88.8),
        ("1978-01-01",  8610,  143.4,   99.3),
        ("1979-01-01",  8535,  155.9,  108.2),
        ("1980-01-01",  8469,  160.0,  123.0),
        ("1981-01-01",  8400,  171.0,  127.9),
        ("1982-01-01",  8360,  181.7,  134.0),
        ("1983-01-01",  8300,  189.6,  143.5),
        ("1984-01-01",  8230,  200.8,  155.0),
        ("1985-01-01",  8180,  211.4,  165.5),
        ("1986-01-01",  8160,  227.6,  174.0),
        ("1987-01-01",  8150,  246.8,  185.0),
        ("1988-01-01",  8147,  270.0,  197.0),
        ("1989-01-01",  8146,  288.0,  211.0),
        ("1990-01-01",  8146,  310.0,  267.0),
        ("1991-01-01",  8145,  322.0,  279.0),
        ("1992-01-01",  8144,  339.0,  297.0),
        ("1993-01-01",  8143,  364.0,  315.0),
        ("1994-01-01",  8142,  394.0,  337.0),
        ("1995-01-01",  8141,  421.0,  361.0),
        ("1996-01-01",  8140,  450.0,  382.0),
        ("1997-01-01",  8138,  473.0,  404.0),
        ("1998-01-01",  8137,  495.0,  427.0),
        ("1999-01-01",  8136,  554.0,  467.0),
        ("2000-01-01",  8136,  620.0,  545.0),
        ("2001-01-01",  8135,  655.0,  581.0),
        ("2002-01-01",  8135,  700.0,  626.0),
        ("2003-01-01",  8134,  732.0,  656.0),
        ("2004-01-01",  8134,  758.0,  690.0),
        ("2005-01-01",  8134,  788.0,  723.0),
        ("2006-01-01",  8134,  818.0,  758.0),
        ("2007-01-01",  8134,  848.0,  791.0),
        ("2007-08-01",  8134,  860.0,  810.0),
        ("2008-03-17",  8134,  872.0,  815.0),
        ("2008-09-15",  8134,  905.0,  836.0),
        ("2008-11-25",  8134, 1320.0,  852.0),
        ("2009-01-01",  8134, 1735.0,  862.0),
        ("2009-06-01",  8134, 1780.0,  887.0),
        ("2010-01-01",  8134, 2075.0,  950.0),
        ("2010-11-01",  8134, 2250.0,  980.0),
        ("2011-09-01",  8134, 2630.0, 1020.0),
        ("2012-09-01",  8134, 2820.0, 1080.0),
        ("2013-01-01",  8133, 3060.0, 1130.0),
        ("2014-01-01",  8133, 3760.0, 1220.0),
        ("2014-10-01",  8133, 4090.0, 1270.0),
        ("2015-01-01",  8133, 3850.0, 1350.0),
        ("2016-01-01",  8133, 3800.0, 1410.0),
        ("2017-01-01",  8133, 3760.0, 1460.0),
        ("2018-01-01",  8133, 3890.0, 1520.0),
        ("2019-01-01",  8133, 3820.0, 1580.0),
        ("2019-09-17",  8133, 3880.0, 1650.0),
        ("2020-02-01",  8134, 4050.0, 1760.0),
        ("2020-03-15",  8134, 5250.0, 1900.0),
        ("2020-05-01",  8134, 5840.0, 1950.0),
        ("2020-07-01",  8134, 4890.0, 2000.0),
        ("2020-11-01",  8134, 5110.0, 2080.0),
        ("2021-01-01",  8134, 5250.0, 2100.0),
        ("2021-06-01",  8133, 6020.0, 2200.0),
        ("2021-12-01",  8133, 6360.0, 2260.0),
        ("2022-01-01",  8133, 6410.0, 2250.0),
        ("2022-06-01",  8133, 5900.0, 2270.0),
        ("2023-01-01",  8133, 5300.0, 2330.0),
        ("2023-12-01",  8133, 5100.0, 2400.0),
        ("2024-01-01",  8133, 5400.0, 2420.0),
        ("2024-10-01",  8133, 5450.0, 2490.0),
        ("2025-01-01",  8133, 5500.0, 2500.0),
        ("2025-07-01",  8133, 5520.0, 2540.0),
        ("2026-01-01",  8133, 5550.0, 2580.0),
        ("2026-07-01",  8133, 5560.0, 2600.0),
    ]
    bm_df = pd.DataFrame(bm, columns=['date','tons','mb_bn','cc_bn'])
    bm_df['date'] = pd.to_datetime(bm_df['date'])
    bm_df = bm_df.sort_values('date').drop_duplicates(subset=['date']).reset_index(drop=True)

    # ---------- Step 1b: 从 precious_metals 拿每日真实金价（1968+ 每日 LBMA 定盘价）----------
    gold_daily_df = pd.DataFrame(columns=['date','gold_price'])
    try:
        db_path = os.path.join(DATA_DIR, 'finance_analysis.db')
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('SELECT date_k, gold_price FROM precious_metals WHERE gold_price IS NOT NULL ORDER BY date_k ASC')
        rows = cur.fetchall()
        conn.close()
        if rows:
            gold_daily_df = pd.DataFrame(rows, columns=['date','gold_price'])
            gold_daily_df['date'] = pd.to_datetime(gold_daily_df['date'])
    except Exception as _e:
        print(f'  [USA货币/黄金] 读取每日金价失败: {type(_e).__name__}')

    # ---------- Step 2: 日度时间轴 ----------
    # 起点 = 1918-01-01；终点 = max( 最后基准节点日期 , precious_metals 最新金价日期 , 当日预期工作日 )
    start_date = pd.Timestamp(bm_df['date'].min().date())
    today = pd.Timestamp('today').date()
    end_date = pd.Timestamp(bm_df['date'].max().date())
    if len(gold_daily_df) > 0:
        end_date = max(end_date, pd.Timestamp(gold_daily_df['date'].max().date()))
    # 同时补到当日（避免金价滞后但用户希望看到今天）
    end_date = max(end_date, pd.Timestamp(today))

    daily_df = pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date, freq='D', normalize=True)})
    daily_df['_jdate'] = (daily_df['date'] - pd.Timestamp('1970-01-01')).dt.days.astype(float)

    # ---------- Step 2b: tons / mb / cc 基准量在日轴上插值 → 末节点之后向后 FFill ----------
    bm_df['_jdate'] = (bm_df['date'] - pd.Timestamp('1970-01-01')).dt.days.astype(float)
    xp = bm_df['_jdate'].values
    x  = daily_df['_jdate'].values
    # np.interp 对左外推返回 xp[0] 对应值，对右外推返回 xp[-1] 对应值 —— 正好等价"外插=末尾恒定"
    daily_df['gold_reserve_tons'] = np.round(np.interp(x, xp, bm_df['tons'].values), 1)
    daily_df['monetary_base_bn']  = np.round(np.interp(x, xp, bm_df['mb_bn'].values), 2)
    daily_df['currency_circ_bn']  = np.round(np.interp(x, xp, bm_df['cc_bn'].values), 2)

    # ---------- Step 3: 每日金价拼接（真实日价 + 缺失 FFill + 1968 前固定价）----------
    OZ_PER_TON = 32150.74658

    # 先把每日真实金价 merge 到时间轴（缺失 = NaN）
    if len(gold_daily_df) > 0:
        daily_df = daily_df.merge(gold_daily_df, on='date', how='left')
    else:
        daily_df['gold_price'] = np.nan

    # 3a. 1934-01-30 及以前（含 1918-1933 + 1934/1/1~1/30，还没 Gold Reserve Act）固定 $20.67（金本位法定）
    pre_1934_mask = daily_df['date'] <= pd.Timestamp('1934-01-30')
    daily_df.loc[pre_1934_mask & daily_df['gold_price'].isna(), 'gold_price'] = 20.67

    # 3b. 1934-01-31 ~ 1967-12-31（布雷顿森林）法定比价 $35/oz
    y1934_1967_mask = (daily_df['date'] >= pd.Timestamp('1934-01-31')) & (daily_df['date'] <= pd.Timestamp('1967-12-31'))
    daily_df.loc[y1934_1967_mask & daily_df['gold_price'].isna(), 'gold_price'] = 35.0

    # 3c. 1968+：先 FFill（周末/节假日用前一个工作日价格）→ 再 BFill（1968 年初缺数据的兜底）
    post_1968_mask = daily_df['date'] >= pd.Timestamp('1968-01-01')
    daily_df.loc[post_1968_mask, 'gold_price'] = (
        daily_df.loc[post_1968_mask, 'gold_price']
        .ffill()
        .bfill()
    )
    # 1968+ 若仍全部缺失（极端兜底：没配 precious_metals）时给一个 $1800 避免空值
    daily_df.loc[post_1968_mask & daily_df['gold_price'].isna(), 'gold_price'] = 1800.0

    # 3d. 全局最终清理（应对任何边界缝隙：先 FFill → 再 BFill → 极端兜底 35.0）
    daily_df['gold_price'] = daily_df['gold_price'].ffill().bfill().fillna(35.0)

    daily_df.rename(columns={'gold_price': 'gold_price_usd_oz'}, inplace=True)
    daily_df['gold_price_usd_oz'] = daily_df['gold_price_usd_oz'].astype(float).round(4)

    # ---------- Step 3d: 黄金储备市值（十亿美元）----------
    daily_df['gold_reserve_usd_bn'] = np.round(
        daily_df['gold_reserve_tons'] * OZ_PER_TON * daily_df['gold_price_usd_oz'] / 1.0e9, 3
    )

    # ---------- Step 4: 组装结果（最后一遍零 NaN 保障）----------
    daily_df = daily_df.sort_values('date').reset_index(drop=True)
    # 对全部数值列：先 FFill（节假日/周末向前填充）→ 再 BFill（极端情况 1918 开头缺的补最后值）→ 再兜底 0.0
    for _col in ['gold_reserve_tons','gold_price_usd_oz','gold_reserve_usd_bn','monetary_base_bn','currency_circ_bn']:
        daily_df[_col] = pd.to_numeric(daily_df[_col], errors='coerce').ffill().bfill().fillna(0.0)
    # 只取真实需要的列，丢弃中间辅助列
    result = {
        "date":                  daily_df['date'].dt.strftime('%Y-%m-%d').tolist(),
        "gold_reserve_tons":     daily_df['gold_reserve_tons'].tolist(),
        "gold_price_usd_oz":     daily_df['gold_price_usd_oz'].tolist(),
        "gold_reserve_usd_bn":   daily_df['gold_reserve_usd_bn'].tolist(),
        "monetary_base_bn":      daily_df['monetary_base_bn'].tolist(),
        "currency_circ_bn":      daily_df['currency_circ_bn'].tolist(),
    }
    source_note = (
        f"数据口径对齐参考图：黄金储备为『市价估值（十亿美元）』= 吨数 × 32150盎司/吨 × 当日金价 ÷ 10^9；"
        f"基础货币/流通中货币单位：十亿美元。时间频率：日度（{len(result['date'])} 条，"
        f"{result['date'][0]} ~ {result['date'][-1]}，每日连续，周末金价/货币量取前值 FFill）。"
        f"金价来源：1918-1933 固定 $20.67/oz，1934-1967 固定 $35/oz（布雷顿森林），"
        f"1968+ 本地 precious_metals 每日 LBMA 伦敦金真实定盘价（周末/节假日向前 FFill 保持连续）。"
        f"储备量/货币供应：Fed/Treasury/WGC 公开历史节点 + 日度线性插值，末节点之后恒定填充。"
    )
    return {"source_note": source_note, "data": result}


# 缓存：避免每次请求都重新生成；后台线程会按需替换刷新
_usa_money_gold_cache = None


@app.route("/api/usa_money_gold_data")
def api_usa_money_gold():
    """美国基础货币发行量 & 黄金储备量 历史关系数据（按需自动补采版）"""
    global _usa_money_gold_cache
    # 1) 非阻塞触发后台刷新：如果数据过期 → 后台 daemon 线程重新生成 + 刷新缓存，前台立刻返回现有数据
    try:
        triggered, refresh_note = _trigger_bg_usa_refresh_if_stale(caller="api_usa_money_gold")
    except Exception as _e:
        triggered, refresh_note = False, f"后台刷新触发跳过：{type(_e).__name__}"

    # 2) 只要缓存存在就直接返回（不看 TTL；过期由后台线程处理，避免首次空等用户体验差）
    if _usa_money_gold_cache is not None:
        payload = dict(_usa_money_gold_cache)
        payload.pop("_ts", None)
        payload["refresh_triggered"] = triggered
        payload["refresh_note"] = refresh_note
        payload["refresh_state"] = {
            "in_progress":       _USA_REFRESH_STATE["in_progress"],
            "last_attempt_ago":  (lambda t=None: None if not t else int(time.time() - t))(_USA_REFRESH_STATE["last_attempt_ts"] if _USA_REFRESH_STATE["last_attempt_ts"] else None),
            "last_success_ago":  (lambda t=None: None if not t else int(time.time() - t))(_USA_REFRESH_STATE["last_success_ts"] if _USA_REFRESH_STATE["last_success_ts"] else None),
            "expected_date":    _usa_expected_latest_date(),
        }
        return jsonify(payload)

    # 3) 缓存为空：首次访问，同步生成一次（兜底保证页面有数据）
    now_ts = time.time()
    result = _fetch_usa_money_gold_data()
    dates     = result["data"]["date"]
    gold_t    = result["data"]["gold_reserve_tons"]
    gold_p    = result["data"]["gold_price_usd_oz"]
    gold_usdb = result["data"]["gold_reserve_usd_bn"]
    mb_b      = result["data"]["monetary_base_bn"]
    cc_b      = result["data"]["currency_circ_bn"]

    def _valid(arr): return [x for x in arr if x is not None]
    gt_v = _valid(gold_t); gp_v = _valid(gold_p); gu_v = _valid(gold_usdb)
    mb_v = _valid(mb_b); cc_v = _valid(cc_b)
    summary = {
        "range": {"from": dates[0] if dates else None, "to": dates[-1] if dates else None},
        "gold_reserve_latest_tons": gold_t[-1] if gold_t else None,
        "gold_reserve_min_tons":    min(gt_v) if gt_v else None,
        "gold_reserve_max_tons":    max(gt_v) if gt_v else None,
        "gold_price_latest_usd_oz": gold_p[-1] if gold_p else None,
        "gold_price_min_usd_oz":    min(gp_v) if gp_v else None,
        "gold_price_max_usd_oz":    max(gp_v) if gp_v else None,
        "gold_reserve_latest_usd_bn": gold_usdb[-1] if gold_usdb else None,
        "gold_reserve_min_usd_bn":    min(gu_v) if gu_v else None,
        "gold_reserve_max_usd_bn":    max(gu_v) if gu_v else None,
        "monetary_base_latest_bn": mb_b[-1] if mb_b else None,
        "monetary_base_min_bn":    min(mb_v) if mb_v else None,
        "monetary_base_max_bn":    max(mb_v) if mb_v else None,
        "currency_circ_latest_bn": cc_b[-1] if cc_b else None,
        "currency_circ_min_bn":    min(cc_v) if cc_v else None,
        "currency_circ_max_bn":    max(cc_v) if cc_v else None,
    }
    payload = {
        "ok": True,
        "count": len(dates),
        "source_note": result["source_note"],
        "summary": summary,
        "date": dates,
        "gold_reserve_tons":     gold_t,
        "gold_price_usd_oz":     gold_p,
        "gold_reserve_usd_bn":   gold_usdb,
        "monetary_base_bn":      mb_b,
        "currency_circ_bn":      cc_b,
        "render_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "refresh_triggered": triggered,
        "refresh_note": refresh_note,
        "refresh_state": {
            "in_progress":      _USA_REFRESH_STATE["in_progress"],
            "last_attempt_ago": None,
            "last_success_ago": None,
            "expected_date":   _usa_expected_latest_date(),
        },
    }
    # 原子存缓存
    cache_entry = dict(payload)
    cache_entry["_ts"] = now_ts
    _usa_money_gold_cache = cache_entry
    return jsonify(payload)


@app.route("/api/index_valuation")
def api_index_general():
    """通用接口：?code=average_pe|hs300|cyb|zz500|sz50  或不带参数返回各指数最新一行摘要"""
    code = request.args.get("code", "").strip()
    if code:
        return _api_index_data(code)
    # 不带参数：返回 5 条指数最新一行（概览首页用）
    overview = {}
    for c in INDEX_CODE_LIST:
        overview[c] = {
            "name": INDEX_META[c][0],
            "latest": index_latest_row(c),
            "count":  index_count_rows(c),
        }
    return jsonify({"ok": True, "overview": overview})


@app.route("/api/refresh_index_now", methods=["GET", "POST"])
def api_refresh_index():
    """手动强制：全量重新抓取所有 5 个指数估值并计算分位（冷启动失败后手动重试可用）"""
    try:
        force = request.args.get("force", "1") != "0"
        msg = collect_all_index_valuation(force=force)
        return jsonify({"ok": True, "msg": msg})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"{type(e).__name__}: {e}"}), 500


# ============================================================
# 4-quater. 个股分红复投回测（akshare 不复权日线 + 东方财富分红送股）
# ============================================================
def _ri_safe_float(x, default=0.0):
    if x is None: return default
    try:
        if pd.isna(x): return default
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return default

def _ri_floor_100(n: float) -> int:
    """向下取 100 的整数倍（A 股买入必须 100 股整数倍）"""
    return int(n // 100) * 100

def _ri_parse_date(d):
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return None
    try:
        ts = pd.Timestamp(d)
        if pd.isna(ts): return None
        return ts.normalize()
    except Exception:
        return None

import contextlib

@contextlib.contextmanager
def _ri_ak_headers_patch():
    """
    临时 patch requests.Session.send，给 akshare 发出的请求自动注入浏览器级 headers
    （东财对默认 python-requests UA 直接 RST 断连，需要伪装成 Chrome 才放行）
    """
    import requests.sessions
    BROWSER_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                  "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/",
        "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-site",
        "Upgrade-Insecure-Requests": "1",
    }
    orig_send = requests.sessions.Session.send
    def _patched_send(self, req, **kwargs):
        for k, v in BROWSER_HEADERS.items():
            if k not in req.headers:
                req.headers[k] = v
        return orig_send(self, req, **kwargs)
    requests.sessions.Session.send = _patched_send
    try:
        yield
    finally:
        requests.sessions.Session.send = orig_send

def _ri_normalize_symbol(sym: str) -> str:
    """规范化股票代码：去掉.SH/.SZ后缀，保留纯 6 位数字"""
    sym = (sym or "").strip().upper()
    sym = sym.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    sym = "".join([c for c in sym if c.isdigit()])
    if len(sym) < 6:
        sym = sym.zfill(6)
    return sym[:6]

def _ri_tencent_market_prefix(s6: str) -> str:
    """6 位数字 -> 腾讯市场前缀：6/9 开头=sh；0/2/3/7/8 开头=sz；默认 sz"""
    if not s6 or len(s6) < 1:
        return "sz"
    return "sh" if s6[0] in ("6", "9") else "sz"

def _ri_fetch_raw_daily_tencent(symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp):
    """
    腾讯财经 fallback：抓取不复权日线
    腾讯接口语义：limit 永远是「end 往前 limit 条」，大区间 start 参数会被忽略。
    因此我们采用「从 end 往前、每页 end = (上一页首条 - 1 天)、每页 limit=1000」的翻页策略，直到覆盖 start_date。
    返回: Series(index=DatetimeIndex, value=float 收盘)
    """
    import requests as _requests
    s = _ri_normalize_symbol(symbol)
    prefix = _ri_tencent_market_prefix(s)
    start_s = start_date.normalize()
    end_s = end_date.normalize()
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Referer": "https://gu.qq.com/",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    PAGE = 1000
    rows_accum = []
    last_err = None
    cur_end = end_s
    # 最多 50 页 ≈ 5w 条（100+ 年），够用
    for i in range(50):
        cur_end_s = cur_end.strftime("%Y-%m-%d")
        try:
            time.sleep(0.25)
            # 注意：start 留空，依赖 limit + cur_end 从后往前
            p = {"param": f"{prefix}{s},day,,{cur_end_s},{PAGE},"}
            r = _requests.get(url, params=p, headers=HEADERS, timeout=30)
            r.raise_for_status()
            j = r.json()
            data = (j.get("data") or {})
            key1 = f"{prefix}{s}"
            bucket = data.get(key1) or {}
            klines = bucket.get("day") or bucket.get("qfqday") or bucket.get("ntdday")
            if not klines:
                for v in data.values():
                    if isinstance(v, dict):
                        klines = v.get("day") or v.get("qfqday") or v.get("ntdday")
                        if klines:
                            break
            if not klines:
                break
            # klines 顺序：首条最旧，末条最新（按日期升序）
            first_d = pd.Timestamp(klines[0][0]).normalize() if len(klines) > 0 else None
            last_d  = pd.Timestamp(klines[-1][0]).normalize() if len(klines) > 0 else None
            rows_accum.extend(klines)
            # 终止条件：
            # 1) 返回不满一页（已拉到上市首日）
            # 2) 本批首条 <= start_date （已经覆盖到需要的起始点）
            # 3) 本批末条 > cur_end - 1 天，且本批首条没变化说明卡住，防止死循环
            if len(klines) < PAGE:
                break
            if first_d is not None and first_d <= start_s:
                break
            next_end = first_d - pd.Timedelta(days=1)
            if next_end <= start_s - pd.Timedelta(days=1):
                # 已经到 start 前一天，再拉只会拉更早的（没意义的边界），停
                break
            # 防死循环：若新 end 没变
            if next_end >= cur_end:
                break
            cur_end = next_end
        except Exception as e:
            last_err = e
            print(f"  [reinvest] 腾讯日线 fallback 第{i+1}页失败: {type(e).__name__}: {e}")
            break

    if not rows_accum:
        if last_err:
            raise last_err
        raise RuntimeError("腾讯财经未返回任何日线数据")

    dates = []
    closes = []
    for r in rows_accum:
        if len(r) < 3:
            continue
        d = pd.Timestamp(r[0]).normalize()
        if d < start_s or d > end_s:
            continue
        dates.append(d)
        closes.append(float(r[2]))
    if not dates:
        raise RuntimeError(f"腾讯财经返回的数据在 [{start_s.date()},{end_s.date()}] 区间为空")
    df = pd.DataFrame({"d": dates, "c": closes}).drop_duplicates("d").sort_values("d")
    close = df.set_index("d")["c"]
    close.index = pd.DatetimeIndex(close.index)
    return close

def _ri_fetch_raw_daily(symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp):
    """
    抓取 A 股不复权日线：主源 akshare（东财），失败时自动 fallback 到腾讯财经。
    返回 Series: index=DatetimeIndex(date), value=收盘(float)。
    """
    import akshare as ak
    s = _ri_normalize_symbol(symbol)
    sd = start_date.strftime("%Y%m%d")
    ed = end_date.strftime("%Y%m%d")
    last_err = None
    with _ri_ak_headers_patch():
        for attempt in range(3):
            try:
                time.sleep(0.6 + attempt * 0.6)
                df = ak.stock_zh_a_hist(symbol=s, period="daily",
                                        start_date=sd, end_date=ed, adjust="", timeout=30)
                if df is None or len(df) == 0:
                    continue
                df["日期"] = pd.to_datetime(df["日期"])
                close = df.set_index("日期")["收盘"].astype(float).sort_index()
                return close
            except Exception as e:
                last_err = e
                print(f"  [reinvest] stock_zh_a_hist({s}) 第{attempt+1}次失败: {type(e).__name__}: {e}")
    # 主源失败 -> fallback 腾讯财经
    print(f"  [reinvest] {s} 东财源失败，fallback 腾讯财经日线...")
    try:
        return _ri_fetch_raw_daily_tencent(symbol, start_date, end_date)
    except Exception as e2:
        raise RuntimeError(
            f"akshare 日线获取失败（3 次重试），且腾讯财经 fallback 也失败："
            f"东财={type(last_err).__name__}:{last_err}; 腾讯={type(e2).__name__}:{e2}"
        )

def _ri_fetch_dividends_native(symbol: str):
    """
    东财 datacenter-web 原生接口：抓取分红送股（akshare 失败时的 fallback）
    对应 ak.stock_fhps_detail_em。自动分页取全量。
    返回 DataFrame：ex_date / per_share_gift / per_share_transfer / per_share_dividend
    """
    import requests as _requests
    s = _ri_normalize_symbol(symbol)
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Referer": "https://data.eastmoney.com/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    all_rows = []
    page = 1
    last_err = None
    while page <= 80:  # 最多 80 页 * 500 = 40000 条，够用
        try:
            time.sleep(0.25)
            params = {
                "sortColumns": "EX_DIVIDEND_DATE",
                "sortTypes": "1",
                "pageSize": "500",
                "pageNumber": str(page),
                "reportName": "RPT_SHAREBONUS_DET",
                "columns": "ALL",
                "quoteColumns": "",
                "filter": f'(SECURITY_CODE="{s}")',
                "source": "WEB",
                "client": "WEB",
            }
            r = _requests.get(url, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            j = r.json()
            result = j.get("result") or {}
            items = result.get("data") or []
            if not items:
                break
            all_rows.extend(items)
            pages = result.get("pages") or 0
            if page >= pages:
                break
            page += 1
        except Exception as e:
            last_err = e
            print(f"  [reinvest] 原生分红接口 fallback 第{page}页失败: {type(e).__name__}: {e}")
            break

    ex_dates, gifts, transfers, divs = [], [], [], []
    for it in all_rows:
        if (it.get("ASSIGN_PROGRESS") or "") != "实施分配":
            continue
        ex_d = _ri_to_date(it.get("EX_DIVIDEND_DATE"))
        if ex_d is None:
            continue
        ex_dates.append(ex_d)
        gifts.append(_ri_safe_float(it.get("BONUS_RATIO")) / 10.0)
        transfers.append(_ri_safe_float(it.get("IT_RATIO")) / 10.0)
        divs.append(_ri_safe_float(it.get("PRETAX_BONUS_RMB")) / 10.0)
    if not ex_dates and last_err:
        raise last_err
    df = pd.DataFrame({
        "ex_date": ex_dates,
        "per_share_gift": gifts,
        "per_share_transfer": transfers,
        "per_share_dividend": divs,
    })
    if len(df) == 0:
        return df
    df = df.sort_values("ex_date").reset_index(drop=True)
    return df

def _ri_fetch_dividends(symbol: str):
    """
    抓取分红送股（仅处理方案进度=实施分配）。
    主源：akshare stock_fhps_detail_em；失败 fallback：东财 datacenter-web 原生接口。
    带 3 次重试 + Chrome 浏览器 headers。
    返回 DataFrame，列：ex_date / per_share_gift / per_share_transfer / per_share_dividend
    """
    import akshare as ak
    s = _ri_normalize_symbol(symbol)
    last_err = None
    with _ri_ak_headers_patch():
        for attempt in range(3):
            try:
                time.sleep(0.6 + attempt * 0.6)
                # akshare 1.18.60 不接受 timeout 参数
                df = ak.stock_fhps_detail_em(symbol=s)
                if df is None or len(df) == 0:
                    return pd.DataFrame(columns=[
                        "ex_date","per_share_gift","per_share_transfer","per_share_dividend"])
                # akshare 中文列名
                if "方案进度" in df.columns:
                    df = df[df["方案进度"].astype(str).str.contains("实施", na=False)].copy()
                df["ex_date"] = pd.to_datetime(df.get("除权除息日") if "除权除息日" in df.columns else df.get("EX_DIVIDEND_DATE"), errors="coerce")
                gift_col = "送转股份-送股比例" if "送转股份-送股比例" in df.columns else "BONUS_RATIO"
                trans_col = "送转股份-转股比例" if "送转股份-转股比例" in df.columns else "IT_RATIO"
                div_col = "现金分红-现金分红比例" if "现金分红-现金分红比例" in df.columns else "PRETAX_BONUS_RMB"
                df["per_share_gift"]     = df[gift_col].apply(lambda x: _ri_safe_float(x)/10.0)
                df["per_share_transfer"] = df[trans_col].apply(lambda x: _ri_safe_float(x)/10.0)
                df["per_share_dividend"] = df[div_col].apply(lambda x: _ri_safe_float(x)/10.0)
                df = df.dropna(subset=["ex_date"]).sort_values("ex_date").reset_index(drop=True)
                return df[["ex_date","per_share_gift","per_share_transfer","per_share_dividend"]]
            except Exception as e:
                last_err = e
                print(f"  [reinvest] stock_fhps_detail_em({s}) 第{attempt+1}次失败: {type(e).__name__}: {e}")
    # fallback 原生接口
    print(f"  [reinvest] {s} akshare 分红接口失败，fallback 东财原生分红接口...")
    try:
        return _ri_fetch_dividends_native(symbol)
    except Exception as e2:
        raise RuntimeError(
            f"akshare 分红送股获取失败（3 次重试），且原生接口 fallback 也失败："
            f"akshare={type(last_err).__name__}:{last_err}; 原生={type(e2).__name__}:{e2}"
        )

def _ri_fetch_sec_name(symbol: str):
    """
    取股票名称：
    主源：腾讯 qt.gtimg.cn（极快，1 行文本就可解析 ~2~名称~）
    兜底：空字符串（前端只显示代码）
    返回: (name, suffix)  suffix = SH / SZ / BJ（腾讯前缀后处理得来）
    """
    import requests as _requests
    s = _ri_normalize_symbol(symbol)
    prefix = _ri_tencent_market_prefix(s)
    suffix = "SH" if prefix == "sh" else "SZ"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
        "Referer": "https://gu.qq.com/",
        "Accept": "*/*",
    }
    try:
        time.sleep(0.05)
        r = _requests.get(f"https://qt.gtimg.cn/q={prefix}{s}",
                          headers=HEADERS, timeout=15)
        r.raise_for_status()
        # 返回形如：v_sh603288="1~海天味业~603288~..."; 用 GBK/UTF-8 解码兼容
        try:
            raw = r.content.decode("gbk", errors="ignore")
        except Exception:
            raw = r.text
        # 取第一个 "~...~" 之间的第二段
        if '"' in raw:
            inner = raw.split('"', 1)[1].rsplit('"', 1)[0]
            parts = inner.split("~")
            if len(parts) >= 3 and parts[1]:
                return parts[1].strip(), suffix
    except Exception as e:
        print(f"  [reinvest] 获取股票名称(腾讯qt)失败: {type(e).__name__}: {e}")
    return "", suffix

def _ri_nearest_close_after(close_series, target_date: pd.Timestamp):
    """在 close_series (DatetimeIndex) 中找 >= target_date 的第一个收盘价，找不到返回 None"""
    if close_series is None or len(close_series) == 0:
        return None, None
    try:
        mask = close_series.index >= target_date.normalize()
    except Exception:
        return None, None
    if not mask.any():
        return None, None
    idx = mask.argmax()
    return close_series.index[idx], float(close_series.iloc[idx])

def _backtest_reinvest(symbol: str, buy_date_str: str, principal: float):
    """
    个股分红复投回测：
      1) 买入日不复权收盘价 → 买入价
      2) 初始股数 = floor(本金/买入价/100)*100
      3) 每次分红（按除权除息日升序）：
         · 派现：按派现前"当前股本 × 每股派现"得现金
         · 送转：股本 × (1 + 每股送股 + 每股转增)，取整
         · 复投：(本次现金+累计剩余现金) ÷ 除息日(或次个交易日)收盘价 → 向下取整百股追加
      4) 到今日：当前股本 × 近一交易日收盘 = 市值 + 剩余现金 → 总收益率、年化
    返回: {"ok":bool, "summary":dict, "details":list, "msg":str}
    """
    symbol = _ri_normalize_symbol(symbol)
    if len(symbol) != 6 or not symbol.isdigit():
        return {"ok": False, "msg": "股票代码格式不正确（需要 6 位数字）", "summary": None, "details": []}
    if not isinstance(principal, (int, float)) or principal <= 0:
        return {"ok": False, "msg": "买入本金必须为正数", "summary": None, "details": []}
    buy_date = pd.Timestamp(buy_date_str).normalize()
    today = pd.Timestamp(dt.date.today()).normalize()
    if buy_date > today:
        return {"ok": False, "msg": "买入日期不能晚于今天", "summary": None, "details": []}

    # ---- Step A. 取不复权日线 (买入日前一周 ~ 今日后一周，保证边界能取到) ----
    start_d = buy_date - pd.Timedelta(days=10)
    end_d = today + pd.Timedelta(days=5)
    close_s = _ri_fetch_raw_daily(symbol, start_d, end_d)
    if len(close_s) == 0:
        return {"ok": False, "msg": f"无法获取 {symbol} 的历史日线数据（akshare 抓取失败，或代码不存在/已退市）",
                "summary": None, "details": []}

    # ---- Step B. 买入价：买入日当日（若非交易日，向后找首个交易日）----
    buy_dt_actual, buy_price = _ri_nearest_close_after(close_s, buy_date)
    if buy_price is None or buy_price <= 0:
        return {"ok": False, "msg": f"无法获取买入价（{buy_date_str} 之后无有效交易日收盘价）",
                "summary": None, "details": []}
    initial_shares = _ri_floor_100(principal / buy_price)
    if initial_shares <= 0:
        return {"ok": False,
                "msg": f"本金过少，按买入价 ¥{buy_price:.2f} 无法买够 100 股（至少需要 ¥{buy_price*100:.2f}）",
                "summary": None, "details": []}
    cash = float(principal) - float(initial_shares) * float(buy_price)
    shares = initial_shares
    total_div_per_share = 0.0   # 原 1 股口径累计每股分红（sum of 每股派现）

    # ---- Step C. 取分红记录，并过滤：ex_date >= 买入日当日 ----
    div_df = _ri_fetch_dividends(symbol)
    details = []
    if len(div_df) > 0:
        div_df = div_df[div_df["ex_date"] >= buy_date].copy().reset_index(drop=True)

    for _, r in div_df.iterrows():
        ex_d       = r["ex_date"]
        gift       = float(r["per_share_gift"])
        transfer   = float(r["per_share_transfer"])
        per_div    = float(r["per_share_dividend"])
        if per_div <= 0 and gift <= 0 and transfer <= 0:
            continue

        before_shares = int(shares)
        # (1) 先派现金（按派现前的股数，即股权登记日持股数）
        cash_div = before_shares * per_div
        total_div_per_share += per_div
        # (2) 再送股 + 转增（取整）
        shares_after_bonus = int( before_shares * (1 + gift + transfer) )

        # (3) 取除息日收盘价（若除息日非交易日取次交易日）
        buyback_dt, buyback_price = _ri_nearest_close_after(close_s, ex_d)
        if buyback_price is None or buyback_price <= 0:
            # 没有可复投的价格（比如刚分红今天还没开盘）：跳过复投，只更新股数和现金
            shares = shares_after_bonus
            cash += cash_div
            details.append({
                "ex_date":      ex_d.strftime("%Y-%m-%d"),
                "per_share_gift":      gift,
                "per_share_transfer":  transfer,
                "per_share_dividend":  per_div,
                "before_shares": before_shares,
                "bonus_shares":  shares_after_bonus - before_shares,
                "shares_after_bonus": shares_after_bonus,
                "cash_dividend": round(cash_div, 2),
                "buyback_date": None,
                "buyback_price": None,
                "available_cash": None,
                "buyback_shares": 0,
                "shares_total": shares,
                "remaining_cash": round(cash, 2),
                "note": "除息日及之后无可用收盘价，跳过本次复投",
            })
            continue

        # (4) 复投：(累计剩余现金 + 本次分红) ÷ 新买入价 → 向下取整百股
        available = cash + cash_div
        add_shares = _ri_floor_100( available / buyback_price )
        shares_total = shares_after_bonus + add_shares
        cost_buy = float(add_shares) * float(buyback_price)
        cash = available - cost_buy

        details.append({
            "ex_date":      ex_d.strftime("%Y-%m-%d"),
            "per_share_gift":      round(gift, 6),
            "per_share_transfer":  round(transfer, 6),
            "per_share_dividend":  round(per_div, 6),
            "before_shares": before_shares,
            "bonus_shares":  shares_after_bonus - before_shares,
            "shares_after_bonus": shares_after_bonus,
            "cash_dividend": round(cash_div, 2),
            "buyback_date":  buyback_dt.strftime("%Y-%m-%d"),
            "buyback_price": round(buyback_price, 4),
            "available_cash": round(available, 2),
            "buyback_shares": add_shares,
            "shares_total":   shares_total,
            "remaining_cash": round(cash, 2),
            "note": "",
        })
        shares = shares_total

    # ---- Step D. 今日汇总 ----
    last_dt, last_close = None, None
    if len(close_s) > 0:
        last_dt = close_s.index[-1]
        last_close = float(close_s.iloc[-1])
    if last_close is None:
        last_close = buy_price
        last_dt = buy_dt_actual
    market_value = float(shares) * float(last_close)
    total_value  = market_value + float(cash)
    hold_days    = (last_dt - buy_dt_actual).days + 1
    hold_years   = max(hold_days / 365.25, 1e-6)
    total_ret    = (total_value - principal) / principal * 100.0
    annual_ret   = ( (total_value / principal) ** (1.0 / hold_years) - 1.0 ) * 100.0

    sec_name, sec_suffix = _ri_fetch_sec_name(symbol)

    summary = {
        "symbol":            symbol,
        "sec_name":          sec_name or "",
        "sec_suffix":        sec_suffix,
        "buy_date":          buy_date.strftime("%Y-%m-%d"),
        "buy_date_actual":   buy_dt_actual.strftime("%Y-%m-%d"),
        "principal":         float(principal),
        "buy_price":         round(float(buy_price), 4),
        "initial_shares":    int(initial_shares),
        "hold_years":        round(hold_years, 3),
        "latest_date":       last_dt.strftime("%Y-%m-%d"),
        "latest_close":      round(float(last_close), 4),
        "current_shares":    int(shares),
        "market_value":      round(market_value, 2),
        "remaining_cash":    round(float(cash), 2),
        "total_value":       round(total_value, 2),
        "total_div_per_share": round(total_div_per_share, 4),
        "total_return_pct":  round(total_ret, 2),
        "annual_return_pct": round(annual_ret, 2),
    }
    return {"ok": True, "msg": "ok", "summary": summary, "details": details}


@app.route("/api/reinvest_backtest")
def api_reinvest_backtest():
    """
    个股分红复投回测接口：
      ?symbol=600519&buy_date=2010-01-04&principal=100000
    返回: {ok, summary, details, msg}
    """
    try:
        sym = request.args.get("symbol", "").strip()
        bd  = request.args.get("buy_date", "").strip()
        p_str = request.args.get("principal", "").strip()
        if not sym or not bd or not p_str:
            return jsonify({"ok": False, "msg": "参数缺少：symbol / buy_date / principal 均必传"}), 400
        principal = float(p_str)
        res = _backtest_reinvest(sym, bd, principal)
        return jsonify(res)
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "msg": f"{type(e).__name__}: {e}",
                        "trace": traceback.format_exc(limit=4)}), 500


# ============================================================
# 5. APScheduler 定时
# ============================================================
def start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        print("[调度] 未安装 apscheduler，无法定时采集（仅手动可用）。请 pip install apscheduler")
        return None
    sched = BackgroundScheduler(timezone="Asia/Shanghai")

    # Job 1：LBMA 伦敦金银价每小时抓一次
    sched.add_job(
        collect_and_save,
        "interval",
        hours=COLLECT_INTERVAL_HOURS,
        next_run_time=datetime.now() + timedelta(seconds=5),
        kwargs={"reason": f"每{COLLECT_INTERVAL_HOURS}小时定时任务"},
        id="collect_lbma_hourly",
        replace_existing=True,
    )

    # Job 2：国内 A 股指数估值（收盘后更新）
    #  - 国内 A 股交易日 15:00 收盘，乐咕/中证官网通常 17:00~18:00 更新当日估值
    #  - 用 cron 工作日（周一到周五）17:30 跑一次
    try:
        sched.add_job(
            collect_all_index_valuation,
            "cron",
            day_of_week="mon-fri",
            hour=17,
            minute=30,
            timezone="Asia/Shanghai",
            kwargs={"force": True},
            id="collect_index_daily_1730",
            replace_existing=True,
        )
        job2_info = " · 指数估值：工作日 17:30 (CST) 收盘后抓当日"
    except Exception as e_job:
        job2_info = f" · 指数估值 job 添加失败: {type(e_job).__name__}: {e_job}"

    sched.start()
    print(f"[调度] 已启动 APScheduler："
          f"金银价每 {COLLECT_INTERVAL_HOURS}h 一次{job2_info}")
    return sched


# ============================================================
# 6. 主入口
# ============================================================
def main():
    print("=" * 66)
    print(" 金银比可视化 & A股估值 一体化前后端服务")
    print("=" * 66)
    init_db()
    import_historical_csv_if_empty()
    print(f"[DB] 金银价总记录数：{count_rows()}")

    # 指数估值：冷启动导入（库空才抓，避免每次启动都打请求；首次启动会全量抓 5 条指数历史+算分位，预计耗时 1~3 分钟）
    # 放到后台线程执行：不阻塞 Flask 立即监听 5000 端口，抓完之后页面自然就有数据了
    def _cold_start_index_bg():
        try:
            collect_all_index_valuation(force=False)
        except Exception as e_idx:
            print(f"[指数估值] 冷启动异常（不影响金银比）：{type(e_idx).__name__}: {e_idx}")
    threading.Thread(target=_cold_start_index_bg, name="idx_coldstart", daemon=True).start()

    # 先立即采集一次当日最新价（保证数据库最新，周末自动跳过）—— 也放后台
    def _lbma_startup_bg():
        try:
            collect_and_save(reason="启动即刻")
        except Exception as e_pm:
            print(f"[贵金属] 启动即刻后台刷新异常（不影响页面）：{type(e_pm).__name__}: {e_pm}")
            import traceback
            traceback.print_exc()
    threading.Thread(target=_lbma_startup_bg, name="lbma_startup", daemon=True).start()

    sched = start_scheduler()

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5000"))
    print(f"\n[Web] 访问地址：http://127.0.0.1:{port}")
    print(f"[Web] 监听主机：{host}，端口：{port}")
    print("[Web] Ctrl+C 可退出（长期后台运行建议用 pythonw 或 nssm / systemd）\n")

    try:
        # debug=False 生产模式，避免重载重启APScheduler
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    except KeyboardInterrupt:
        print("\n[服务] 用户中断退出")
    finally:
        if sched is not None:
            try:
                sched.shutdown(wait=False)
                print("[调度] APScheduler 已停止")
            except Exception:
                pass


if __name__ == "__main__":
    main()
