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
import numpy as np
import requests
from flask import Flask, render_template, jsonify, request, session

# ============================================================
# 1. 配置区（数据库等）
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
os.makedirs(TEMPLATE_DIR, exist_ok=True)

# —— 数据库连接 ——
#  用户提供的 MySQL 主库（优先使用）：
_MYSQL_USER = "admin"   # 2026-07-11 由 financeAnalysis 切换为 admin，获取更高的建表/写权限
# 注意：用户提供的密码原文是 "st!:a&h=C2eu "（末尾带一个空格，但用户提示"密码不包含分号"，
# 实测表明那个末尾空格大概率是输入时的手误，所以这里统一采用 rstrip() 后的值。
_MYSQL_PASS_RAW = "st!:a&h=C2eu "
_MYSQL_PASS = _MYSQL_PASS_RAW.rstrip()   # 去掉可能误输入的末尾空格；若要保留原始值可改回 _MYSQL_PASS_RAW
_MYSQL_HOST = "175.178.250.9"
_MYSQL_PORT = 3306
_MYSQL_DB   = "finance_analysis"
# 拼接 SQLAlchemy URL 时，对密码做 URL 安全编码（避免特殊字符如 !&=? 干扰）
try:
    from urllib.parse import quote_plus as _url_quote
except Exception:
    from urllib import quote_plus as _url_quote
DB_URL = (
    f"mysql+pymysql://{_MYSQL_USER}:{_url_quote(_MYSQL_PASS)}@{_MYSQL_HOST}:{_MYSQL_PORT}"
    f"/{_MYSQL_DB}?charset=utf8mb4"
)
#  SQLite 回退/迁移源（之前保存的所有历史数据，用于一键迁移到 MySQL）
DB_NAME = "finance_analysis"
SQLITE_DB_PATH = os.path.join(DATA_DIR, f"{DB_NAME}.db")
SQLITE_DB_URL = f"sqlite:///{SQLITE_DB_PATH}"

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
#  —————————— 告警邮件发送方配置 ——————————
EMAIL_ENABLED = True
EMAIL_HOST = "smtp.163.com"
EMAIL_PORT = 465
EMAIL_USER = "huangxuanheng@163.com"
EMAIL_PASS = "DS6A532wSJNmYQMB"   # 163授权码
EMAIL_FROM = EMAIL_USER   # 发件人（=SMTP登录账号）
EMAIL_TO   = EMAIL_USER   # 默认收件人（告警引擎会按用户注册邮箱覆盖）


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
from sqlalchemy.types import TypeEngine

# -------- 可选：先确保 MySQL 中 finance_analysis 数据库存在（没权限则跳过）--------
def _ensure_mysql_database_exists():
    """如果是 MySQL，先连到实例级别（不指定 database），CREATE DATABASE IF NOT EXISTS。
    无权限或网络不通时静默跳过，后面 SQLAlchemy 再抛具体错误。"""
    if not DB_URL.startswith("mysql+pymysql://"):
        return
    try:
        import pymysql
        try:
            c = pymysql.connect(host=_MYSQL_HOST, port=_MYSQL_PORT,
                                user=_MYSQL_USER, password=_MYSQL_PASS,
                                charset="utf8mb4", connect_timeout=10)
        except Exception:
            # 密码里若带末尾空格被用户误输入，再尝试去掉末尾空格
            c = pymysql.connect(host=_MYSQL_HOST, port=_MYSQL_PORT,
                                user=_MYSQL_USER, password=_MYSQL_PASS.rstrip(),
                                charset="utf8mb4", connect_timeout=10)
        with c.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{_MYSQL_DB}` "
                f"DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci"
            )
        c.commit()
        c.close()
    except Exception as _e:
        print(f"  [db_init] 跳过 MySQL 建库检查：{type(_e).__name__}: {_e}")

_ensure_mysql_database_exists()

# -------- MySQL 连接验证：连不上则报错退出，确保使用统一数据库 --------
_MYSQL_FALLBACK_MSG = None
try:
    with create_engine(DB_URL, echo=False, future=True,
                       connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {},
                       pool_pre_ping=False, poolclass=__import__("sqlalchemy.pool", fromlist=["NullPool"]).NullPool
                       ).connect() as _probe:
        _probe.execute(text("SELECT 1"))
except Exception as _conn_err:
    _MYSQL_FALLBACK_MSG = (
        f"[FATAL] MySQL 连接失败："
        f"{type(_conn_err).__name__}: {_conn_err}"
    )
    print("\n" + "!" * 72)
    print(_MYSQL_FALLBACK_MSG)
    print("  原因常见：MySQL 账号/密码不对、服务器 3306 端口未开放、IP 白名单未加当前出口 IP。")
    print("  请检查 MySQL 连接配置后重启进程！")
    print("!" * 72 + "\n")
    raise RuntimeError(f"MySQL 连接失败：{_conn_err}")

engine = create_engine(DB_URL, echo=False, future=True,
                       connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
Base = declarative_base()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# -------- SQLite 回退/迁移源 engine（仅用于把老 sqlite 数据迁到 MySQL）--------
_sqlite_engine = None
def get_sqlite_engine():
    global _sqlite_engine
    if _sqlite_engine is None and os.path.exists(SQLITE_DB_PATH):
        _sqlite_engine = create_engine(SQLITE_DB_URL, echo=False, future=True,
                                       connect_args={"check_same_thread": False})
    return _sqlite_engine


def _ensure_sqlite_columns():
    """SQLite 专属：为 Base 下所有 ORM 表，对比 PRAGMA table_info 与 ORM 列定义，
    对缺失的列执行 ALTER TABLE ... ADD COLUMN 补齐（所有新增列都是可空类型，
    不会造成数据丢失）。
    MySQL / PG 等其他方言：空操作，假设你使用正规迁移工具管理升级。"""
    if engine.dialect.name != "sqlite":
        return

    def _sa_type_to_sql(t: TypeEngine) -> str:
        # SQLite 类型亲和度宽松，按大类映射即可
        s = str(t).upper()
        if "FLOAT" in s or "DOUBLE" in s or "REAL" in s or "NUMERIC" in s:
            return "FLOAT"
        if "INT" in s or "BIGINT" in s:
            return "INTEGER"
        if "DATE" in s or "DATETIME" in s or "TIME" in s:
            return "DATE"
        if "BOOL" in s:
            return "INTEGER"
        if "BLOB" in s or "BINARY" in s:
            return "BLOB"
        return "TEXT"

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for mapper in Base.registry.mappers:
                table = mapper.persist_selectable
                tname = table.name
                rows = conn.execute(text(f'PRAGMA table_info("{tname}")')).fetchall()
                existing = {r[1].lower() for r in rows}  # 第 1 列 = 列名
                for col in table.columns:
                    if col.name.lower() in existing:
                        continue
                    sql_type = _sa_type_to_sql(col.type)
                    ddl = f'ALTER TABLE "{tname}" ADD COLUMN "{col.name}" {sql_type}'
                    try:
                        conn.execute(text(ddl))
                        print(f"[DB][迁移] 表 {tname} 新增列: {col.name} ({sql_type})")
                    except Exception as ex:
                        # ignore duplicate / already added on concurrent runs
                        print(f"[DB][迁移] 忽略警告：{ddl} 失败: {ex}")
            trans.commit()
        except Exception:
            trans.rollback()
            raise


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
    """初始化：建库（若MySQL）+ 建表 + SQLite 下自动补齐缺失列 + 可选 SQLite→MySQL 数据迁移。
    注意：如果 MySQL 连接失败或建表失败，不再自动回退到 SQLite，而是直接报错退出，
    确保服务器和本地使用同一数据库，避免数据不一致。"""
    global engine, SessionLocal

    # SQLite 不需要 CREATE DATABASE
    if DB_URL.startswith("mysql+pymysql://"):
        try:
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{_MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4"))
                conn.commit()
        except Exception:
            pass
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as _e:
        if DB_URL.startswith("mysql+pymysql://"):
            print(f"\n[FATAL] MySQL 建表失败：{type(_e).__name__}: {str(_e)[:160]}")
            print("  请检查 MySQL 用户权限（需要 CREATE, ALTER 权限）")
            print("  授权命令：GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,ALTER,INDEX,DROP")
            print(f"    ON `{_MYSQL_DB}`.* TO '{_MYSQL_USER}'@'%'; FLUSH PRIVILEGES;")
            raise
        else:
            raise
    if DB_URL.startswith("sqlite"):
        _ensure_sqlite_columns()
    # —— 自动迁移：SQLite → MySQL（仅当 MySQL 所有存量表都空空如也，且 sqlite 文件存在时执行 1 次）——
    _try_auto_migrate_sqlite_to_mysql()
    db_path = DB_URL.split('@')[-1] if '@' in DB_URL else DB_URL
    _is_mysql = DB_URL.startswith("mysql+pymysql://")
    _qt = "`" if _is_mysql else '"'
    print(f"[DB] 已初始化，连接：{db_path}")
    print(f"     - 表 {TABLE_NAME}            (金银价)：记录数 = {count_rows()}")
    print(f"     - 表 {INDEX_TABLE}         (指数估值)：记录数 = {index_count_rows()}")
    try:
        with SessionLocal() as _s:
            _n = _s.execute(text(f'SELECT COUNT(*) FROM {_qt}{DIVIDEND_INDEX_TABLE}{_qt}')).scalar()
            print(f"     - 表 {DIVIDEND_INDEX_TABLE} (红利指数估值)：记录数 = {_n}")
    except Exception as _e:
        print(f"     - 表 {DIVIDEND_INDEX_TABLE} (红利指数估值)：未就绪 - {_e}")
    try:
        with SessionLocal() as _s:
            _n_sd = _s.execute(text(f'SELECT COUNT(*) FROM {_qt}{STOCK_DAILY_TABLE}{_qt}')).scalar()
            _n_sdv = _s.execute(text(f'SELECT COUNT(*) FROM {_qt}{STOCK_DIVIDEND_TABLE}{_qt}')).scalar()
            print(f"     - 表 {STOCK_DAILY_TABLE}       (个股日K)：记录数 = {_n_sd}")
            print(f"     - 表 {STOCK_DIVIDEND_TABLE}    (个股分红)：记录数 = {_n_sdv}")
    except Exception as _e2:
        print(f"     - 表 stock_daily / stock_dividend：未就绪 - {_e2}")


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

# ============================================================
# 2-quater-div. 红利指数元数据 & ORM 模型
# ============================================================
DIVIDEND_INDEX_TABLE = "dividend_index_daily"
# 仅保留 2 只核心红利指数：(中文名, 乐咕 indexCode, 描述)
DIVIDEND_INDEX_META = {
    # idx_code        (中文名,             乐咕index_code,     指数简介)
    "csi_dividend":  ("中证红利",       "000922.CSI",   "沪深A股中高股息率/分红连续稳定的100只股票，最主流红利宽基·市值加权"),
    "sse_dividend":  ("上证红利",       "000015.SH",    "沪市50只高股息蓝筹·经典红利指数"),
}
DIVIDEND_INDEX_CODES = list(DIVIDEND_INDEX_META.keys())


class DividendIndexValuation(Base):
    """(date_k, idx_code) 联合主键，存储红利指数每日股息率 / 市盈率 / 市净率 估值数据。
    字段来源：乐咕 legulegu.com /api/stockdata/index-basic 接口。
    主口径（市值加权）：pe_ttm / pe_lyr / pb（也是图表默认绘制）。
    辅助口径：等权 add_*、中位数 middle_*、以及股息率 dv_*（数据保留供参考）。
    """
    __tablename__ = DIVIDEND_INDEX_TABLE
    date_k           = Column(Date, nullable=False, comment="交易日")
    idx_code         = Column(String(20), nullable=False, comment="红利指数代码: csi_dividend 等")
    close            = Column(Float, nullable=True, comment="指数收盘点位")
    # ---- 股息率（市值加权）----
    dv_ratio_lyr     = Column(Float, nullable=True, comment="静态股息率(%) - LYR = 近1年度")
    dv_ttm           = Column(Float, nullable=True, comment="滚动股息率TTM(%) - 近 4 季度滚动分红")
    add_dv_ratio     = Column(Float, nullable=True, comment="等权静态股息率(%)")
    add_dv_ttm       = Column(Float, nullable=True, comment="等权滚动股息率TTM(%)")
    dv_ratio_q       = Column(Float, nullable=True, comment="静态股息率 历史分位数(0-1)")
    dv_ttm_q         = Column(Float, nullable=True, comment="滚动股息率TTM 历史分位数(0-1)")
    # ---- 市盈率 + 市净率（图表默认绘制；市值加权 · 整体法）----
    pe_ttm           = Column(Float, nullable=True, comment="滚动市盈率(TTM · 市值加权整体法)")
    pe_lyr           = Column(Float, nullable=True, comment="静态市盈率(LYR · 市值加权整体法)")
    pb               = Column(Float, nullable=True, comment="市净率(市值加权整体法)")
    pe_ttm_q         = Column(Float, nullable=True, comment="滚动PE(TTM) 历史分位数(0-1)")
    pe_lyr_q         = Column(Float, nullable=True, comment="静态PE(LYR) 历史分位数(0-1)")
    pb_q             = Column(Float, nullable=True, comment="PB 历史分位数(0-1)")
    # ---- 辅助口径（市盈率/市净率 · 等权 / 中位数）----
    add_pe_ttm       = Column(Float, nullable=True, comment="等权滚动市盈率(TTM)")
    add_pe_lyr       = Column(Float, nullable=True, comment="等权静态市盈率(LYR)")
    add_pb           = Column(Float, nullable=True, comment="等权市净率")
    middle_pe_ttm    = Column(Float, nullable=True, comment="中位数滚动市盈率(TTM)")
    middle_pe_lyr    = Column(Float, nullable=True, comment="中位数静态市盈率(LYR)")
    middle_pb        = Column(Float, nullable=True, comment="中位数市净率")

    __table_args__ = (
        PrimaryKeyConstraint("date_k", "idx_code", name="pk_dividend_index_daily"),
        {"comment": "A 股主流红利指数 股息率/PE/PB 估值历史（日度）"},
    )

    def to_dict(self):
        return {
            "date_k":         self.date_k.isoformat() if self.date_k else None,
            "close":          float(self.close)          if self.close is not None else None,
            # 股息率
            "dv_ratio_lyr":   float(self.dv_ratio_lyr)   if self.dv_ratio_lyr is not None else None,
            "dv_ttm":         float(self.dv_ttm)         if self.dv_ttm is not None else None,
            "add_dv_ratio":   float(self.add_dv_ratio)   if self.add_dv_ratio is not None else None,
            "add_dv_ttm":     float(self.add_dv_ttm)     if self.add_dv_ttm is not None else None,
            "dv_ratio_q":     float(self.dv_ratio_q)     if self.dv_ratio_q is not None else None,
            "dv_ttm_q":       float(self.dv_ttm_q)       if self.dv_ttm_q is not None else None,
            # PE / PB（默认图表口径）
            "pe_ttm":         float(self.pe_ttm)         if self.pe_ttm is not None else None,
            "pe_lyr":         float(self.pe_lyr)         if self.pe_lyr is not None else None,
            "pb":             float(self.pb)             if self.pb is not None else None,
            "pe_ttm_q":       float(self.pe_ttm_q)       if self.pe_ttm_q is not None else None,
            "pe_lyr_q":       float(self.pe_lyr_q)       if self.pe_lyr_q is not None else None,
            "pb_q":           float(self.pb_q)           if self.pb_q is not None else None,
            # 辅助：等权 / 中位数
            "add_pe_ttm":     float(self.add_pe_ttm)     if self.add_pe_ttm is not None else None,
            "add_pe_lyr":     float(self.add_pe_lyr)     if self.add_pe_lyr is not None else None,
            "add_pb":         float(self.add_pb)         if self.add_pb is not None else None,
            "middle_pe_ttm":  float(self.middle_pe_ttm)  if self.middle_pe_ttm is not None else None,
            "middle_pe_lyr":  float(self.middle_pe_lyr)  if self.middle_pe_lyr is not None else None,
            "middle_pb":      float(self.middle_pb)      if self.middle_pb is not None else None,
        }


# ============================================================
# 2-ter. 个股历史数据 ORM 模型（日K线 + 分红送股）
# ============================================================
STOCK_DAILY_TABLE = "stock_daily"
STOCK_DIVIDEND_TABLE = "stock_dividend"


class StockDaily(Base):
    """个股不复权日K线（(code, date_k) 联合主键），用于股息率图表 / 回测等。
    数据源：akshare stock_zh_a_hist（东财）→ fallback 腾讯财经。
    增量更新：首次请求时拉取上市首日→当日；后续只追加 库中最大日期+1 → 今日。
    """
    __tablename__ = STOCK_DAILY_TABLE
    code       = Column(String(16), nullable=False, comment="证券代码，纯数字 6 位，如 000895")
    date_k     = Column(Date, nullable=False, comment="交易日")
    open_p     = Column(Float, nullable=True, comment="开盘价（不复权）")
    high_p     = Column(Float, nullable=True, comment="最高价（不复权）")
    low_p      = Column(Float, nullable=True, comment="最低价（不复权）")
    close_p    = Column(Float, nullable=False, comment="收盘价（不复权，用于股息率分母）")
    volume     = Column(Float, nullable=True, comment="成交量（手）")
    amount     = Column(Float, nullable=True, comment="成交额（元）")
    turnover   = Column(Float, nullable=True, comment="换手率(%)")

    __table_args__ = (
        PrimaryKeyConstraint("code", "date_k", name="pk_stock_daily"),
        {"comment": "A 股个股不复权日 K 线（按 code + 交易日增量更新）"},
    )

    def to_dict(self):
        return {
            "code": self.code,
            "date_k": self.date_k.isoformat() if self.date_k else None,
            "open_p":  _safe_float(self.open_p),
            "high_p":  _safe_float(self.high_p),
            "low_p":   _safe_float(self.low_p),
            "close_p": _safe_float(self.close_p),
            "volume":  _safe_float(self.volume),
            "amount":  _safe_float(self.amount),
            "turnover":_safe_float(self.turnover),
        }


class StockDividend(Base):
    """个股历次分红送股（(code, ex_date, per_share_dividend, per_share_gift, per_share_transfer)
    近似唯一，以 code + ex_date + 金额/股数 作为主键去重；
    同时保留股东大会决议公告日/监管发布公告日（reg_publish_date）作为 V6 known_from 双重约束依据。
    """
    __tablename__ = STOCK_DIVIDEND_TABLE
    code                 = Column(String(16), nullable=False, comment="证券代码 6 位")
    ex_date              = Column(Date, nullable=False, comment="除权除息日（股权登记日之后第 1 个交易日）")
    per_share_dividend   = Column(Float, nullable=False, default=0.0, comment="每股派现（元/股，税前，含所有分期年度）")
    per_share_gift       = Column(Float, nullable=False, default=0.0, comment="每股送股（股/股，来源：未分配利润）")
    per_share_transfer   = Column(Float, nullable=False, default=0.0, comment="每股转增（股/股，来源：资本公积）")
    record_date          = Column(Date, nullable=True, comment="股权登记日")
    pay_date             = Column(Date, nullable=True, comment="派息日（现金红利到账日）")
    reg_publish_date     = Column(Date, nullable=True, comment="监管层面最早发布该方案的日期（股东大会决议公告/IPO 招股书等，V6 前视约束用）")
    plan_publish_date    = Column(Date, nullable=True, comment="董事会预案公告日（原始披露，可能未通过；仅用于辅助 debug）")
    impl_publish_date    = Column(Date, nullable=True, comment="实施公告日（东财 data.eastmoney.com 公告详情页公开）")
    report_year          = Column(String(10), nullable=True, comment="分红对应的报告期/年度，如 2023、2023H1、2023Q3")
    report_date          = Column(Date, nullable=True, comment="分红对应的报告期期末日，如 2023-12-31、2023-06-30")
    raw_note             = Column(String(128), nullable=True, comment="原始备注（如有）")
    created_at           = Column(Date, nullable=True, comment="入库日期（本表拉取/更新的日期）")

    __table_args__ = (
        PrimaryKeyConstraint("code", "ex_date", "per_share_dividend", "per_share_gift", "per_share_transfer",
                             name="pk_stock_dividend"),
        {"comment": "个股历次分红送股（除权除息日 + 每股派现/送/转增 联合主键）"},
    )

    def to_dict(self):
        def _d(x):
            return x.isoformat() if x is not None else None
        return {
            "code":               self.code,
            "ex_date":            _d(self.ex_date),
            "per_share_dividend": _safe_float(self.per_share_dividend),
            "per_share_gift":     _safe_float(self.per_share_gift),
            "per_share_transfer": _safe_float(self.per_share_transfer),
            "record_date":        _d(self.record_date),
            "pay_date":           _d(self.pay_date),
            "reg_publish_date":   _d(self.reg_publish_date),
            "plan_publish_date":  _d(self.plan_publish_date),
            "impl_publish_date":  _d(self.impl_publish_date),
            "report_year":        self.report_year,
            "report_date":        _d(self.report_date),
            "raw_note":           self.raw_note,
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


# ============================================================
# 2-bis. 用户认证 & 告警设置 ORM 模型
# ============================================================
from sqlalchemy import Integer, Text, Boolean, ForeignKey, UniqueConstraint

class User(Base):
    """users 表：注册用户（用户名/邮箱唯一，密码 RSA 非对称加密存储）"""
    __tablename__ = "users"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    username      = Column(String(64), nullable=False, unique=True, comment="用户名（唯一）")
    email         = Column(String(128), nullable=False, unique=True, comment="邮箱（唯一，仅支持163/QQ）")
    password_enc  = Column(Text, nullable=False, comment="RSA公钥加密后的密码密文（base64）")
    created_at    = Column(Date, nullable=True, comment="注册日期")

    __table_args__ = (
        {"comment": "注册用户表（密码非对称加密存储）"},
    )

class AlertSetting(Base):
    """alert_settings 表：用户告警配置（每用户每股票一行）"""
    __tablename__ = "alert_settings"
    id                       = Column(Integer, primary_key=True, autoincrement=True)
    user_id                  = Column(Integer, nullable=False, comment="关联 users.id")
    stock_code               = Column(String(16), nullable=False, comment="股票代码 6 位")
    stock_name               = Column(String(64), nullable=True, comment="股票名称（自动查询）")
    szpe_enabled             = Column(Boolean, default=False, comment="是否启用深证PE告警")
    szpe_buy_threshold       = Column(Float, nullable=True, comment="深证PE买入阈值")
    szpe_sell_threshold      = Column(Float, nullable=True, comment="深证PE卖出阈值")
    div_enabled              = Column(Boolean, default=False, comment="是否启用股息率告警")
    div_buy_threshold        = Column(Float, nullable=True, comment="股息率买入阈值(%)")
    div_sell_threshold       = Column(Float, nullable=True, comment="股息率卖出阈值(%)")
    pettm_enabled            = Column(Boolean, default=False, comment="是否启用PE(TTM)告警")
    pettm_buy_threshold      = Column(Float, nullable=True, comment="PE(TTM)买入阈值")
    pettm_sell_threshold     = Column(Float, nullable=True, comment="PE(TTM)卖出阈值")
    gold_silver_enabled      = Column(Boolean, default=False, comment="是否启用金银比告警")
    gold_silver_buy_threshold= Column(Float, nullable=True, comment="金银比买入阈值")
    gold_silver_sell_threshold= Column(Float, nullable=True, comment="金银比卖出阈值")
    updated_at               = Column(Date, nullable=True, comment="最后修改日期")

    __table_args__ = (
        UniqueConstraint("user_id", "stock_code", name="uq_user_stock"),
        {"comment": "用户告警配置表"},
    )

_STOCK_NAME_CACHE = {}

def _get_stock_name(code: str) -> str | None:
    """根据股票代码获取股票名称，带缓存"""
    if not code:
        return None
    code_norm = code.strip()
    if code_norm in _STOCK_NAME_CACHE:
        return _STOCK_NAME_CACHE[code_norm]
    try:
        import akshare as ak
        df = ak.stock_info_a_code_name()
        if df is not None and len(df) > 0:
            for _, row in df.iterrows():
                c = str(row['code']).strip()
                if c == code_norm or c.endswith(code_norm):
                    name = str(row['name']).strip()
                    if name:
                        _STOCK_NAME_CACHE[code_norm] = name
                        return name
    except Exception as e:
        print(f"[股票名称查询] {code} 失败: {e}")
    return None


# ============================================================
# 2-ter. RSA 非对称加密（密码加密/解密）
# ============================================================
_RSA_PRIVATE_KEY_PEM = os.path.join(DATA_DIR, "rsa_private_key.pem")
_RSA_PUBLIC_KEY_PEM  = os.path.join(DATA_DIR, "rsa_public_key.pem")

def _load_or_create_rsa_keys():
    """启动时加载 RSA 密钥对；不存在则生成 2048 位密钥对并写入文件。"""
    try:
        from cryptography.hazmat.primitives.asymmetric import rsa, padding
        from cryptography.hazmat.primitives import serialization, hashes
    except ImportError:
        print("[RSA] cryptography 库未安装，将使用标准库 hashlib 加密方案")
        return None, None
    if os.path.exists(_RSA_PRIVATE_KEY_PEM) and os.path.exists(_RSA_PUBLIC_KEY_PEM):
        try:
            with open(_RSA_PRIVATE_KEY_PEM, "rb") as f:
                priv = serialization.load_pem_private_key(f.read(), password=None)
            with open(_RSA_PUBLIC_KEY_PEM, "rb") as f:
                pub = serialization.load_pem_public_key(f.read())
            print("[RSA] 密钥加载成功，将使用标准库 hashlib 加密方案")
            return None, None
        except Exception as e:
            print(f"[RSA] 密钥文件加载失败，将使用标准库 hashlib 加密方案：{e}")
            return None, None
    print("[RSA] 密钥文件不存在，将使用标准库 hashlib 加密方案")
    return None, None

_RSA_PRIV, _RSA_PUB = _load_or_create_rsa_keys()

def rsa_encrypt_password(plain: str) -> str | None:
    """用标准库 hashlib PBKDF2 加密密码，返回 base64 密文（格式：$HL$salt$key）。"""
    import hashlib
    import os
    import base64
    try:
        salt = os.urandom(16)
        key = hashlib.pbkdf2_hmac('sha256', plain.encode('utf-8'), salt, 100000)
        return base64.b64encode(b'$HL$' + salt + b'$' + key).decode('ascii')
    except Exception as e:
        print(f"[HASH] 加密失败：{e}")
        return None


def rsa_decrypt_password(cipher_b64: str) -> str | None:
    """解密密码（哈希方案不支持解密，返回 None）。"""
    return None


# —— Flask session 配置（app 创建后设置，见下方 app = Flask(...) 处）——
_FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "value500_secret_key_2026")


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


# ============= 红利指数：读写/计数/最新行 =============
def upsert_bulk_dividend(rows: list[dict]):
    """批量 upsert 红利指数估值/股息率/PE/PB 数据
    rows 支持的字段（缺省的保持不变）：
      date_k, idx_code, close,
      dv_ratio_lyr, dv_ttm, add_dv_ratio, add_dv_ttm, dv_ratio_q, dv_ttm_q,
      pe_ttm, pe_lyr, pb, pe_ttm_q, pe_lyr_q, pb_q,
      add_pe_ttm, add_pe_lyr, add_pb, middle_pe_ttm, middle_pe_lyr, middle_pb
    """
    if not rows:
        return 0
    with SessionLocal() as s:
        chg = 0
        for r in rows:
            pk = (r["date_k"], r["idx_code"])
            row = s.get(DividendIndexValuation, pk)
            # 所有可能需要 upsert 的列 (json_key, col_name)
            col_pairs = [
                ("close","close"),
                # 股息率
                ("dv_ratio_lyr","dv_ratio_lyr"), ("dv_ttm","dv_ttm"),
                ("add_dv_ratio","add_dv_ratio"), ("add_dv_ttm","add_dv_ttm"),
                ("dv_ratio_q","dv_ratio_q"), ("dv_ttm_q","dv_ttm_q"),
                # PE/PB（主口径）
                ("pe_ttm","pe_ttm"), ("pe_lyr","pe_lyr"), ("pb","pb"),
                ("pe_ttm_q","pe_ttm_q"), ("pe_lyr_q","pe_lyr_q"), ("pb_q","pb_q"),
                # 等权 / 中位数
                ("add_pe_ttm","add_pe_ttm"), ("add_pe_lyr","add_pe_lyr"), ("add_pb","add_pb"),
                ("middle_pe_ttm","middle_pe_ttm"), ("middle_pe_lyr","middle_pe_lyr"), ("middle_pb","middle_pb"),
            ]
            if row is None:
                kwargs = {"date_k": r["date_k"], "idx_code": r["idx_code"]}
                for k, col in col_pairs:
                    v = _safe_float(r.get(k))
                    if v is not None:
                        kwargs[col] = v
                row = DividendIndexValuation(**kwargs)
                s.add(row)
            else:
                changed = False
                for k, col in col_pairs:
                    v = _safe_float(r.get(k))
                    if v is not None and getattr(row, col) != v:
                        setattr(row, col, v)
                        changed = True
                if changed:
                    chg += 1
        s.commit()
        return chg


def read_dividend_index_data(idx_code: str) -> list[dict]:
    if idx_code not in DIVIDEND_INDEX_META:
        return []
    with SessionLocal() as s:
        rows = (s.query(DividendIndexValuation)
                 .filter(DividendIndexValuation.idx_code == idx_code)
                 .order_by(DividendIndexValuation.date_k)
                 .all())
        return [r.to_dict() for r in rows]


def dividend_index_latest_row(idx_code: str) -> dict | None:
    if idx_code not in DIVIDEND_INDEX_META:
        return None
    with SessionLocal() as s:
        r = (s.query(DividendIndexValuation)
              .filter(DividendIndexValuation.idx_code == idx_code)
              .order_by(DividendIndexValuation.date_k.desc())
              .first())
        return r.to_dict() if r else None


def dividend_index_count(idx_code: str | None = None) -> int:
    with SessionLocal() as s:
        q = s.query(DividendIndexValuation)
        if idx_code:
            q = q.filter(DividendIndexValuation.idx_code == idx_code)
        return q.count()


# ============================================================
# 2-quater-migrate. SQLite → MySQL 一键迁移 + 存量表行数判断
# ============================================================
_MIGRATION_FLAG: dict = {"done": False}


def _mysql_count_all_tables() -> dict:
    """统计主库（MySQL）各主要存量表的行数，用于判断是否需要自动迁移。"""
    tables = [TABLE_NAME, INDEX_TABLE, DIVIDEND_INDEX_TABLE]
    out = {}
    with SessionLocal() as s:
        for t in tables:
            try:
                out[t] = s.execute(text(f"SELECT COUNT(*) FROM `{t}`")).scalar() or 0
            except Exception:
                out[t] = 0
    return out


def _try_auto_migrate_sqlite_to_mysql(force: bool = False):
    """首次启动且 SQLite 有历史数据、MySQL 尚未填充时，自动把存量数据迁移到 MySQL。
    逐表判断（避免"只有一张表有数据、其他两张空空如也"却整体跳过的误判）：
      - 每张目标表如果已经有任何行 → 跳过该表（防止重复）
      - 否则从 SQLite 读对应表，chunked append 写入 MySQL
    """
    if not DB_URL.startswith("mysql+pymysql://"):
        return
    sqlite_eng = get_sqlite_engine()
    if sqlite_eng is None:
        return  # 没有 sqlite 历史文件，不用迁移
    _is_mysql_db = True
    _qt_dst = "`"
    _qt_src = '"'
    counts = _mysql_count_all_tables()
    print(f"[migrate] 检查 MySQL 存量表行数：{counts}")
    import pandas as _pd
    migrated = 0
    with sqlite_eng.connect() as src_conn, engine.connect() as dst_conn:
        spec = [
            (TABLE_NAME,             TABLE_NAME,             ["date_k"]),
            (INDEX_TABLE,            INDEX_TABLE,            ["date_k", "idx_code"]),
            (DIVIDEND_INDEX_TABLE,   DIVIDEND_INDEX_TABLE,   ["date_k", "idx_code"]),
        ]
        for t_src, t_dst, pk_cols in spec:
            # —— 目标表已有行？跳过（保护已有数据不重复）——
            dst_rows = counts.get(t_dst, 0)
            if (not force) and dst_rows > 0:
                print(f"[migrate]   跳过 {t_dst}（目标表已有 {dst_rows} 行）")
                continue
            try:
                df = _pd.read_sql(text(f'SELECT * FROM {_qt_src}{t_src}{_qt_src}'), src_conn)
            except Exception as e:
                print(f"[migrate]   跳过 {t_src}（源表不存在或读失败：{e}）")
                continue
            if df is None or len(df) == 0:
                print(f"[migrate]   跳过 {t_src}（源表无行）")
                continue
            # 规范化 date_k 列为 python date
            if "date_k" in df.columns:
                df["date_k"] = _pd.to_datetime(df["date_k"]).dt.date
            try:
                df = df.drop_duplicates(subset=pk_cols)
                for i in range(0, len(df), 2000):
                    chunk = df.iloc[i:i+2000]
                    chunk.to_sql(name=t_dst, con=dst_conn, if_exists="append", index=False,
                                 method="multi", chunksize=500)
                dst_conn.commit()
                print(f"[migrate]   ✓ {t_dst} 迁移 {len(df)} 行")
                migrated += len(df)
            except Exception as e:
                print(f"[migrate]   ✗ {t_dst} 迁移失败：{type(e).__name__}: {e}")
                try:
                    dst_conn.rollback()
                except Exception:
                    pass
    _MIGRATION_FLAG["done"] = True
    if migrated > 0:
        print(f"[migrate] 完成！共迁移 {migrated} 行（贵金属 / 指数估值 / 红利指数估值）")
    else:
        print(f"[migrate] 没有需要新增迁移的表（所有目标表都有数据）")


# ============================================================
# 2-quater-stock. 个股数据：stock_daily / stock_dividend 读写 & 增量更新
# ============================================================
def upsert_bulk_stock_daily(rows: list[dict]) -> int:
    """批量 upsert stock_daily：rows=[{code, date_k, open_p, high_p, low_p, close_p, volume, amount, turnover}]。
    主键冲突（同 code + 同 date_k）时：只在库中 close_p 为 None 或传入数值更"完整"时覆盖。"""
    if not rows:
        return 0
    with SessionLocal() as s:
        chg = 0
        for r in rows:
            pk = (r["code"], r["date_k"])
            row = s.get(StockDaily, pk)
            if row is None:
                s.add(StockDaily(
                    code=r["code"], date_k=r["date_k"],
                    open_p=_safe_float(r.get("open_p")),
                    high_p=_safe_float(r.get("high_p")),
                    low_p=_safe_float(r.get("low_p")),
                    close_p=_safe_float(r.get("close_p")),
                    volume=_safe_float(r.get("volume")),
                    amount=_safe_float(r.get("amount")),
                    turnover=_safe_float(r.get("turnover")),
                ))
                chg += 1
            else:
                changed = False
                for col, jk in [("open_p","open_p"),("high_p","high_p"),("low_p","low_p"),
                                ("close_p","close_p"),("volume","volume"),("amount","amount"),("turnover","turnover")]:
                    v = _safe_float(r.get(jk))
                    if v is not None and getattr(row, col) != v:
                        setattr(row, col, v)
                        changed = True
                if changed:
                    chg += 1
        s.commit()
        return chg


def upsert_bulk_stock_dividend(rows: list[dict]) -> int:
    """批量 upsert stock_dividend。主键 = (code, ex_date, per_share_dividend, per_share_gift, per_share_transfer)。"""
    if not rows:
        return 0
    with SessionLocal() as s:
        chg = 0
        today_d = dt.date.today()
        for r in rows:
            pk = (r["code"], r["ex_date"],
                  float(r.get("per_share_dividend") or 0.0),
                  float(r.get("per_share_gift") or 0.0),
                  float(r.get("per_share_transfer") or 0.0))
            row = s.get(StockDividend, pk)
            if row is None:
                kwargs = dict(
                    code=r["code"], ex_date=r["ex_date"],
                    per_share_dividend=float(r.get("per_share_dividend") or 0.0),
                    per_share_gift=float(r.get("per_share_gift") or 0.0),
                    per_share_transfer=float(r.get("per_share_transfer") or 0.0),
                    record_date=r.get("record_date"),
                    pay_date=r.get("pay_date"),
                    reg_publish_date=r.get("reg_publish_date"),
                    plan_publish_date=r.get("plan_publish_date"),
                    impl_publish_date=r.get("impl_publish_date"),
                    report_year=r.get("report_year"),
                    raw_note=r.get("raw_note"),
                    created_at=today_d,
                )
                s.add(StockDividend(**kwargs))
                chg += 1
            else:
                changed = False
                for col, jk in [("record_date","record_date"),("pay_date","pay_date"),
                                ("reg_publish_date","reg_publish_date"),
                                ("plan_publish_date","plan_publish_date"),
                                ("impl_publish_date","impl_publish_date"),
                                ("report_year","report_year"),("raw_note","raw_note")]:
                    v = r.get(jk)
                    if v is not None and getattr(row, col) != v:
                        setattr(row, col, v)
                        changed = True
                if changed:
                    chg += 1
        s.commit()
        return chg


def _sd_max_daily_date(code: str) -> dt.date | None:
    with SessionLocal() as s:
        r = (s.query(func.max(StockDaily.date_k))
              .filter(StockDaily.code == code).scalar())
        return r


def _sd_has_any_dividend(code: str) -> bool:
    with SessionLocal() as s:
        return s.query(func.count(StockDividend.code)).filter(StockDividend.code == code).scalar() > 0


# 避免需要 func.* 时漏 import
from sqlalchemy import func as _sa_func   # noqa: E402
# 为了统一：下面代码统一用 func（_sa_func 已别名给全局 func 不存在的情况，先给个引用即可）
if 'func' not in globals():
    func = _sa_func


def _sd_fetch_full_daily_ohlcv(code: str, start_dt: pd.Timestamp, end_dt: pd.Timestamp):
    """复用 _ri_fetch_raw_daily 主流程，但返回完整 OHLCV DataFrame（不只是 close 一列）。
    主源：akshare.stock_zh_a_hist；失败 fallback：腾讯财经（退化为只取 close 一列）。"""
    import akshare as ak
    s = _ri_normalize_symbol(code)
    sd, ed = start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
    last_err = None
    with _ri_ak_headers_patch():
        for attempt in range(3):
            try:
                time.sleep(0.6 + attempt * 0.6)
                df = ak.stock_zh_a_hist(symbol=s, period="daily",
                                        start_date=sd, end_date=ed, adjust="", timeout=30)
                if df is None or len(df) == 0:
                    continue
                # 列名统一（akshare 中文列名：日期/开盘/收盘/最高/最低/成交量/成交额/振幅/涨跌幅/涨跌额/换手率）
                col_map = {"日期":"date_k","开盘":"open_p","收盘":"close_p","最高":"high_p","最低":"low_p",
                           "成交量":"volume","成交额":"amount","换手率":"turnover"}
                df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
                df["date_k"] = pd.to_datetime(df["date_k"]).dt.date
                df = df.sort_values("date_k").reset_index(drop=True)
                out_cols = ["date_k","open_p","high_p","low_p","close_p","volume","amount","turnover"]
                df = df[[c for c in out_cols if c in df.columns]]
                for fc in ["open_p","high_p","low_p","close_p","volume","amount","turnover"]:
                    if fc in df.columns:
                        df[fc] = pd.to_numeric(df[fc], errors="coerce")
                return df
            except Exception as e:
                last_err = e
                print(f"  [stock_daily] stock_zh_a_hist({s}) 第{attempt+1}次失败: {type(e).__name__}: {e}")
    # fallback 腾讯：只有 close 一列
    print(f"  [stock_daily] {s} 东财源失败，fallback 腾讯财经日线（仅 close 有效）…")
    try:
        close_s = _ri_fetch_raw_daily_tencent(code, start_dt, end_dt)
        if close_s is None or len(close_s) == 0:
            raise RuntimeError("腾讯 fallback 也返回空")
        df = pd.DataFrame({"date_k": close_s.index.date, "close_p": close_s.values})
        return df.sort_values("date_k").reset_index(drop=True)
    except Exception as e2:
        raise RuntimeError(
            f"akshare 日线获取失败（3 次重试），且腾讯财经 fallback 也失败："
            f"东财={type(last_err).__name__}:{last_err}; 腾讯={type(e2).__name__}:{e2}"
        )


def get_and_update_stock_daily(code: str):
    """1° 查 MySQL 中该 code 现有最大交易日；
    2° 若缺失 或 最大日 < 今日 → 仅拉取 [max日+1, 今日]（首次则拉 2000-01-01 ~ 今）；
    3° upsert 入库；
    4° 返回完整 {dates, close}（同 _ri_fetch_raw_daily 格式，方便替换）及 原始 OHLCV list[dict]。"""
    code = _ri_normalize_symbol(code).split(".")[0]   # 只要 6 位纯数字
    today_d = dt.date.today()
    max_d = _sd_max_daily_date(code)
    # 确定抓取区间
    if max_d is None:
        start_fetch = pd.Timestamp("2000-01-01")
    else:
        # 最大日之后还有交易日：max_d + 1 起抓取
        start_fetch = pd.Timestamp(max_d) + pd.Timedelta(days=1)
    end_fetch = pd.Timestamp(today_d)
    fetched_df = None
    if start_fetch <= end_fetch:
        print(f"  [stock_daily] {code}: 库最大日期 {max_d}，拉取区间 {start_fetch.date()} ~ {end_fetch.date()}")
        try:
            fetched_df = _sd_fetch_full_daily_ohlcv(code, start_fetch, end_fetch)
        except Exception as e:
            print(f"  [stock_daily] {code}: 增量拉取失败：{e}")
            fetched_df = None
        if fetched_df is not None and len(fetched_df) > 0:
            rows = []
            for _, r in fetched_df.iterrows():
                rows.append(dict(
                    code=code, date_k=r["date_k"],
                    open_p=r.get("open_p"), high_p=r.get("high_p"), low_p=r.get("low_p"),
                    close_p=r["close_p"],
                    volume=r.get("volume"), amount=r.get("amount"), turnover=r.get("turnover"),
                ))
            upsert_bulk_stock_daily(rows)
            print(f"  [stock_daily] {code}: 入库/更新 {len(rows)} 行")
    # 从库中读全量，转为 close Series（index=DatetimeIndex）
    with SessionLocal() as s:
        q = (s.query(StockDaily)
              .filter(StockDaily.code == code)
              .order_by(StockDaily.date_k).all())
        ohlcv_list = [r.to_dict() for r in q]
    idx = pd.DatetimeIndex([pd.Timestamp(x["date_k"]) for x in ohlcv_list])
    close_vals = [x["close_p"] for x in ohlcv_list]
    close_s = pd.Series(close_vals, index=idx, name="close").sort_index()
    return close_s, ohlcv_list


def get_and_update_stock_dividend(code: str):
    """查 MySQL 中该 code 是否有分红；若没有则调用 _ri_fetch_dividends 一次性拉全量并入库。
    返回与 _ri_fetch_dividends 完全同构的 DataFrame（ex_date / per_share_gift / per_share_transfer / per_share_dividend 等），
    额外带 V6 所需的 plan/reg/impl publish_date 列（若抓取层提供）。"""
    code6 = _ri_normalize_symbol(code).split(".")[0]
    if not _sd_has_any_dividend(code6):
        print(f"  [stock_div] {code6}: 库空，从接口拉取全量分红…")
        try:
            div_df = _ri_fetch_dividends(code)   # 复用复投/低估回测现成函数
        except Exception as e:
            raise RuntimeError(f"无法获取 {code} 的分红送股数据：{e}")
        if div_df is not None and len(div_df) > 0:
            rows = []
            today_d = dt.date.today()
            # 兼容列：_ri_fetch_dividends 的返回列名（ex_date 为 date；其他列含 per_share_*；可能还有 plan/impl/reg_publish_date）
            def _to_date(x):
                if x is None or pd.isna(x):
                    return None
                if isinstance(x, dt.date):
                    return x
                try:
                    return pd.Timestamp(x).date()
                except Exception:
                    return None
            for _, r in div_df.iterrows():
                # 如果抓取结果里缺失 reg/plan/impl publish_date，则兜底推断（但这样 V6 双重约束会退化为 ex_date-15）
                # 这里仅把我们能拿到的都入库
                reg_pub = _to_date(r.get("reg_publish_date")) or _to_date(r.get("shareholders_meeting_date"))
                plan_pub = _to_date(r.get("plan_publish_date")) or _to_date(r.get("board_plan_date"))
                impl_pub = _to_date(r.get("impl_publish_date")) or _to_date(r.get("announce_date"))
                rows.append(dict(
                    code=code6,
                    ex_date=_to_date(r["ex_date"]),
                    per_share_dividend=float(r.get("per_share_dividend") or 0.0),
                    per_share_gift=float(r.get("per_share_gift") or 0.0),
                    per_share_transfer=float(r.get("per_share_transfer") or 0.0),
                    record_date=_to_date(r.get("record_date")),
                    pay_date=_to_date(r.get("pay_date")),
                    reg_publish_date=reg_pub,
                    plan_publish_date=plan_pub,
                    impl_publish_date=impl_pub,
                    report_year=str(r["report_year"]) if "report_year" in r and pd.notna(r.get("report_year")) else None,
                    report_date=_to_date(r.get("report_date")),
                    raw_note=None,
                    created_at=today_d,
                ))
            upsert_bulk_stock_dividend(rows)
            print(f"  [stock_div] {code6}: 入库 {len(rows)} 条分红记录")
    # 从库读回 → 转为 DataFrame，列名对齐 _ri_fetch_dividends
    with SessionLocal() as s:
        rows = (s.query(StockDividend)
                 .filter(StockDividend.code == code6)
                 .order_by(StockDividend.ex_date.desc()).all())
    dicts = [r.to_dict() for r in rows]
    if not dicts:
        return pd.DataFrame(columns=["ex_date","per_share_dividend","per_share_gift","per_share_transfer",
                                     "record_date","pay_date","reg_publish_date","plan_publish_date","impl_publish_date",
                                     "report_year","report_date"])
    df = pd.DataFrame(dicts)
    for dc in ["ex_date","record_date","pay_date","reg_publish_date","plan_publish_date","impl_publish_date","report_date"]:
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc]).dt.date
    return df.reset_index(drop=True)


def _is_div_index_stale(idx_code: str) -> tuple:
    """红利指数是否过期：比预期最新交易日少就算过期"""
    expected = _expected_latest_trade_date()
    lr = dividend_index_latest_row(idx_code)
    if lr is None:
        return True, "库空无数据"
    latest = lr["date_k"]
    if isinstance(latest, str):
        latest = dt.date.fromisoformat(latest)
    if latest < expected:
        return True, f"库最新 {latest} < 预期 {expected}"
    return False, f"库最新 {latest} 已是最新"


_DIV_REFRESH_STATE: dict = {}
_DIV_REFRESH_LOCK = threading.Lock()
_DIV_REFRESH_MIN_SEC = 30 * 60


def _div_refresh_now(idx_code: str) -> str:
    """【阻塞】抓 1 个红利指数 → upsert 入库。返回摘要字符串。"""
    try:
        rows = _fetch_one_dividend_index(idx_code)
    except Exception as e:
        return f"{idx_code} 抓取异常 {type(e).__name__}: {str(e)[:140]}"
    if rows:
        chg = upsert_bulk_dividend(rows)
        return f"{idx_code} 写入{len(rows)}行，更新{chg}行；最新日期={rows[-1]['date_k']}"
    return f"{idx_code} 未抓到任何有效数据"


def _trigger_bg_div_refresh_if_stale(idx_code: str, caller: str = "api") -> tuple:
    """不阻塞：若红利指数数据过期 → 起 daemon 线程后台补采；30 分钟节流 + 同指数并发锁。"""
    stale, note = _is_div_index_stale(idx_code)
    if not stale:
        return False, note
    now_ts = time.time()
    with _DIV_REFRESH_LOCK:
        last = _DIV_REFRESH_STATE.get(idx_code, 0)
        if now_ts - last < _DIV_REFRESH_MIN_SEC:
            return False, f"{idx_code} 30分钟内已触发过刷新，跳过"
        _DIV_REFRESH_STATE[idx_code] = now_ts

    def _worker(code):
        print(f"[dividend-bg][{caller}] 后台刷新 {code}...")
        try:
            _div_refresh_now(code)
            print(f"[dividend-bg][{caller}] {code} 刷新完成 ✓")
        except Exception as ex:
            print(f"[dividend-bg][{caller}] {code} 刷新异常：{type(ex).__name__}: {ex}")

    t = threading.Thread(
        target=_worker, args=(idx_code,),
        name=f"bg_div_refresh_{idx_code}",
        daemon=True,
    )
    t.start()
    return True, note + "（后台刷新中）"


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
# ---- 红利指数：乐咕 index-basic 接口抓取（单指数单次接口） ----
# 鉴权方案：与 akshare stock_index_pe_lg 保持一致：
#   1. Session GET 一个公开页面获取 cookies + <meta name="_csrf"> 中的 X-CSRF-Token
#   2. py_mini_racer 执行一段 JS (hex 函数)，输入当天日期字符串 → token
#   3. GET /api/stockdata/index-basic?token=X&indexCode=Y 带 cookies + X-CSRF-Token
import importlib as _importlib
import py_mini_racer as _pmr
from bs4 import BeautifulSoup as _BS

_LEG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36",
}

def _leg_fetch_csrf(url_for_csrf: str) -> dict:
    """自建的安全版 get_cookie_csrf：不会因为 csrf_tag=None 崩溃，session 独立隔离。"""
    sess = requests.Session()
    sess.headers.update(_LEG_HEADERS)
    try:
        r = sess.get(url_for_csrf, timeout=20)
    except Exception:
        return {"cookies": None, "headers": _LEG_HEADERS.copy()}
    csrf_token = None
    try:
        soup = _BS(r.text, features="lxml")
        csrf_tag = soup.find(name="meta", attrs={"name": "_csrf"})
        if csrf_tag is not None and hasattr(csrf_tag, "attrs"):
            csrf_token = csrf_tag.attrs.get("content")
    except Exception:
        csrf_token = None
    h = _LEG_HEADERS.copy()
    if csrf_token:
        h.update({"X-CSRF-Token": csrf_token})
    return {"cookies": r.cookies, "headers": h}

# 从 akshare stock_a_pe_and_pb 里拿 hash_code（9410 字符 JS 代码：含 hex/utf-8 编解码函数）
def _load_lg_hash_and_init_vm():
    try:
        mod_pe_pb = _importlib.import_module("akshare.stock_feature.stock_a_pe_and_pb")
        hc = getattr(mod_pe_pb, "hash_code", None)
        if not hc:
            return None
        vm = _pmr.MiniRacer()
        vm.eval(hc)
        return vm
    except Exception:
        return None

_LEG_JS_VM = _load_lg_hash_and_init_vm()

def _leg_gen_token() -> str:
    """生成当天 hex token；失败时兜底返回 MD5(YYYY-MM-DD)。"""
    if _LEG_JS_VM is not None:
        try:
            return _LEG_JS_VM.call("hex", datetime.now().date().isoformat()).lower()
        except Exception:
            pass
    # Fallback: akshare stock_a_gxl_lg 方案 (MD5(date))
    from hashlib import md5 as _md5
    o = _md5()
    o.update(datetime.now().date().isoformat().encode("utf-8"))
    return o.hexdigest()


def _fetch_one_dividend_index(idx_code: str) -> list[dict]:
    """抓取 1 个红利指数的 PE(TTM/LYR) / PB / 股息率 历史数据。
    复用 akshare stock_index_pe_lg 的鉴权/API 方案，数据来自乐咕 /api/stockdata/index-basic。

    解析策略：
      - 真实数据行：包含 `date` + `close` → 正常交易日 → 存入
      - 末端汇总行（无 date/close，有 *Quantile）：用于对"当前最新值"回填 乐咕官方计算的历史分位
    返回 list[dict]，符合 upsert_bulk_dividend 的字段规范。
    """
    import time as _time
    if idx_code not in DIVIDEND_INDEX_META:
        return []
    name, legu_code, _desc = DIVIDEND_INDEX_META[idx_code]
    print(f"  [{idx_code}({name})] 正在抓取红利指数估值(PE/PB/股息率): leguCode={legu_code} ...")

    # ---- 1. 生成 token + CSRF 会话 ----
    token = _leg_gen_token()
    csrf_kw = _leg_fetch_csrf("https://legulegu.com/stockdata/sz50-ttm-lyr")

    # ---- 2. 调用 API ----
    api_url = "https://legulegu.com/api/stockdata/index-basic"
    params = {"token": token, "indexCode": legu_code}
    last_err = None
    for _try in range(2):
        try:
            r = requests.get(api_url, params=params, timeout=30, **csrf_kw)
            if r.status_code == 200 and r.content:
                break
            last_err = f"HTTP {r.status_code} 空内容"
            csrf_kw = _leg_fetch_csrf("https://legulegu.com/stockdata/shanghaiPE")
            _time.sleep(0.8)
        except Exception as _ex:
            last_err = f"{type(_ex).__name__}: {_ex}"
            csrf_kw = _leg_fetch_csrf("https://legulegu.com/stockdata/shanghaiPE")
            _time.sleep(1.0)
    else:
        raise RuntimeError(f"乐咕 index-basic 接口连续失败（{idx_code}）：{last_err}")
    j = r.json()
    raw_data = j.get("data") or []
    print(f"    ↳ 原始行数 {len(raw_data)} 行")

    # ---- 3. 分离：真实数据行 vs 末端 Quantile 汇总行 ----
    rows = []
    quantile_row = None  # 最后一个含 ttmPeQuantile 但不含 date 的汇总行
    for d in raw_data:
        td = d.get("date")
        close = d.get("close")
        if td and close is not None:
            if isinstance(td, str):
                date_str = td[0:10]
            else:
                import pandas as _pd
                date_str = _pd.Timestamp(td).strftime("%Y-%m-%d")
            rows.append({
                "date_k":        dt.date.fromisoformat(date_str),
                "idx_code":      idx_code,
                "close":         _safe_float(close),
                # 股息率（市值加权）
                "dv_ratio_lyr":  _safe_float(d.get("dvRatio")),
                "dv_ttm":        _safe_float(d.get("dvTtm")),
                "add_dv_ratio":  _safe_float(d.get("addDvRatio")),
                "add_dv_ttm":    _safe_float(d.get("addDvTtm")),
                # PE / PB（市值加权 · 整体法）
                "pe_ttm":        _safe_float(d.get("ttmPe")),
                "pe_lyr":        _safe_float(d.get("lyrPe")),
                "pb":            _safe_float(d.get("pb")),
                # 等权 PE/PB
                "add_pe_ttm":    _safe_float(d.get("addTtmPe")),
                "add_pe_lyr":    _safe_float(d.get("addLyrPe")),
                "add_pb":        _safe_float(d.get("addPb")),
                # 中位数 PE/PB
                "middle_pe_ttm": _safe_float(d.get("middleTtmPe")),
                "middle_pe_lyr": _safe_float(d.get("middleLyrPe")),
                "middle_pb":     _safe_float(d.get("middlePb")),
                # 分位先占位（后面统一自算 + 如果最后有 Quantile 汇总行则覆盖最新一天）
                "dv_ratio_q":    None,
                "dv_ttm_q":      None,
                "pe_ttm_q":      None,
                "pe_lyr_q":      None,
                "pb_q":          None,
            })
        elif (d.get("ttmPeQuantile") is not None) or (d.get("dvTtmQuantile") is not None):
            # 末端 Quantile 汇总行：只对"最新值"有效，拿到后回填给最后一行
            quantile_row = d
    rows.sort(key=lambda r: r["date_k"])

    # ---- 4. 历史分位自算（覆盖 ≥30 点，全部行生效）----
    def _arr_and_pct(field_key):
        arr = np.array([r[field_key] for r in rows if r[field_key] is not None], dtype=float)
        if len(arr) < 30:
            return arr, None
        def _pct(v):
            if v is None: return None
            return float((arr <= float(v)).sum() / len(arr))
        return arr, _pct
    dvttm_arr,   dvttm_pct_fn   = _arr_and_pct("dv_ttm")
    dvr_arr,     dvr_pct_fn     = _arr_and_pct("dv_ratio_lyr")
    pettm_arr,   pettm_pct_fn   = _arr_and_pct("pe_ttm")
    pelyr_arr,   pelyr_pct_fn   = _arr_and_pct("pe_lyr")
    pb_arr,      pb_pct_fn      = _arr_and_pct("pb")
    for r in rows:
        if dvttm_pct_fn:   r["dv_ttm_q"]    = dvttm_pct_fn(r["dv_ttm"])
        if dvr_pct_fn:     r["dv_ratio_q"]  = dvr_pct_fn(r["dv_ratio_lyr"])
        if pettm_pct_fn:   r["pe_ttm_q"]    = pettm_pct_fn(r["pe_ttm"])
        if pelyr_pct_fn:   r["pe_lyr_q"]    = pelyr_pct_fn(r["pe_lyr"])
        if pb_pct_fn:      r["pb_q"]        = pb_pct_fn(r["pb"])

    # ---- 5. 如果有乐咕官方的末端 Quantile 行 → 用官方分位覆盖最后一行（更权威） ----
    if quantile_row is not None and rows:
        last = rows[-1]
        q_mapping = [
            ("ttmPeQuantile",    "pe_ttm_q"),
            ("lyrPeQuantile",    "pe_lyr_q"),
            ("pbQuantile",       "pb_q"),
            ("dvTtmQuantile",    "dv_ttm_q"),
            ("dvRatioQuantile",  "dv_ratio_q"),
        ]
        for api_k, local_k in q_mapping:
            v = _safe_float(quantile_row.get(api_k))
            if v is not None:
                last[local_k] = v

    _time.sleep(0.6)
    if rows:
        print(f"    ↳ ✓ {len(rows)} 行，区间 {rows[0]['date_k']} ~ {rows[-1]['date_k']}"
              f"；最新 PE(TTM)={rows[-1]['pe_ttm']}, PB={rows[-1]['pb']}, DV(TTM)={rows[-1]['dv_ttm']}%")
    return rows


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


def send_email(subject: str, html_body: str, to_addrs=None) -> tuple[bool, str]:
    """
    用 SMTP_SSL 发送一封 HTML 邮件。
    兼容 163、QQ 及大部分主流邮箱（都走 465 / SSL）。
    to_addrs: 自定义收件人（字符串或列表）；为 None 时用全局 EMAIL_TO。
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
    msg["From"] = formataddr((str(Header("Value500告警", "utf-8")), EMAIL_FROM or EMAIL_USER))
    # 收件人：优先用传入的 to_addrs
    if to_addrs is not None:
        to_list = to_addrs if isinstance(to_addrs, (list, tuple)) else [to_addrs]
    else:
        to_list = EMAIL_TO if isinstance(EMAIL_TO, (list, tuple)) else [EMAIL_TO]
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
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
app.secret_key = _FLASK_SECRET_KEY
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
            ("dividend_indices_valuation", "红利指数估值", "/chart/dividend_indices_valuation_raw"),
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
            ("undervalue_backtest","个股低估回测",   "/chart/undervalue_backtest_raw"),
            ("stock_dividend_yield", "个股历史股息率", "/chart/stock_dividend_yield_raw"),
            ("finance_comparison",   "个股同行分析",    "/chart/finance_input"),
            ("user_settings",         "设置",             "/chart/settings_raw"),
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
    # 获取当前登录用户名（用于顶部栏显示登录/退出按钮）
    _u = _current_user()
    _uname = _u.username if _u else None
    # 动态菜单：未登录时隐藏"设置"项
    menu_groups_render = MENU_GROUPS
    if not _uname:
        menu_groups_render = []
        for gid, gname, items in MENU_GROUPS:
            if gid == "tools":
                items = [it for it in items if it[0] != "user_settings"]
            menu_groups_render.append((gid, gname, items))
    return render_template(
        "main_frame.html",
        MENU_GROUPS=menu_groups_render,
        active_group_idx=gi,
        active_menu_idx=mi,
        active_key=active,
        active_raw_url=active_raw_url,
        active_title=active_title,
        active_group_name=group_name,
        render_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        current_username=_uname,
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
@app.route("/chart/dividend_indices_valuation")
def page_dividend_indices_valuation(): return _render_frame("dividend_indices_valuation")
@app.route("/chart/usa_money_gold")
def page_usa_money_gold(): return _render_frame("usa_money_gold")
@app.route("/chart/finance_calculator")
def page_finance_calculator(): return _render_frame("finance_calc")
@app.route("/chart/reinvest_backtest")
def page_reinvest_backtest(): return _render_frame("reinvest_backtest")
@app.route("/chart/undervalue_backtest")
def page_undervalue_backtest(): return _render_frame("undervalue_backtest")
@app.route("/chart/stock_dividend_yield")
def page_stock_dividend_yield(): return _render_frame("stock_dividend_yield")
@app.route("/chart/finance_comparison")
def page_finance_comparison(): return _render_frame("finance_comparison")


# ------- 2. 右侧内容页（raw，嵌入 iframe 的独立页面）--------
@app.route("/chart/finance_calculator_raw")
def finance_calculator_raw():
    """理财计算器独立页（纯前端计算，四个页签）"""
    return render_template("finance_calculator_raw.html")

@app.route("/chart/reinvest_backtest_raw")
def reinvest_backtest_raw():
    """个股分红复投回测独立页（表单输入 + 汇总/明细展示）"""
    return render_template("reinvest_backtest_raw.html")

@app.route("/chart/undervalue_backtest_raw")
def undervalue_backtest_raw():
    """个股低估回测独立页（深证PE择时+个股PE阈值筛选+分红复投+高PE卖出）"""
    return render_template("undervalue_backtest_raw.html")

@app.route("/chart/stock_dividend_yield_raw")
def stock_dividend_yield_raw():
    """个股历史股息率图表页：输入个股代码 → 绘制日度动态股息率（V6 双重约束 + 十字光标）"""
    return render_template("stock_history_dividend.html")

@app.route("/chart/finance_input")
def finance_input_raw():
    """个股同行分析 - 股票输入页"""
    return render_template("finance_input.html")


# ============================================================
# 3-bis. 用户认证 & 设置 页面路由 + API
# ============================================================
@app.route("/finance/compare")
def finance_compare_page():
    codes = request.args.get("codes", "")
    return render_template("finance.html", codes=codes)

@app.route("/api/financial_data", methods=["GET"])
def api_financial_data():
    codes = request.args.get("codes", "")
    years = int(request.args.get("years", 5))
    if not codes:
        return jsonify({"error": "请输入股票代码"}), 400
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if len(code_list) == 0:
        return jsonify({"error": "请输入股票代码"}), 400
    
    try:
        result = _fetch_financial_data(code_list, years)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/login")
def page_login():
    return render_template("login.html")

@app.route("/register")
def page_register():
    return render_template("register.html")

@app.route("/settings")
def page_settings():
    return _render_frame("user_settings")

@app.route("/chart/settings_raw")
def settings_raw():
    return render_template("settings_raw.html")

@app.route("/chart/alert_list_raw")
def alert_list_raw():
    """告警设置列表页（iframe 嵌入）"""
    return render_template("alert_list_raw.html")


# —— 认证辅助 ——
def _current_user():
    """从 Flask session 中获取当前登录用户对象，未登录返回 None"""
    uid = session.get("user_id")
    if not uid:
        return None
    with SessionLocal() as s:
        u = s.get(User, uid)
        if u:
            return u
    return None


@app.route("/api/register", methods=["POST"])
def api_register():
    """用户注册：用户名+邮箱唯一性校验，密码 RSA 加密后落库"""
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not username or len(username) < 2:
        return jsonify({"ok": False, "msg": "用户名至少 2 个字符"})
    if not email or not (email.endswith("@163.com") or email.endswith("@qq.com")):
        return jsonify({"ok": False, "msg": "邮箱只支持 163 邮箱或 QQ 邮箱"})
    if len(password) < 6:
        return jsonify({"ok": False, "msg": "密码至少 6 个字符"})

    # 唯一性检查
    with SessionLocal() as s:
        existing = s.query(User).filter(
            (User.username == username) | (User.email == email)
        ).first()
        if existing:
            if existing.username == username:
                return jsonify({"ok": False, "msg": "用户名已存在"})
            else:
                return jsonify({"ok": False, "msg": "邮箱已被注册"})

        enc = rsa_encrypt_password(password)
        if not enc:
            return jsonify({"ok": False, "msg": "密码加密失败（RSA密钥未就绪）"})
        user = User(username=username, email=email, password_enc=enc,
                    created_at=datetime.now().date())
        s.add(user)
        s.commit()
    return jsonify({"ok": True, "msg": "注册成功"})


@app.route("/api/login", methods=["POST"])
def api_login():
    """用户登录：验证哈希密码，写入 session。"""
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"ok": False, "msg": "请输入用户名和密码"})

    with SessionLocal() as s:
        user = s.query(User).filter(User.username == username).first()
        if not user:
            return jsonify({"ok": False, "msg": "用户名不存在"})
        
        # 验证哈希密码（格式：base64($HL$salt$key)）
        enc = user.password_enc
        import hashlib
        import base64
        try:
            decoded = base64.b64decode(enc)
            parts = decoded.split(b'$')
            if len(parts) == 4 and parts[1] == b'HL':
                salt = parts[2]
                stored_key = parts[3]
                key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
                if key == stored_key:
                    session["user_id"] = user.id
                    session["username"] = user.username
                    return jsonify({"ok": True, "msg": "登录成功", "username": username})
        except Exception as e:
            print(f"[HASH] 验证失败：{e}")
        
        return jsonify({"ok": False, "msg": "密码错误"})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True, "msg": "已退出登录"})


@app.route("/api/current_user")
def api_current_user():
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "logged_in": False})
    return jsonify({"ok": True, "logged_in": True, "username": u.username, "email": u.email})


def _alert_to_dict(row, email: str = "") -> dict:
    """把 AlertSetting ORM 对象转成 dict"""
    return {
        "id": row.id,
        "email": email,
        "stock_code": row.stock_code,
        "stock_name": row.stock_name,
        "szpe_enabled": row.szpe_enabled,
        "szpe_buy": row.szpe_buy_threshold,
        "szpe_sell": row.szpe_sell_threshold,
        "div_enabled": row.div_enabled,
        "div_buy": row.div_buy_threshold,
        "div_sell": row.div_sell_threshold,
        "pettm_enabled": row.pettm_enabled,
        "pettm_buy": row.pettm_buy_threshold,
        "pettm_sell": row.pettm_sell_threshold,
        "gold_silver_enabled": row.gold_silver_enabled,
        "gold_silver_buy": row.gold_silver_buy_threshold,
        "gold_silver_sell": row.gold_silver_sell_threshold,
        "updated_at": str(row.updated_at) if row.updated_at else None,
    }

@app.route("/api/alert_settings/list")
def api_alert_settings_list():
    """获取当前用户所有告警配置列表"""
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "code": "not_logged_in", "msg": "未登录"}), 401
    with SessionLocal() as s:
        rows = s.query(AlertSetting).filter(AlertSetting.user_id == u.id)\
            .order_by(AlertSetting.updated_at.desc()).all()
        data = [_alert_to_dict(r, u.email) for r in rows]
    return jsonify({"ok": True, "data": data})

@app.route("/api/alert_settings/detail/<int:aid>")
def api_alert_settings_detail(aid):
    """获取单条告警配置详情"""
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "code": "not_logged_in", "msg": "未登录"}), 401
    with SessionLocal() as s:
        row = s.query(AlertSetting).filter(AlertSetting.id == aid, AlertSetting.user_id == u.id).first()
        if not row:
            return jsonify({"ok": False, "msg": "告警配置不存在"}), 404
        return jsonify({"ok": True, "data": _alert_to_dict(row, u.email)})

@app.route("/api/alert_settings/save", methods=["POST"])
def api_alert_settings_save():
    """保存告警配置：有id则更新，无id则新增（同一用户同一股票代码唯一）"""
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "code": "not_logged_in", "msg": "未登录"}), 401
    data = request.get_json(silent=True) or {}
    aid = data.get("id")
    stock_code = (data.get("stock_code") or "").strip()
    with SessionLocal() as s:
        if aid:
            try:
                aid_int = int(aid)
                row = s.query(AlertSetting).filter(AlertSetting.id == aid_int, AlertSetting.user_id == u.id).first()
            except (ValueError, TypeError):
                row = None
            if not row:
                return jsonify({"ok": False, "msg": "告警配置不存在"}), 404
        else:
            # 新增模式：先检查是否已存在该用户的该股票配置
            existing = s.query(AlertSetting).filter(AlertSetting.user_id == u.id, AlertSetting.stock_code == stock_code).first()
            if existing:
                row = existing
            else:
                row = AlertSetting(user_id=u.id, stock_code=stock_code)
                row.stock_name = _get_stock_name(stock_code)
                s.add(row)
        if stock_code:
            row.stock_code = stock_code
            # 只在编辑模式且查询成功时更新股票名称，避免覆盖已有名称
            if aid:
                new_name = _get_stock_name(row.stock_code)
                if new_name:
                    row.stock_name = new_name
        row.szpe_enabled = bool(data.get("szpe_enabled", False))
        row.szpe_buy_threshold = _safe_float(data.get("szpe_buy"))
        row.szpe_sell_threshold = _safe_float(data.get("szpe_sell"))
        row.div_enabled = bool(data.get("div_enabled", False))
        row.div_buy_threshold = _safe_float(data.get("div_buy"))
        row.div_sell_threshold = _safe_float(data.get("div_sell"))
        row.pettm_enabled = bool(data.get("pettm_enabled", False))
        row.pettm_buy_threshold = _safe_float(data.get("pettm_buy"))
        row.pettm_sell_threshold = _safe_float(data.get("pettm_sell"))
        row.gold_silver_enabled = bool(data.get("gold_silver_enabled", False))
        row.gold_silver_buy_threshold = _safe_float(data.get("gold_silver_buy"))
        row.gold_silver_sell_threshold = _safe_float(data.get("gold_silver_sell"))
        row.updated_at = datetime.now().date()
        s.commit()
        s.refresh(row)
        new_id = row.id
    return jsonify({"ok": True, "msg": "告警设置已保存", "id": new_id})

@app.route("/api/alert_settings/delete/<int:aid>", methods=["POST"])
def api_alert_settings_delete(aid):
    """删除单条告警配置"""
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "code": "not_logged_in", "msg": "未登录"}), 401
    with SessionLocal() as s:
        row = s.query(AlertSetting).filter(AlertSetting.id == aid, AlertSetting.user_id == u.id).first()
        if not row:
            return jsonify({"ok": False, "msg": "告警配置不存在"}), 404
        s.delete(row)
        s.commit()
    return jsonify({"ok": True, "msg": "已删除"})

@app.route("/api/alert_settings/test", methods=["POST"])
def api_alert_settings_test():
    """手动测试告警检查（用于调试）"""
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "code": "not_logged_in", "msg": "未登录"}), 401
    _check_user_alerts(force=True)
    return jsonify({"ok": True, "msg": "告警检查已触发，请查看服务器日志"})

@app.route("/api/alert_settings", methods=["GET", "POST"])
def api_alert_settings():
    """
    【兼容旧版】GET: 获取当前用户第一条告警配置; POST: 保存（支持只更新邮箱或只更新告警字段）
    新版请用 /api/alert_settings/list、/api/alert_settings/save 等接口
    """
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "code": "not_logged_in", "msg": "未登录"}), 401

    if request.method == "GET":
        with SessionLocal() as s:
            row = s.query(AlertSetting).filter(AlertSetting.user_id == u.id)\
                .order_by(AlertSetting.updated_at.desc()).first()
            if row:
                return jsonify({"ok": True, "data": _alert_to_dict(row, u.email)})
            else:
                return jsonify({"ok": True, "data": {"email": u.email, "stock_code": "",
                    "szpe_enabled": False, "szpe_buy": None, "szpe_sell": None,
                    "div_enabled": False, "div_buy": None, "div_sell": None,
                    "pettm_enabled": False, "pettm_buy": None, "pettm_sell": None,
                    "gold_silver_enabled": False, "gold_silver_buy": None, "gold_silver_sell": None}})

    # POST: 保存（兼容旧版）
    data = request.get_json(silent=True) or {}
    with SessionLocal() as s:
        # 如果只更新邮箱
        if "email" in data and len(data) == 1:
            new_email = (data["email"] or "").strip().lower()
            if new_email and not (new_email.endswith("@163.com") or new_email.endswith("@qq.com")):
                return jsonify({"ok": False, "msg": "邮箱只支持 163/QQ"})
            existing = s.query(User).filter(User.email == new_email, User.id != u.id).first()
            if existing:
                return jsonify({"ok": False, "msg": "该邮箱已被其他用户使用"})
            user_obj = s.get(User, u.id)
            if user_obj and new_email:
                user_obj.email = new_email
                s.commit()
            return jsonify({"ok": True, "msg": "邮箱已更新"})

        # 更新告警配置（取第一条）
        row = s.query(AlertSetting).filter(AlertSetting.user_id == u.id)\
            .order_by(AlertSetting.updated_at.desc()).first()
        if not row:
            row = AlertSetting(user_id=u.id, stock_code="")
            s.add(row)
        row.stock_code = (data.get("stock_code") or "").strip()
        row.szpe_enabled = bool(data.get("szpe_enabled", False))
        row.szpe_buy_threshold = _safe_float(data.get("szpe_buy"))
        row.szpe_sell_threshold = _safe_float(data.get("szpe_sell"))
        row.div_enabled = bool(data.get("div_enabled", False))
        row.div_buy_threshold = _safe_float(data.get("div_buy"))
        row.div_sell_threshold = _safe_float(data.get("div_sell"))
        row.pettm_enabled = bool(data.get("pettm_enabled", False))
        row.pettm_buy_threshold = _safe_float(data.get("pettm_buy"))
        row.pettm_sell_threshold = _safe_float(data.get("pettm_sell"))
        row.gold_silver_enabled = bool(data.get("gold_silver_enabled", False))
        row.gold_silver_buy_threshold = _safe_float(data.get("gold_silver_buy"))
        row.gold_silver_sell_threshold = _safe_float(data.get("gold_silver_sell"))
        row.updated_at = datetime.now().date()
        s.commit()
    return jsonify({"ok": True, "msg": "告警设置已保存"})


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


# ---------------- 红利指数估值 双指数市盈率/市净率折线对比图 ----------------
@app.route("/chart/dividend_indices_valuation_raw")
def raw_dividend_indices_valuation():
    """中证红利 / 上证红利：市盈率(TTM · 市值加权整体法)为主的多折线对比图。
    切换口径可查看 静态PE(LYR) / PB / 等权PE / 股息率 等。"""
    latest_map: dict[str, dict] = {}
    for ic in DIVIDEND_INDEX_CODES:
        lr = dividend_index_latest_row(ic)
        if lr:
            latest_map[ic] = lr
    return render_template(
        "dividend_indices_valuation_raw.html",
        page_title="红利指数估值 · 市盈率历史走势（中证红利 vs 上证红利）",
        page_subtitle="主图指标：滚动市盈率 PE(TTM · 市值加权整体法 = ∑总市值/∑净利润TTM)。股息率字段保留供切换参考。",
        page_data_source="乐咕 index-basic 接口（legulegu.com）：中证红利(000922.CSI) + 上证红利(000015.SH) 每交易日 PE/PB/股息率",
        render_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        DATA_API="/api/dividend_indices_data",
        DIVIDEND_INDEX_META=DIVIDEND_INDEX_META,
        DIVIDEND_INDEX_CODES=DIVIDEND_INDEX_CODES,
        latest_map=latest_map,
    )


@app.route("/api/dividend_indices_data")
def api_dividend_indices_data():
    """红利指数：PE(TTM/LYR) / PB / 股息率 + 收盘点位 + 历史分位；过期自动后台补采。
    前端默认以 pe_ttm 作为主绘制序列。"""
    refresh_status = {}
    for ic in DIVIDEND_INDEX_CODES:
        refreshed, note = _trigger_bg_div_refresh_if_stale(ic, caller="dividend_api")
        refresh_status[ic] = {"refreshed_bg": refreshed, "note": note}
    result_series: dict[str, dict] = {}
    for ic in DIVIDEND_INDEX_CODES:
        rows = read_dividend_index_data(ic)
        result_series[ic] = {
            # 时间轴 + 收盘
            "date":         [r["date_k"]         for r in rows],
            "close":        [r["close"]          for r in rows],
            # 主绘制口径：PE(TTM/LYR) / PB（市值加权）
            "pe_ttm":       [r["pe_ttm"]         for r in rows],
            "pe_lyr":       [r["pe_lyr"]         for r in rows],
            "pb":           [r["pb"]             for r in rows],
            # 主绘制分位
            "pe_ttm_q":     [r["pe_ttm_q"]       for r in rows],
            "pe_lyr_q":     [r["pe_lyr_q"]       for r in rows],
            "pb_q":         [r["pb_q"]           for r in rows],
            # 股息率（保留供参考/切换）
            "dv_ttm":       [r["dv_ttm"]         for r in rows],
            "dv_ratio_lyr": [r["dv_ratio_lyr"]   for r in rows],
            "dv_ttm_q":     [r["dv_ttm_q"]       for r in rows],
            "dv_ratio_q":   [r["dv_ratio_q"]     for r in rows],
            # 辅助：等权 / 中位数
            "add_pe_ttm":   [r["add_pe_ttm"]     for r in rows],
            "add_pe_lyr":   [r["add_pe_lyr"]     for r in rows],
            "add_pb":       [r["add_pb"]         for r in rows],
            "middle_pe_ttm":[r["middle_pe_ttm"]  for r in rows],
            "middle_pe_lyr":[r["middle_pe_lyr"]  for r in rows],
            "middle_pb":    [r["middle_pb"]      for r in rows],
        }
    return jsonify({
        "codes": DIVIDEND_INDEX_CODES,
        "meta": {
            ic: {
                "name":       DIVIDEND_INDEX_META[ic][0],
                "legu_code":  DIVIDEND_INDEX_META[ic][1],
                "desc":       DIVIDEND_INDEX_META[ic][2],
            } for ic in DIVIDEND_INDEX_CODES
        },
        "series": result_series,
        "latest": {ic: dividend_index_latest_row(ic) for ic in DIVIDEND_INDEX_CODES},
        "refresh_status": refresh_status,
    })


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

def _ri_safe_json_num(x, digits=None):
    """用于JSON序列化：把 None/NaN/pd.NA → None（合法JSON null），非空数可选round"""
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        v = float(x)
        if v != v:  # float('nan') 自比较不等
            return None
        if digits is not None:
            v = round(v, digits)
        return v
    except Exception:
        return None

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

    ex_dates, gifts, transfers, divs, report_years = [], [], [], [], []
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
        report_date = it.get("REPORT_DATE")
        if report_date:
            try:
                report_years.append(str(pd.Timestamp(report_date).year))
            except Exception:
                report_years.append(None)
        else:
            report_years.append(None)
    if not ex_dates and last_err:
        raise last_err
    df = pd.DataFrame({
        "ex_date": ex_dates,
        "per_share_gift": gifts,
        "per_share_transfer": transfers,
        "per_share_dividend": divs,
        "report_year": report_years,
    })
    if len(df) == 0:
        return df
    df = df.sort_values("ex_date").reset_index(drop=True)
    return df

def _ri_fetch_dividends(symbol: str):
    """
    抓取分红送股（仅处理方案进度=实施分配）。
    主源：巨潮资讯网 stock_dividend_cninfo（含特别分红、中期分红等，数据最全）；
    失败 fallback：akshare stock_fhps_detail_em；再失败 fallback：东财 datacenter-web 原生接口。
    带 3 次重试 + Chrome 浏览器 headers。
    返回 DataFrame，列：ex_date / per_share_gift / per_share_transfer / per_share_dividend / report_year / report_date
    """
    import akshare as ak
    s = _ri_normalize_symbol(symbol)
    last_err = None
    
    try:
        df = ak.stock_dividend_cninfo(symbol=s)
        if df is not None and len(df) > 0:
            df = df.copy()
            df["ex_date"] = pd.to_datetime(df.get("除权日"), errors="coerce")
            df = df.dropna(subset=["ex_date"]).copy()
            
            df["per_share_dividend"] = df["派息比例"].apply(lambda x: _ri_safe_float(x)/10.0)
            df["per_share_gift"] = df["送股比例"].apply(lambda x: _ri_safe_float(x)/10.0)
            df["per_share_transfer"] = df["转增比例"].apply(lambda x: _ri_safe_float(x)/10.0)
            
            df["report_date"] = df["报告时间"].apply(lambda x: _parse_report_date(x))
            df["report_year"] = df["report_date"].apply(lambda x: str(x.year) if x is not None else None)
            
            df = df.sort_values("ex_date").reset_index(drop=True)
            return df[["ex_date","per_share_gift","per_share_transfer","per_share_dividend","report_year","report_date"]]
    except Exception as e:
        last_err = e
        print(f"  [reinvest] stock_dividend_cninfo({s}) 失败: {type(e).__name__}: {e}")

    with _ri_ak_headers_patch():
        for attempt in range(3):
            try:
                time.sleep(0.6 + attempt * 0.6)
                df = ak.stock_fhps_detail_em(symbol=s)
                if df is None or len(df) == 0:
                    return pd.DataFrame(columns=[
                        "ex_date","per_share_gift","per_share_transfer","per_share_dividend","report_year","report_date"])
                if "方案进度" in df.columns:
                    df = df[df["方案进度"].astype(str).str.contains("实施", na=False)].copy()
                df["ex_date"] = pd.to_datetime(df.get("除权除息日") if "除权除息日" in df.columns else df.get("EX_DIVIDEND_DATE"), errors="coerce")
                gift_col = "送转股份-送股比例" if "送转股份-送股比例" in df.columns else "BONUS_RATIO"
                trans_col = "送转股份-转股比例" if "送转股份-转股比例" in df.columns else "IT_RATIO"
                div_col = "现金分红-现金分红比例" if "现金分红-现金分红比例" in df.columns else "PRETAX_BONUS_RMB"
                df["per_share_gift"]     = df[gift_col].apply(lambda x: _ri_safe_float(x)/10.0)
                df["per_share_transfer"] = df[trans_col].apply(lambda x: _ri_safe_float(x)/10.0)
                df["per_share_dividend"] = df[div_col].apply(lambda x: _ri_safe_float(x)/10.0)
                report_col = "报告期" if "报告期" in df.columns else "REPORT_DATE"
                df["report_date"] = df[report_col].apply(lambda x: pd.Timestamp(x).normalize() if pd.notna(x) else None)
                df["report_year"] = df["report_date"].apply(lambda x: str(x.year) if x is not None else None)
                df = df.dropna(subset=["ex_date"]).sort_values("ex_date").reset_index(drop=True)
                return df[["ex_date","per_share_gift","per_share_transfer","per_share_dividend","report_year","report_date"]]
            except Exception as e:
                last_err = e
                print(f"  [reinvest] stock_fhps_detail_em({s}) 第{attempt+1}次失败: {type(e).__name__}: {e}")
    print(f"  [reinvest] {s} akshare 分红接口失败，fallback 东财原生分红接口...")
    try:
        return _ri_fetch_dividends_native(symbol)
    except Exception as e2:
        raise RuntimeError(
            f"akshare 分红送股获取失败（3 次重试），且原生接口 fallback 也失败："
            f"akshare={type(last_err).__name__}:{last_err}; 原生={type(e2).__name__}:{e2}"
        )


def _parse_report_date(report_time_str):
    """
    解析巨潮资讯网的报告时间字符串，返回报告期期末日。
    示例："2025年报" → 2025-12-31, "2025半年报" → 2025-06-30, "2025三季报" → 2025-09-30
    """
    if report_time_str is None or pd.isna(report_time_str):
        return None
    s = str(report_time_str).strip()
    import re
    match = re.match(r'(\d{4})(年报|半年报|三季报|一季报)', s)
    if match:
        year = int(match.group(1))
        report_type = match.group(2)
        if report_type == '年报':
            return pd.Timestamp(year=year, month=12, day=31)
        elif report_type == '半年报':
            return pd.Timestamp(year=year, month=6, day=30)
        elif report_type == '三季报':
            return pd.Timestamp(year=year, month=9, day=30)
        elif report_type == '一季报':
            return pd.Timestamp(year=year, month=3, day=31)
    return None

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
# 4-tris. 个股低估回测 (深证PE择时 + 个股PE阈值 + 分红复投)
# ============================================================
def _uv_load_shenzhen_pe_history():
    """从本地DB 读取深证指数市场分类整体法 PE-TTM 历史（指数代码 sz_market）。
    返回 pandas Series (index=Timestamp date, value=pe_ttm)。按日期升序。"""
    rows = read_index_data("sz_market")
    if not rows:
        return pd.Series(dtype="float64")
    df = pd.DataFrame(rows)
    df["date_k"] = pd.to_datetime(df["date_k"])
    df = df.dropna(subset=["pe_ttm"]).sort_values("date_k").set_index("date_k")
    return df["pe_ttm"].astype("float64")



def _uv_find_buy_date(sz_pe_hist: pd.Series, div_yield_hist: pd.Series,
                      buy_sz_pe_threshold, buy_div_yield_threshold):
    """找「首次买入日期」（V4：仅支持 深证PE + 股息率 二维组合，已移除个股PE）

    买入条件（与主循环 _check_buy_ok 保持一致）：
      设 szB=buy_sz_pe_threshold 启用；divB=buy_div_yield_threshold 启用
           C_sz  = 深证PE <= buy_sz_pe_threshold
           C_div = 当日股息率% > buy_div_yield_threshold
      ┌───────────────────────┬───────────────────────────┐
      │ 条件组合               │ 触发公式                  │
      ├───────────────────────┼───────────────────────────┤
      │ szB=1 且 divB=1        │ C_sz AND C_div            │
      │ szB=1 且 divB=0        │ C_sz                      │
      │ szB=0 且 divB=1        │ C_div                     │
      └───────────────────────┴───────────────────────────┘
    选日期策略：从距离今天最近的合格日期往前遍历，找到第1个满足条件的日期即确定买入日。
    返回 (buy_date Timestamp or None, 跳过计数 int, 最终筛选出的原因 str)"""

    szB = (buy_sz_pe_threshold is not None)
    divB = (buy_div_yield_threshold is not None)

    if not szB and not divB:
        return None, 0, "自动择时需要至少启用深证PE或买入股息率阈值之一，或改为填写「买入日期」手动指定"

    if szB and sz_pe_hist is not None and len(sz_pe_hist) > 0:
        candidate_dates = sz_pe_hist[sz_pe_hist <= buy_sz_pe_threshold].sort_index(ascending=False)
    else:
        idx_pool = pd.DatetimeIndex([])
        if szB and sz_pe_hist is not None and isinstance(sz_pe_hist.index, pd.DatetimeIndex):
            idx_pool = idx_pool.union(sz_pe_hist.index)
        if divB and div_yield_hist is not None and isinstance(div_yield_hist.index, pd.DatetimeIndex):
            idx_pool = idx_pool.union(div_yield_hist.index)
        if len(idx_pool) == 0:
            return None, 0, "无可用日期索引（深证PE/股息率均为空），请改为填写「买入日期」手动指定"
        candidate_dates = pd.Series(0.0, index=idx_pool).sort_index(ascending=False)

    if len(candidate_dates) == 0:
        return None, 0, (f"深证指数历史中从未出现过PE <= {buy_sz_pe_threshold:.2f} 的日期" if szB
                         else "没有可用的候选日期（请填写自定义买入日）")

    skip_cnt = 0
    for d in candidate_dates.index:
        sz_pe_v = None
        try:
            if sz_pe_hist is not None and len(sz_pe_hist) > 0 and isinstance(sz_pe_hist.index, pd.DatetimeIndex):
                if d in sz_pe_hist.index:
                    sz_pe_v = float(sz_pe_hist.loc[d])
                else:
                    near = sz_pe_hist.index[sz_pe_hist.index <= d]
                    if len(near) > 0:
                        sz_pe_v = float(sz_pe_hist.loc[near[-1]])
        except Exception:
            sz_pe_v = None
        div_y_v = None
        try:
            if divB and div_yield_hist is not None and isinstance(div_yield_hist.index, pd.DatetimeIndex):
                if d in div_yield_hist.index and pd.notna(div_yield_hist.loc[d]):
                    div_y_v = float(div_yield_hist.loc[d])
                else:
                    near = div_yield_hist.index[div_yield_hist.index <= d]
                    if len(near) > 0:
                        div_y_v = float(div_yield_hist.loc[near[-1]])
        except Exception:
            div_y_v = None

        C_sz  = (sz_pe_v is not None) and (sz_pe_v <= buy_sz_pe_threshold) if szB else True
        C_div = (div_y_v is not None) and (div_y_v > buy_div_yield_threshold) if divB else False

        passed = False
        if szB and divB:
            passed = bool(C_sz and C_div)
        elif szB:
            passed = bool(C_sz)
        else:
            passed = bool(C_div)

        if passed:
            parts = []
            if szB: parts.append(f"深证PE({sz_pe_v:.2f})<={buy_sz_pe_threshold:.2f}" if sz_pe_v is not None else f"深证PE<={buy_sz_pe_threshold:.2f}")
            if divB: parts.append(f"股息率({div_y_v:.2f}%)>{buy_div_yield_threshold:.2f}%" if div_y_v is not None else f"股息率>{buy_div_yield_threshold:.2f}%")
            return d, skip_cnt, "确定为买入日 · " + " · ".join(parts)
        skip_cnt += 1
        if skip_cnt > 3000:
            break

    fail_parts = []
    if szB: fail_parts.append(f"深证PE <= {buy_sz_pe_threshold:.2f}")
    if divB: fail_parts.append(f"股息率 > {buy_div_yield_threshold:.2f}%")
    return None, skip_cnt, f"遍历了 {skip_cnt} 个候选日期，但未找到同时满足条件：{' AND '.join(fail_parts)}。建议放宽阈值或改为手动指定买入日期"


def _derive_report_period_end(ex_d, min_days_lag: int = 40) -> pd.Timestamp:
    """
    根据「除权除息日 ex_date」推导该分红对应的 报告期期末日 report_period_end。
    核心规则：分红一定发生在「报告期结束 + 一段时间（公告/股东大会/登记）」之后。
    因此我们取 距离 ex_date 最近、且距离 ex_date 至少有 min_days_lag 天 滞后的 季度末（3/31、6/30、9/30、12/31）。
    该算法无需硬编码月份分支，同时天然兼容 A 股 年报/中报 / Q1 / Q3 四种分红场景（2024 年新规后季度分红逐渐普及）。

    例：
      ex=2024-04-26 (双汇 2023 年度):
        2024-03-31 → 距 04-26 仅 26 天（<40，太短，不可能是 Q1 分红）✗
        2023-12-31 → 距 04-26 有 117 天（≥40）✓  → report_period_end = 2023-12-31  ✓
      ex=2023-09-11 (双汇 2023 中报):
        2023-09-30 → 未来（-19 天）✗
        2023-06-30 → 73 天 ✓                  → report_period_end = 2023-06-30  ✓
      ex=2024-05-10 (某股 2024 Q1):
        2024-03-31 → 40 天 ✓                  → report_period_end = 2024-03-31  ✓
      ex=2024-11-20 (某股 2024 Q3):
        2024-09-30 → 51 天 ✓                  → report_period_end = 2024-09-30  ✓
    """
    ex_norm = pd.Timestamp(ex_d).normalize()
    candidates = []
    y = ex_norm.year
    for yy in (y + 1, y, y - 1, y - 2):
        for (mm, dd) in ((12, 31), (9, 30), (6, 30), (3, 31)):
            try:
                candidates.append(pd.Timestamp(year=yy, month=mm, day=dd))
            except Exception:
                pass
    valid = [c for c in candidates if (ex_norm - c).days >= min_days_lag]
    if not valid:
        ym = ex_norm.month
        if ym <= 6:
            return pd.Timestamp(year=ex_norm.year - 1, month=12, day=31)
        else:
            return pd.Timestamp(year=ex_norm.year, month=6, day=30)
    valid.sort(reverse=True)
    return valid[0]


def _infer_slot_and_rpe(ex_d, report_year=None, report_date=None):
    """
    V13 修正：优先使用报告期信息（report_year, report_date）推断分红类型，
    仅当报告期信息缺失时才回退到基于除息日的滞后区间推断。

    参数：
      ex_d: 除息日期
      report_year: 报告年度（字符串或整数，如 "2025" 或 2025）
      report_date: 报告期期末日（如 "2025-06-30" 或 pd.Timestamp）

    返回 (slot, rpe)
    """
    ex_norm = pd.Timestamp(ex_d).normalize()
    y, m, d = ex_norm.year, ex_norm.month, ex_norm.day

    if report_date is not None:
        try:
            rpe_ts = pd.Timestamp(report_date).normalize()
            mm, dd = rpe_ts.month, rpe_ts.day
            if mm == 12 and dd == 31:
                return "Y", rpe_ts
            elif mm == 6 and dd == 30:
                return "H", rpe_ts
            elif mm == 9 and dd == 30:
                return "Q3", rpe_ts
            elif mm == 3 and dd == 31:
                return "Q1", rpe_ts
        except Exception:
            pass

    if report_year is not None:
        try:
            ry = int(report_year)
            candidates = []

            def _valid_cand(slot_name, rpe_ts, lag_min, lag_max, prio):
                if rpe_ts is None:
                    return None
                if rpe_ts >= ex_norm:
                    return None
                lag = (ex_norm - rpe_ts).days
                if not (lag_min <= lag <= lag_max):
                    return None
                return (prio, lag, slot_name, rpe_ts)

            candidates.append(_valid_cand("Y", pd.Timestamp(year=ry, month=12, day=31),
                                          70, 330, prio=1))
            candidates.append(_valid_cand("H", pd.Timestamp(year=ry, month=6, day=30),
                                          30, 270, prio=1))
            candidates.append(_valid_cand("Q3", pd.Timestamp(year=ry, month=9, day=30),
                                          10, 210, prio=2))
            candidates.append(_valid_cand("Q1", pd.Timestamp(year=ry, month=3, day=31),
                                          30, 150, prio=3))

            cands = [c for c in candidates if c is not None]
            if len(cands) > 0:
                cands.sort(key=lambda t: (t[0], t[1]))
                _, _, slot_sel, rpe_sel = cands[0]
                return slot_sel, rpe_sel
        except Exception:
            pass

    def _valid_cand(slot_name, rpe_ts, lag_min, lag_max, prio):
        if rpe_ts is None:
            return None
        if rpe_ts >= ex_norm:
            return None
        lag = (ex_norm - rpe_ts).days
        if not (lag_min <= lag <= lag_max):
            return None
        return (prio, lag, slot_name, rpe_ts)

    candidates = []
    candidates.append(_valid_cand("Y", pd.Timestamp(year=y-1, month=12, day=31),
                                  70, 330, prio=1))
    candidates.append(_valid_cand("Y", pd.Timestamp(year=y-2, month=12, day=31),
                                  365+70, 365+360, prio=1))
    candidates.append(_valid_cand("H", pd.Timestamp(year=y, month=6, day=30),
                                  30, 270, prio=1))
    candidates.append(_valid_cand("H", pd.Timestamp(year=y-1, month=6, day=30),
                                  365+30, 365+270, prio=1))
    candidates.append(_valid_cand("Q3", pd.Timestamp(year=y, month=9, day=30),
                                  10, 210, prio=2))
    candidates.append(_valid_cand("Q3", pd.Timestamp(year=y-1, month=9, day=30),
                                  365+10, 365+210, prio=2))
    candidates.append(_valid_cand("Q1", pd.Timestamp(year=y, month=3, day=31),
                                  30, 150, prio=3))
    candidates.append(_valid_cand("Q1", pd.Timestamp(year=y-1, month=3, day=31),
                                  365+30, 365+150, prio=3))

    cands = [c for c in candidates if c is not None]
    if len(cands) > 0:
        cands.sort(key=lambda t: (t[0], t[1]))
        _, _, slot_sel, rpe_sel = cands[0]
        return slot_sel, rpe_sel

    if m in (5, 6, 7):
        slot = "Y"; rpe = pd.Timestamp(year=y-1, month=12, day=31)
    elif m in (8, 9, 10):
        slot = "H"; rpe = pd.Timestamp(year=y, month=6, day=30)
    elif m in (11, 12):
        slot = "Q3"; rpe = pd.Timestamp(year=y, month=9, day=30)
    elif m in (1, 2):
        slot = "Q3"; rpe = pd.Timestamp(year=y-1, month=9, day=30)
    elif m in (3, 4):
        slot = "Y"; rpe = pd.Timestamp(year=y-1, month=12, day=31)
    else:
        slot = "Y"; rpe = pd.Timestamp(year=y-1, month=12, day=31)
    return slot, rpe


def _earliest_release_date(rpe) -> pd.Timestamp:
    """
    根据「报告期期末 rpe」推导该财报**法律上最早可能公开发布的日期**（保守下界，用作前向含权的硬约束）。

    A 股监管层对财报披露日期有法定最晚日期限制，且实际操作中交易所不会允许比下表更早
    的大规模披露（用于防止任何形式的前视偏差）：

      报告期       法定最晚披露日   保守最早可能发布日（本函数返回）
      ----------   -------------   ---------------------------
      年报 12/31    次年 4/30       次年 3/1   （大部分公司 3 月中下旬才开始集中披露）
      中报 6/30     当年 8/31       当年 7/1   （7 月起开始有中报）
      Q3   9/30     当年 10/31      当年 10/15（三季报通常 10 月中下旬披露）
      Q1   3/31     当年 4/30       当年 4/10 （一季报通常 4 月中下旬披露）

    对未知季度：fallback = rpe + 45 天。
    """
    r = pd.Timestamp(rpe).normalize()
    mm, dd = r.month, r.day
    if mm == 12 and dd == 31:              # 年报
        return pd.Timestamp(year=r.year + 1, month=3,  day=1)
    elif mm == 6  and dd == 30:             # 中报
        return pd.Timestamp(year=r.year,     month=7,  day=1)
    elif mm == 9  and dd == 30:             # Q3
        return pd.Timestamp(year=r.year,     month=10, day=15)
    elif mm == 3  and dd == 31:             # Q1
        return pd.Timestamp(year=r.year,     month=4,  day=10)
    return r + pd.Timedelta(days=45)


def _uv_calc_daily_div_yield(div_df, stock_hist_series: pd.Series):
    """V12 版（用户最终口径）：按报告期 slot 去重计息 + slot 单笔自然过期撤回。

    用户最终定义（反复强调）：
      计息周期内，最多只能出现一次 年度分红 + 三季度分红 + 中期分红 + 一季度分红。
      若同一天花板出现两次年度分红（如 2021 年度和 2022 年度）就是计算错误！
      同理：两次中期/两次 Q1/两次 Q3 均为错。每个季度类型最多只能出现一次！

    V12 新增【自然过期撤回】修复（由长江电力 600900 反例触发）：
      若某 slot 多年不再发新分红（如长江电力 2009 年后再无中期 H 分红），
      旧算法只有"下一笔同 slot 激活→撤回当前"的覆盖逻辑，
      导致 2009 年 H 分红永久保留到 2026 年（累计了十七年前的分红！完全失真）。
      修复：对每笔分红单独设置"存活上限" MAX_LIFESPAN_DAYS = 550 天（≈18 个月），
      覆盖撤回日 与 自然过期撤回日 取较早者，确保某 slot 停发后旧笔最多再保留约 1.5 年。
      A 股正常年度分红间隔约 360~420 天、中期 300~360 天、Q3/Q1 更短，550 天足够覆盖任何延迟。

    形式化：对每个交易日 T，
      D_total(T) = Σ_{slot ∈ {Y, H, Q1, Q3}} latest per_sh where:
          slot 匹配；activate ≤ T < withdraw_date。
      withdraw_date = min( next_same_slot_activate, activate + 550 )
      同 slot 存在多笔时，按 activate 升序滚动，每笔到 withdraw_date 即被撤回。

    算例（用户原话，双汇 2023-04-28）：
      笔 A：2021 年度分红，ex=2022-06-10，slot=Y，金额=1.000
      笔 B：2022 中期分红，ex=2022-09-12，slot=H，金额=0.898
      笔 C：2022 年度分红，ex=2023-04-27，slot=Y，金额=1.000
      → slot=Y，笔 C 的 activate 比笔 A 近 → C 覆盖日 = C.activate → A 在 C.activate 被撤回
      → D_total(2023-04-28) = slotY(C: 1.000) + slotH(B: 0.898) + slotQ1(0) + slotQ3(0) = 1.898 ✔️
      （不再错误地把 A+C 都算进来得到 2.898！也不会把十七年前的旧 H 算进来。）

    实现：slot 事件法。同 slot 组内按 activate 升序 (d0, d1, d2... dN-1)：
      emit (d_i.activate, +d_i.ps)
      withdraw_i = min( d_{i+1}.activate (若有), d_i.activate + 550 )
      emit (withdraw_i, -d_i.ps)
    """
    MAX_LIFESPAN_DAYS = 550  # ≈ 18 个月：A 股两次年度分红最长不会超过这个间隔
    trade_idx = stock_hist_series.index.sort_values()

    slot_items_dict = {}  # key: (activate_norm, slot)  value: max(per_sh)  — 防数据库重复行 / akshare单位换算失败导致同(activate,slot)重复双计
    if len(div_df) > 0:
        div_df = div_df.copy()
        try:
            div_df["_ex_norm"] = pd.to_datetime(div_df["ex_date"], errors="coerce").dt.normalize()
            div_df = div_df.dropna(subset=["_ex_norm"]).copy()
            div_df["_per_sh"] = pd.to_numeric(div_df.get("per_share_dividend", 0), errors="coerce").fillna(0.0).round(5)
            div_df = div_df[div_df["_per_sh"] > 0].copy()
            # 去掉完全重复行（同一除息日+同每股金额重复入库）
            div_df = div_df.drop_duplicates(subset=["_ex_norm", "_per_sh"]).copy()
        except Exception:
            pass

        for _, r in div_df.iterrows():
            try:
                exd_norm = r["_ex_norm"] if "_ex_norm" in r and pd.notna(r["_ex_norm"]) else pd.Timestamp(r["ex_date"]).normalize()
                per_sh = float(r["_per_sh"]) if "_per_sh" in r and pd.notna(r["_per_sh"]) else float(r.get("per_share_dividend") or 0.0)
                if per_sh <= 0:
                    continue
                # 防御：现实中 A 股单笔分红 per_sh 几乎从未超过 5 元/股（双汇最高 2020 年度=1.60 元）。
                # 如果 akshare/东财 列名变更导致 /10 换算漏做，会出现 10×、20× 假值（如 16.0 而非 1.6），直接把股息率推到 60%+。
                # 这里 clamp 掉异常值，避免整图失真。
                if per_sh > 6.0:
                    candidate = per_sh / 10.0
                    if candidate <= 6.0:
                        # 大概率是 "10派X元" 被误按每股存（没 /10），自动修正
                        per_sh = candidate
                    else:
                        continue
                report_year_val = r.get("report_year")
                report_date_val = r.get("report_date")
                slot, rpe = _infer_slot_and_rpe(exd_norm, report_year=report_year_val, report_date=report_date_val)
                # V6 双重前视约束
                hard_earliest       = _earliest_release_date(rpe) + pd.Timedelta(days=15)
                practical_earliest  = exd_norm - pd.Timedelta(days=15)
                known_from = max(hard_earliest, practical_earliest)
                activate = max(known_from, exd_norm)
                key = (activate, slot)
                # 同 activate、同 slot 的多条记录，只保留 per_sh 最大的一条（防重复入库/接口重复返回）
                if key not in slot_items_dict or per_sh > slot_items_dict[key]:
                    slot_items_dict[key] = per_sh
            except Exception:
                continue

    slot_items = [(act, slot, ps) for (act, slot), ps in slot_items_dict.items()]

    if len(slot_items) == 0:
        zeros = pd.Series(0.0, index=trade_idx, dtype="float64")
        return zeros, zeros.copy()

    from collections import defaultdict
    by_slot = defaultdict(list)
    for act, slot, ps in slot_items:
        by_slot[slot].append((act, ps))
    events = []
    LIFESPAN_TD = pd.Timedelta(days=MAX_LIFESPAN_DAYS)
    for slot, lst in by_slot.items():
        lst.sort(key=lambda x: x[0])
        # 同 slot 内、若 activate 相同（罕见），只保留 per_sh 最大那一条（去重已在上面 key 级做过，这里再保险）
        dedup_lst = []
        last_act = None
        for a, p in lst:
            if a == last_act and len(dedup_lst) > 0:
                if p > dedup_lst[-1][1]:
                    dedup_lst[-1] = (a, p)
            else:
                dedup_lst.append((a, p))
            last_act = a
        n = len(dedup_lst)
        for i in range(n):
            act_i, ps_i = dedup_lst[i]
            # 【V12】撤回日 = min( 下一笔同slot激活日（若有）, 自然过期日(activate+550天) )
            natural_expire = act_i + LIFESPAN_TD
            if i + 1 < n:
                next_act = dedup_lst[i + 1][0]
                withdraw_i = min(next_act, natural_expire)
            else:
                withdraw_i = natural_expire
            # 激活事件
            events.append((act_i, ps_i))
            # 撤回事件（两种触发：覆盖 / 自然过期，取先触发者）
            events.append((withdraw_i, -ps_i))

    if len(events) == 0:
        zeros = pd.Series(0.0, index=trade_idx, dtype="float64")
        return zeros, zeros.copy()
    ev_df = pd.DataFrame(events, columns=["date", "delta"])
    ev_by_day = ev_df.groupby("date", as_index=True)["delta"].sum().sort_index()
    D_sparse = ev_by_day.cumsum()
    union_idx = D_sparse.index.union(trade_idx).sort_values()
    rolling_1y = (D_sparse.reindex(union_idx).ffill()
                            .reindex(trade_idx)
                            .ffill()
                            .fillna(0.0)
                            .clip(lower=0.0))
    yield_pct = pd.Series(index=trade_idx, dtype="float64")
    valid_close = stock_hist_series.reindex(trade_idx)
    mask = (rolling_1y > 0) & (valid_close > 0)
    yield_pct.loc[mask] = (rolling_1y.loc[mask] / valid_close.loc[mask]) * 100.0
    yield_pct = yield_pct.ffill().fillna(0.0)
    rolling_1y = rolling_1y.ffill().fillna(0.0)
    return rolling_1y, yield_pct


def _backtest_undervalue(symbol: str, principal: float,
                         sz_buy_pe=None, sz_sell_pe=None,
                         buy_div_yield_threshold=None, sell_div_yield_threshold=None,
                         buy_date=None, buy_price=None):
    """个股低估回测核心逻辑（V4：深证PE × 股息率 二维版，已移除个股PE）。

    【买入/复投条件判断规则（V4）】：
      设 szB = 深证买入阈值 启用；divB = 买入股息率阈值 启用
          C_sz  = 深证PE <= sz_buy_pe
          C_div = 当日股息率% > buy_div_yield_threshold
      ┌────────────────────┬───────────────────────────────┐
      │ 条件组合            │ 触发公式                      │
      ├────────────────────┼───────────────────────────────┤
      │ szB=1 且 divB=1     │ C_sz AND C_div                │
      │ szB=1 且 divB=0     │ C_sz                          │
      │ szB=0 且 divB=1     │ C_div                         │
      │ 两者全不启用         │ 「分红当日」直接按收盘价复投  │
      └────────────────────┴───────────────────────────────┘

    【卖出条件判断规则（V4）】：
      设 szS = 深证卖出阈值 启用；divS = 卖出股息率阈值 启用
          S_sz  = 深证PE > sz_sell_pe
          S_div = 当日股息率% < sell_div_yield_threshold
      ┌────────────────────┬───────────────────────────────┐
      │ 条件组合            │ 触发公式                      │
      ├────────────────────┼───────────────────────────────┤
      │ szS=1 且 divS=1     │ S_sz AND S_div                │
      │ szS=1 且 divS=0     │ S_sz                          │
      │ szS=0 且 divS=1     │ S_div                         │
      │ 两者全不启用         │ 什么都不做，继续持有          │
      └────────────────────┴───────────────────────────────┘

    支持 买入→卖出→再买入→再卖出 的多轮循环，分阶段统计收益，并汇总总收益。

    参数:
      symbol: 6位A股代码
      principal: 初始买入本金（元）
      sz_buy_pe: 可选（非必填），深证指数 PE 买入阈值。留空=不启用深证PE过滤。
      sz_sell_pe: 可选（非必填），深证指数 PE 卖出阈值。留空=不启用深证PE过滤。
      buy_div_yield_threshold: 可选，买入股息率阈值(%)。填了则当日股息率>阈值才考虑买入。
      sell_div_yield_threshold: 可选，卖出股息率阈值(%)。填了则当日股息率<阈值就考虑卖出。
      buy_date: 可选，自定义买入日期。指定后不再自动找低估日。
      buy_price: 可选，自定义买入价（不复权）。仅当 buy_date 已指定时有效。
    返回: {ok, summary, events: [..], stages: [..], msg}
    """
    symbol = _ri_normalize_symbol(symbol)
    if len(symbol) != 6 or not symbol.isdigit():
        return {"ok": False, "msg": "股票代码格式不正确（需要6位数字）", "summary": None, "events": [], "stages": []}
    if not isinstance(principal, (int, float)) or principal <= 0:
        return {"ok": False, "msg": "买入本金必须为正数", "summary": None, "events": [], "stages": []}
    if buy_div_yield_threshold is not None and (not isinstance(buy_div_yield_threshold, (int, float)) or buy_div_yield_threshold < 0):
        return {"ok": False, "msg": "买入股息率阈值必须 ≥ 0（或留空不启用），单位 %，例如填 5 表示 5%", "summary": None, "events": [], "stages": []}
    if sell_div_yield_threshold is not None and (not isinstance(sell_div_yield_threshold, (int, float)) or sell_div_yield_threshold < 0):
        return {"ok": False, "msg": "卖出股息率阈值必须 ≥ 0（或留空不启用），单位 %，例如填 1 表示 1%", "summary": None, "events": [], "stages": []}
    # sz_buy_pe / sz_sell_pe 非必填：允许留空；如果填了必须是正数
    if sz_buy_pe is not None and isinstance(sz_buy_pe, (int, float)) and sz_buy_pe <= 0:
        sz_buy_pe = None  # 填了0/负数视为未启用
    if sz_sell_pe is not None and isinstance(sz_sell_pe, (int, float)) and sz_sell_pe <= 0:
        sz_sell_pe = None

    # ============================================================
    # Part 1. 参数预处理 + 拉取数据
    # ============================================================
    raw_buy_date_in = buy_date
    raw_buy_price_in = buy_price
    user_specified_buy_date = False
    parsed_buy_date = None
    if buy_date is not None and str(buy_date).strip() != "":
        user_specified_buy_date = True
        try:
            if isinstance(buy_date, (pd.Timestamp, dt.datetime, dt.date)):
                parsed_buy_date = pd.Timestamp(buy_date).normalize()
            else:
                parsed_buy_date = pd.Timestamp(str(buy_date).strip()).normalize()
        except Exception:
            return {"ok": False, "msg": "自定义买入日期格式不正确（支持 YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD 等）", "summary": None, "events": [], "stages": []}
        if buy_price is not None and str(buy_price).strip() != "":
            try:
                bp = float(buy_price)
                if bp <= 0:
                    return {"ok": False, "msg": "自定义买入价格必须为正数（或留空=取当日收盘价）", "summary": None, "events": [], "stages": []}
            except Exception:
                return {"ok": False, "msg": "自定义买入价格格式不正确", "summary": None, "events": [], "stages": []}

    today = pd.Timestamp(dt.date.today()).normalize()

    # --- 拉取深证 PE 历史（如果 sz_buy_pe 或 sz_sell_pe 任一启用了才必须有；否则即使没数据也能走纯个股/股息率逻辑）---
    sz_pe_hist_raw = pd.Series(dtype="float64")
    need_sz_pe = (sz_buy_pe is not None) or (sz_sell_pe is not None)
    if need_sz_pe:
        sz_pe_hist_raw = _uv_load_shenzhen_pe_history()
        if len(sz_pe_hist_raw) == 0:
            return {"ok": False, "msg": "您启用了深证指数PE阈值，但无深证指数（sz_market）估值历史数据：请先在「深证指数估值」页刷新抓取完整历史再回测",
                    "summary": None, "events": [], "stages": []}
    else:
        # 没启用深证PE阈值：即使没历史也没关系；后面 forward-fill 时填占位
        sz_pe_hist_raw = _uv_load_shenzhen_pe_history()  # 能拉到就拉，拉不到也不报错

    sz_start = sz_pe_hist_raw.index.min() if len(sz_pe_hist_raw) > 0 else parsed_buy_date or (today - pd.Timedelta(days=365 * 10))
    sz_end = sz_pe_hist_raw.index.max() if len(sz_pe_hist_raw) > 0 else today

    hist_start = sz_start
    if user_specified_buy_date:
        hist_start = min(sz_start, parsed_buy_date)

    # --- 拉取个股 不复权 日线 ---
    stock_hist = _ri_fetch_raw_daily(symbol, hist_start - pd.Timedelta(days=7), sz_end + pd.Timedelta(days=7))
    if len(stock_hist) == 0:
        return {"ok": False, "msg": f"无法获取 {symbol} 的不复权历史日线（akshare/腾讯抓取失败）",
                "summary": None, "events": [], "stages": []}

    # --- 拉取个股 分红历史（股息率计算依赖）---
    div_df = _ri_fetch_dividends(symbol)

    # ============================================================
    # Part 2. 数据预处理：将 深证PE / 股息率 forward-fill 到所有个股交易日
    # ============================================================
    trade_dates = stock_hist.index.sort_values()
    # 深证 PE：按 个股交易日 重新索引 + ffill
    sz_pe_ff = pd.Series(index=trade_dates, dtype="float64")
    if len(sz_pe_hist_raw) > 0:
        sz_pe_ff = sz_pe_hist_raw.reindex(trade_dates, method="ffill")
        if sz_pe_ff.isna().any() and len(sz_pe_hist_raw) > 0:
            first_valid_sz = sz_pe_hist_raw.iloc[0]
            sz_pe_ff = sz_pe_ff.fillna(first_valid_sz)

    # 每日滚动1年股息率（完全基于本地分红+收盘价，无外部接口依赖）
    rolling_div_1y, div_yield_ff = _uv_calc_daily_div_yield(div_df, stock_hist)
    div_yield_ff = div_yield_ff.reindex(trade_dates).ffill()

    # ============================================================
    # Part 3. 确定首次买入日期/价格（首轮）
    # ============================================================
    if user_specified_buy_date:
        first_buy_date = parsed_buy_date
        if first_buy_date > today:
            return {"ok": False, "msg": f"自定义买入日期 {first_buy_date.date()} 晚于今日 {today.date()}",
                    "summary": None, "events": [], "stages": []}
        if first_buy_date > stock_hist.index.max():
            return {"ok": False,
                    "msg": (f"自定义买入日期 {first_buy_date.date()} 晚于可获取的个股历史日线截止日 "
                            f"{stock_hist.index.max().date()}（请选择更早的日期或手动指定价格）"),
                    "summary": None, "events": [], "stages": []}
        buy_reason = f"用户自定义买入日期 {first_buy_date.date()}"
        if buy_price is not None and str(buy_price).strip() != "":
            first_buy_price = float(buy_price)
            first_buy_dt_actual = first_buy_date
            buy_reason += f" · 用户自定义买入价 ¥{first_buy_price:.2f}"
        else:
            first_buy_dt_actual, _bp = _ri_nearest_close_after(stock_hist, first_buy_date)
            if _bp is None or _bp <= 0:
                return {"ok": False,
                        "msg": f"自定义买入日期 {first_buy_date.date()} 附近找不到有效交易日收盘价（请稍后重试或手动指定买入价）",
                        "summary": None, "events": [], "stages": []}
            first_buy_price = float(_bp)
            buy_reason += f" · 自动取当日/最近交易日收盘价 ¥{first_buy_price:.2f}"
    else:
        # 自动择时模式（非自定义买入日）：必须至少启用一个「买入过滤阈值」
        if sz_buy_pe is None and buy_div_yield_threshold is None:
            return {"ok": False,
                    "msg": "您未启用深证PE买入阈值、买入股息率阈值中的任何一个，也没有填写「买入日期」→ 不知道该按什么标准择首次买入时机。请至少填写：①「买入日期」 或 ② 启用任一买入阈值（深证PE/股息率）",
                    "summary": None, "events": [], "stages": []}
        # 调用 find_buy_date：深证 + 股息率 二维条件
        first_buy_date, skip_cnt, buy_reason = _uv_find_buy_date(
            sz_pe_hist_raw, div_yield_ff,
            sz_buy_pe, buy_div_yield_threshold
        )
        if first_buy_date is None:
            return {"ok": False, "msg": "找不到满足条件的买入日期：" + buy_reason,
                    "summary": None, "events": [], "stages": []}
        first_buy_dt_actual, _bp_auto = _ri_nearest_close_after(stock_hist, first_buy_date)
        if _bp_auto is None or _bp_auto <= 0:
            return {"ok": False, "msg": f"找不到买入日期 {first_buy_date.date()} 附近的个股有效收盘价",
                    "summary": None, "events": [], "stages": []}
        first_buy_price = float(_bp_auto)

    initial_shares = _ri_floor_100(principal / first_buy_price)
    if initial_shares <= 0:
        return {"ok": False,
                "msg": f"本金过少，按买入价 ¥{first_buy_price:.2f} 无法买够 100 股（至少需要 ¥{first_buy_price * 100:.2f}）",
                "summary": None, "events": [], "stages": []}

    # ============================================================
    # Part 4. 条件判断函数（V4：深证PE × 股息率 二维组合）
    # ============================================================
    sz_buy_enabled = (sz_buy_pe is not None)
    div_buy_enabled = (buy_div_yield_threshold is not None)
    sz_sell_enabled = (sz_sell_pe is not None)
    div_sell_enabled = (sell_div_yield_threshold is not None)

    def _fmt2(v):
        return f"{v:.2f}" if v is not None else "—"

    def _check_buy_ok(sz_pe_val, div_yield_pct_val):
        """买入/复投条件：按用户V4二维逻辑表判断。
        返回 (ok: bool, reason: str)
        """
        C_sz  = (sz_pe_val is not None) and (sz_pe_val <= sz_buy_pe) if sz_buy_enabled else True
        C_div = (div_yield_pct_val is not None) and (div_yield_pct_val > buy_div_yield_threshold) if div_buy_enabled else False

        sub_reasons = []
        if sz_buy_enabled:
            sub_reasons.append(("✓" if C_sz else "✗") + f" 深证PE={_fmt2(sz_pe_val)}<={sz_buy_pe:.2f}")
        if div_buy_enabled:
            sub_reasons.append(("✓" if C_div else "✗") + f" 股息率={_fmt2(div_yield_pct_val)}%>{buy_div_yield_threshold:.2f}%")

        if sz_buy_enabled and div_buy_enabled:
            ok = bool(C_sz and C_div)
        elif sz_buy_enabled:
            ok = bool(C_sz)
        elif div_buy_enabled:
            ok = bool(C_div)
        else:
            # 两个买入阈值全空：返回 False，主循环会用「分红当日直接复投」逻辑
            ok = False
            sub_reasons = ["全空买入阈值 → 不使用条件判断，仅在「分红当日」直接复投"]
        return ok, "  ".join(sub_reasons)

    def _check_sell_ok(sz_pe_val, div_yield_pct_val):
        """卖出条件：按用户V4二维逻辑表判断。
        返回 (ok: bool, reason: str)
        """
        S_sz  = (sz_pe_val is not None) and (sz_pe_val > sz_sell_pe) if sz_sell_enabled else False
        S_div = (div_yield_pct_val is not None) and (div_yield_pct_val < sell_div_yield_threshold) if div_sell_enabled else False

        sub_reasons = []
        if sz_sell_enabled:
            sub_reasons.append(("✓" if S_sz else "✗") + f" 深证PE={_fmt2(sz_pe_val)}>{sz_sell_pe:.2f}")
        if div_sell_enabled:
            sub_reasons.append(("✓" if S_div else "✗") + f" 股息率={_fmt2(div_yield_pct_val)}%<{sell_div_yield_threshold:.2f}%")

        if sz_sell_enabled and div_sell_enabled:
            ok = bool(S_sz and S_div)
        elif sz_sell_enabled:
            ok = bool(S_sz)
        elif div_sell_enabled:
            ok = bool(S_div)
        else:
            # 两个卖出阈值全空：永远不卖，持有估值到最后
            ok = False
            sub_reasons = ["全空卖出阈值 → 永不主动卖出，直到回测结束按最新价估值"]
        return ok, "  ".join(sub_reasons)

    # ============================================================
    # Part 5. 状态机：多轮买卖循环 + 分阶段统计
    # ============================================================
    date_end = min(today, sz_end, stock_hist.index.max())

    # 分红映射（ex_date → dict）
    div_map = {}
    if len(div_df) > 0:
        for _, r in div_df.iterrows():
            ed = r["ex_date"] if hasattr(r["ex_date"], "date") else pd.Timestamp(r["ex_date"])
            div_map[ed.normalize()] = {
                "gift": float(r["per_share_gift"]),
                "transfer": float(r["per_share_transfer"]),
                "div": float(r["per_share_dividend"]),
            }

    # 只遍历「有收盘价的个股交易日」，且 >= first_buy_dt_actual
    iter_dates = trade_dates[(trade_dates >= first_buy_dt_actual) & (trade_dates <= date_end)]

    # 全局状态
    cash = float(principal)
    shares = 0
    events = []
    stages = []  # 每轮交易的完整记录

    # 状态机
    STATE_CASH = "CASH"      # 空仓：等待买入机会
    STATE_HOLDING = "HOLD"   # 持有：等待卖出机会 + 可复投
    state = STATE_CASH

    current_round = 0          # 当前第几轮（从1开始）
    last_round_buy_ym = None   # 同个自然月内只择一次「再买入」（避免同一低估窗口内天天判断）
    last_reinvest_ym = None    # 同个自然月内只复投一次

    # 记录当前轮的元数据
    round_info = None

    # 辅助：从两个 Series 读取当日值
    def _v_today(d0):
        """返回 (sz_pe, div_yield_pct) 当日值"""
        s = float(sz_pe_ff.loc[d0]) if (d0 in sz_pe_ff.index and pd.notna(sz_pe_ff.loc[d0])) else None
        y = float(div_yield_ff.loc[d0]) if (d0 in div_yield_ff.index and pd.notna(div_yield_ff.loc[d0])) else None
        return s, y

    def _start_new_round(round_no, start_dt, buy_price_val, buy_shares_val, note_str):
        """开始新一轮：记录买入事件，初始化 round_info"""
        nonlocal cash, shares, round_info
        cost = float(buy_shares_val) * float(buy_price_val)
        cash -= cost
        shares += buy_shares_val

        sz_pe_v, dy_v = _v_today(start_dt)

        events.append({
            "round": int(round_no),
            "date": start_dt.strftime("%Y-%m-%d"),
            "action": "买入",
            "price": round(float(buy_price_val), 4),
            "shares_delta": int(buy_shares_val),
            "shares_total": int(shares),
            "cash_delta": round(-cost, 2),
            "cash_after": round(float(cash), 2),
            "sz_pe": round(float(sz_pe_v), 2) if sz_pe_v is not None else None,
            "div_yield_pct": round(float(dy_v), 2) if dy_v is not None else None,
            "per_share_dividend": None,
            "per_share_gift": None,
            "per_share_transfer": None,
            "note": f"[第{round_no}轮] {note_str}",
        })
        return {
            "round": int(round_no),
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "start_dt": start_dt,
            "buy_price": float(buy_price_val),
            "buy_shares": int(buy_shares_val),
            "buy_amount": round(cost, 2),
            "reinvest_cost_total": 0.0,    # 该轮内累计复投入本金
            "dividend_total": 0.0,         # 该轮内累计分红现金
            "end_date": None,
            "end_dt": None,
            "sell_price": None,
            "sell_amount": None,
            "return_pct": None,
            "annual_return_pct": None,
            "hold_days": None,
            "status": "进行中",
            "end_reason": None,
        }

    def _close_round(rinfo, end_dt, sell_price_val, reason_str, is_sold=True):
        """结束当前轮：卖出/估值，计算该轮收益，推入 stages"""
        nonlocal cash, shares
        if rinfo is None:
            return None

        sz_pe_v, dy_v = _v_today(end_dt)

        if is_sold:
            # 真实卖出
            sell_amt = float(shares) * float(sell_price_val)
            cash += sell_amt
            events.append({
                "round": int(rinfo["round"]),
                "date": end_dt.strftime("%Y-%m-%d"),
                "action": "卖出",
                "price": round(float(sell_price_val), 4),
                "shares_delta": -int(shares),
                "shares_total": 0,
                "cash_delta": round(sell_amt, 2),
                "cash_after": round(float(cash), 2),
                "sz_pe": round(float(sz_pe_v), 2) if sz_pe_v is not None else None,
                "div_yield_pct": round(float(dy_v), 2) if dy_v is not None else None,
                "per_share_dividend": None,
                "per_share_gift": None,
                "per_share_transfer": None,
                "note": f"[第{rinfo['round']}轮] {reason_str}",
            })
            total_invested = float(rinfo["buy_amount"]) + float(rinfo["reinvest_cost_total"]) - float(rinfo["dividend_total"])
            # 避免分母为0
            if total_invested <= 0:
                total_invested = max(float(rinfo["buy_amount"]), 1e-6)
            net_profit = sell_amt - total_invested
            ret_pct = net_profit / total_invested * 100.0
            rinfo.update({
                "end_date": end_dt.strftime("%Y-%m-%d"),
                "end_dt": end_dt,
                "sell_price": float(sell_price_val),
                "sell_amount": round(sell_amt, 2),
                "return_pct": round(ret_pct, 2),
                "status": "已卖出",
                "end_reason": reason_str,
            })
            shares = 0
        else:
            # 未卖出，按估值
            sell_amt = float(shares) * float(sell_price_val)
            events.append({
                "round": int(rinfo["round"]),
                "date": end_dt.strftime("%Y-%m-%d"),
                "action": "持有估值",
                "price": round(float(sell_price_val), 4),
                "shares_delta": 0,
                "shares_total": int(shares),
                "cash_delta": 0.0,
                "cash_after": round(float(cash), 2),
                "sz_pe": round(float(sz_pe_v), 2) if sz_pe_v is not None else None,
                "div_yield_pct": round(float(dy_v), 2) if dy_v is not None else None,
                "per_share_dividend": None,
                "per_share_gift": None,
                "per_share_transfer": None,
                "note": f"[第{rinfo['round']}轮] {reason_str}",
            })
            total_invested = float(rinfo["buy_amount"]) + float(rinfo["reinvest_cost_total"]) - float(rinfo["dividend_total"])
            if total_invested <= 0:
                total_invested = max(float(rinfo["buy_amount"]), 1e-6)
            net_profit = sell_amt - total_invested
            ret_pct = net_profit / total_invested * 100.0
            rinfo.update({
                "end_date": end_dt.strftime("%Y-%m-%d"),
                "end_dt": end_dt,
                "sell_price": float(sell_price_val),
                "sell_amount": round(sell_amt, 2),
                "return_pct": round(ret_pct, 2),
                "status": "持有中",
                "end_reason": reason_str,
            })

        # 计算持有天数 & 年化
        start_ts = rinfo["start_dt"]
        end_ts = rinfo["end_dt"]
        hd = (end_ts - start_ts).days + 1
        hy = max(hd / 365.25, 1e-6)
        ann_pct = 0.0
        ratio = (float(rinfo["buy_amount"]) + net_profit) / max(float(rinfo["buy_amount"]), 1e-6)
        if ratio > 0:
            ann_pct = (ratio ** (1.0 / hy) - 1.0) * 100.0
        rinfo["hold_days"] = int(hd)
        rinfo["annual_return_pct"] = round(ann_pct, 2)
        # 清理 datetime，方便 JSON 序列化
        rinfo_out = {k: v for k, v in rinfo.items() if not k.endswith("_dt")}
        stages.append(rinfo_out)
        return rinfo_out

    # ============================================================
    # Part 6. 主循环：按个股交易日逐日遍历
    # ============================================================
    all_buy_empty = (not sz_buy_enabled) and (not div_buy_enabled)
    for idx, d0 in enumerate(iter_dates):
        price_now = float(stock_hist.loc[d0])
        sz_pe_now, dy_now = _v_today(d0)

        # ------ 先处理分红送转（对任何持有股票的日子都生效） ------
        is_dividend_today = (d0 in div_map) and shares > 0
        if is_dividend_today:
            d = div_map[d0]
            before_s = int(shares)
            cash_div_amt = 0.0
            note_parts = []
            if d["div"] > 0:
                cash_div_amt = round(float(before_s) * float(d["div"]), 2)
                cash += cash_div_amt
                if round_info is not None:
                    round_info["dividend_total"] = float(round_info.get("dividend_total", 0.0)) + cash_div_amt
                note_parts.append(f"派现 ¥{cash_div_amt:.2f}（{before_s}股 × ¥{d['div']:.4f}/股）")
            if d["gift"] > 0 or d["transfer"] > 0:
                new_shares = int(before_s * (1 + d["gift"] + d["transfer"]))
                add_s = new_shares - before_s
                shares = new_shares
                note_parts.append(
                    (f"送股 {d['gift']:.4f}/股" if d['gift'] > 0 else "") +
                    (f" 转增 {d['transfer']:.4f}/股" if d['transfer'] > 0 else "") +
                    f" → 新增 {add_s} 股，总 {shares} 股"
                )
            events.append({
                "round": int(round_info["round"]) if round_info else 0,
                "date": d0.strftime("%Y-%m-%d"),
                "action": "分红送转",
                "price": None,
                "shares_delta": int(shares - before_s),
                "shares_total": int(shares),
                "cash_delta": round(cash_div_amt, 2),
                "cash_after": round(float(cash), 2),
                "sz_pe": round(float(sz_pe_now), 2) if sz_pe_now is not None else None,
                "div_yield_pct": round(float(dy_now), 2) if dy_now is not None else None,
                "per_share_dividend": round(float(d["div"]), 6) if d["div"] > 0 else 0.0,
                "per_share_gift": round(float(d["gift"]), 6) if d["gift"] > 0 else 0.0,
                "per_share_transfer": round(float(d["transfer"]), 6) if d["transfer"] > 0 else 0.0,
                "note": ("[第" + str(round_info["round"]) + "轮] " if round_info else "") + " · ".join(p for p in note_parts if p),
            })

        # ============================================================
        # 状态机分支 1：STATE_CASH 空仓 → 寻找买入机会（再买入）
        # ============================================================
        if state == STATE_CASH:
            # 首轮：直接买入
            if current_round == 0:
                current_round = 1
                last_round_buy_ym = first_buy_dt_actual.strftime("%Y-%m")
                round_info = _start_new_round(1, first_buy_dt_actual, first_buy_price, initial_shares, buy_reason)
                state = STATE_HOLDING
                last_reinvest_ym = None
                # 如果首轮买入日不是遍历的第一天（first_buy_dt_actual == d0 则跳过当天其他逻辑），
                # 如果 d0 > first_buy_dt_actual 则继续处理当日卖出/复投判断
                if d0 == first_buy_dt_actual:
                    continue
            else:
                # 非首轮：寻找再买入机会（月度窗口去重）
                # 注意：全空买入阈值时 STATE_CASH 状态下永远不会自动再买入（没有判断依据）
                if not all_buy_empty:
                    cur_ym = d0.strftime("%Y-%m")
                    if last_round_buy_ym is None or last_round_buy_ym != cur_ym:
                        ok, reason = _check_buy_ok(sz_pe_now, dy_now)
                        if ok and cash >= 100.0:
                            add_s = _ri_floor_100(cash / price_now)
                            if add_s > 0:
                                current_round += 1
                                last_round_buy_ym = cur_ym
                                round_info = _start_new_round(
                                    current_round, d0, price_now, add_s,
                                    f"[{cur_ym}窗口] 再买入机会：{reason}"
                                )
                                state = STATE_HOLDING
                                last_reinvest_ym = None
                                continue  # 买入当日不再卖出

        # ============================================================
        # 状态机分支 2：STATE_HOLDING 持有 → 先判断卖出，再判断复投
        # ============================================================
        if state == STATE_HOLDING and shares > 0:
            # ------ 卖出判断（V4二维组合） ------
            all_sell_empty = (not sz_sell_enabled) and (not div_sell_enabled)
            if not all_sell_empty:
                sell_ok, sell_reason = _check_sell_ok(sz_pe_now, dy_now)
                if sell_ok:
                    _close_round(round_info, d0, price_now, f"{sell_reason}，触发清仓卖出", is_sold=True)
                    round_info = None
                    state = STATE_CASH
                    last_reinvest_ym = None
                    continue

            # ------ 复投判断（持有中且有现金 ≥ 100 元） ------
            if cash >= 100.0:
                cur_ym = d0.strftime("%Y-%m")
                # 月度窗口去重：全空阈值时，只有「分红当日」才允许直接复投
                reinvest_ok, reinvest_reason = False, ""
                if all_buy_empty:
                    # 买入阈值全空 → 分红当日直接复投（按不复权收盘价）
                    if is_dividend_today:
                        reinvest_ok = True
                        reinvest_reason = "全空买入阈值 → 分红当日按收盘价直接复投"
                else:
                    reinvest_ok, reinvest_reason = _check_buy_ok(sz_pe_now, dy_now)

                if reinvest_ok:
                    if last_reinvest_ym is None or last_reinvest_ym != cur_ym:
                        add_s = _ri_floor_100(cash / price_now)
                        if add_s > 0:
                            cost = float(add_s) * float(price_now)
                            cash -= cost
                            shares += add_s
                            last_reinvest_ym = cur_ym
                            if round_info is not None:
                                round_info["reinvest_cost_total"] = float(round_info.get("reinvest_cost_total", 0.0)) + cost
                            sz_pe_v, dy_v = _v_today(d0)
                            events.append({
                                "round": int(round_info["round"]) if round_info else 0,
                                "date": d0.strftime("%Y-%m-%d"),
                                "action": "复投",
                                "price": round(float(price_now), 4),
                                "shares_delta": int(add_s),
                                "shares_total": int(shares),
                                "cash_delta": round(-cost, 2),
                                "cash_after": round(float(cash), 2),
                                "sz_pe": round(float(sz_pe_v), 2) if sz_pe_v is not None else None,
                                "div_yield_pct": round(float(dy_v), 2) if dy_v is not None else None,
                                "per_share_dividend": None,
                                "per_share_gift": None,
                                "per_share_transfer": None,
                                "note": (f"[第{round_info['round']}轮] [{cur_ym}窗口] 复投：{reinvest_reason}" if round_info else f"[{cur_ym}窗口] 复投：{reinvest_reason}"),
                            })

    # ============================================================
    # Part 7. 结束循环后：若仍在持有 → 估值收盘
    # ============================================================
    final_dt = None
    final_status = ""
    if state == STATE_HOLDING and shares > 0 and round_info is not None:
        hold_dt = min(today, stock_hist.index.max(), sz_end)
        valid_closes = stock_hist[stock_hist.index <= hold_dt]
        if len(valid_closes) > 0:
            hold_dt, hold_price = valid_closes.index[-1], float(valid_closes.iloc[-1])
        else:
            hold_price = first_buy_price
            hold_dt = first_buy_dt_actual
        # 友好显示"未触发卖出"的具体阈值
        no_trigger = []
        if sz_sell_enabled: no_trigger.append(f"深证PE始终未>{sz_sell_pe:.2f}")
        if div_sell_enabled: no_trigger.append(f"股息率始终未<{sell_div_yield_threshold:.2f}%")
        no_trigger_str = "；".join(no_trigger) if no_trigger else "（未启用任何卖出阈值，策略为一直持有至期末估值）"
        reason_end = f"仍持有（未触发卖出：{no_trigger_str}），按最新收盘价估值"
        _close_round(round_info, hold_dt, hold_price, reason_end, is_sold=False)
        final_dt = hold_dt
        final_status = reason_end
    elif state == STATE_CASH and len(stages) > 0:
        last_stage = stages[-1]
        final_dt = pd.Timestamp(last_stage["end_date"])
        final_status = f"已清仓（最近一轮：{last_stage.get('end_reason', '卖出')}）"
    else:
        final_dt = first_buy_dt_actual
        final_status = "无有效交易"

    # ============================================================
    # Part 8. 汇总统计：总收益率 / 年化（基于投入本金 vs 最终总资产）
    # ============================================================
    total_value_final = float(cash)
    if len(stages) > 0 and stages[-1]["status"] == "持有中":
        last_sa = float(stages[-1]["sell_amount"])
        total_value_final = float(cash) + last_sa

    hold_days_total = (final_dt - first_buy_dt_actual).days + 1
    hold_years_total = max(hold_days_total / 365.25, 1e-6)
    total_ret_pct = (total_value_final - float(principal)) / float(principal) * 100.0
    annual_ret_pct = ((total_value_final / float(principal)) ** (1.0 / hold_years_total) - 1.0) * 100.0

    sec_name, sec_suffix = _ri_fetch_sec_name(symbol)

    total_rounds = len(stages)
    total_invested_sum = sum(float(s["buy_amount"]) + float(s.get("reinvest_cost_total", 0.0)) - float(s.get("dividend_total", 0.0)) for s in stages)
    total_sell_sum = sum(float(s["sell_amount"]) for s in stages if s["sell_amount"])

    # —— 生成 buy_logic / sell_logic 的中文描述（V4 二维组合）——
    def _logic_buy_str():
        parts = []
        if sz_buy_enabled: parts.append(f"深证PE<={sz_buy_pe:.2f}")
        if div_buy_enabled: parts.append(f"股息率>{buy_div_yield_threshold:.2f}%")
        if not parts:
            return "未启用任何买入阈值（分红当日直接复投）"
        if sz_buy_enabled and div_buy_enabled:
            return parts[0] + "  AND  " + parts[1]
        return parts[0]

    def _logic_sell_str():
        parts = []
        if sz_sell_enabled: parts.append(f"深证PE>{sz_sell_pe:.2f}")
        if div_sell_enabled: parts.append(f"股息率<{sell_div_yield_threshold:.2f}%")
        if not parts:
            return "未启用任何卖出阈值（一直持有到期末估值）"
        if sz_sell_enabled and div_sell_enabled:
            return parts[0] + "  AND  " + parts[1]
        return parts[0]

    summary = {
        "symbol": symbol,
        "sec_name": sec_name or "",
        "sec_suffix": sec_suffix,
        "sz_buy_pe": float(sz_buy_pe) if sz_buy_pe is not None else None,
        "sz_sell_pe": float(sz_sell_pe) if sz_sell_pe is not None else None,
        "low_stock_pe": None,
        "high_stock_pe": None,
        "buy_div_yield_threshold": float(buy_div_yield_threshold) if buy_div_yield_threshold is not None else None,
        "sell_div_yield_threshold": float(sell_div_yield_threshold) if sell_div_yield_threshold is not None else None,
        "principal": float(principal),
        "buy_date": first_buy_dt_actual.strftime("%Y-%m-%d"),
        "buy_price": round(float(first_buy_price), 4),
        "initial_shares": int(initial_shares),
        "final_status": final_status,
        "final_date": final_dt.strftime("%Y-%m-%d") if final_dt else "",
        "remaining_cash": round(float(cash), 2),
        "total_value": round(float(total_value_final), 2),
        "hold_days": int(hold_days_total),
        "hold_years": round(hold_years_total, 3),
        "total_return_pct": round(total_ret_pct, 2),
        "annual_return_pct": round(annual_ret_pct, 2),
        "total_rounds": int(total_rounds),
        "total_invested": round(float(total_invested_sum), 2),
        "total_sell": round(float(total_sell_sum), 2),
        "debug_echo_params": {
            "symbol_in": symbol,
            "principal_in": float(principal),
            "low_stock_pe_in": None,
            "high_stock_pe_in": None,
            "sz_buy_pe_in": float(sz_buy_pe) if sz_buy_pe is not None else None,
            "sz_sell_pe_in": float(sz_sell_pe) if sz_sell_pe is not None else None,
            "buy_div_yield_in_pct": float(buy_div_yield_threshold) if buy_div_yield_threshold is not None else None,
            "sell_div_yield_in_pct": float(sell_div_yield_threshold) if sell_div_yield_threshold is not None else None,
            "buy_date_raw": str(raw_buy_date_in) if raw_buy_date_in is not None else None,
            "buy_date_parsed": parsed_buy_date.strftime("%Y-%m-%d") if parsed_buy_date is not None else None,
            "buy_price_raw": str(raw_buy_price_in) if raw_buy_price_in is not None else None,
            "user_specified_buy_date": bool(user_specified_buy_date),
            "mode": ("自定义买入日" if user_specified_buy_date else "（阈值自动择首次买入日）"),
            "buy_logic": _logic_buy_str(),
            "sell_logic": _logic_sell_str(),
        },
    }

    # ============================================================
    # Part 9. 最终返回前：递归清洗所有 NaN → None（保证 JSON 合法，前端不报错）
    # ============================================================
    def _sanitize_obj(o):
        """递归遍历 dict/list/scalar，把 float('nan') / pd.NA / inf 统一转为 None"""
        if o is None:
            return None
        if isinstance(o, dict):
            return {k: _sanitize_obj(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [_sanitize_obj(v) for v in o]
        if isinstance(o, bool):
            return o
        if isinstance(o, (int, float)):
            try:
                fv = float(o)
                import math as _math
                if _math.isnan(fv) or _math.isinf(fv):
                    return None
                return o
            except Exception:
                return o
        if isinstance(o, str):
            return o
        try:
            if pd.isna(o):
                return None
        except Exception:
            pass
        return o

    events_clean = _sanitize_obj(events) or []
    stages_clean = _sanitize_obj(stages) or []
    summary_clean = _sanitize_obj(summary) or {}
    return {"ok": True, "msg": "ok", "summary": summary_clean, "events": events_clean, "stages": stages_clean}


@app.route("/api/undervalue_backtest")
def api_undervalue_backtest():
    """
    个股低估回测接口（V4：深证PE × 股息率 二维组合，多轮买卖循环）。
    参数:
      ?symbol=600519                (必选, 6位数字)
      &principal=100000             (必选, 初始本金)
      &buy_div_yield=5              (可选, 买入股息率阈值%, 当日近1年股息率>此值 时满足买入过滤条件之一)
      &sell_div_yield=1             (可选, 卖出股息率阈值%, 当日近1年股息率<此值 时满足卖出过滤条件之一)
      &sz_buy_pe=20                 (可选, 非必填, 深证指数PE买入阈值; 留空=不启用深证PE过滤)
      &sz_sell_pe=40                (可选, 非必填, 深证指数PE卖出阈值; 留空=不启用深证PE过滤)
      &buy_date=2024-01-02          (可选, 自定义买入日期, 指定后不再自动查找低估日)
      &buy_price=10.15              (可选, 自定义买入价, 仅 buy_date 填写后生效; 留空=取当日收盘价)

    【买入/复投触发条件组合】：
      • 填了 sz_buy_pe 且 填了 buy_div_yield → sz_buy_pe 成立 AND buy_div_yield 成立
      • 填了 sz_buy_pe 但 buy_div_yield 没填 → 只需 sz_buy_pe 成立
      • sz_buy_pe 没填 但填了 buy_div_yield → 只需 buy_div_yield 成立
      • 两个买入阈值全空 → 每遇到「分红当日」直接按不复权收盘价复投
    【卖出触发条件组合】同理（AND/OR 逻辑镜像），最后一项两个阈值全空 → 永远不卖，期末按最新价估值。
    返回: {ok, summary, events: [..], stages: [..], msg}
    """
    try:
        sym = request.args.get("symbol", "").strip()
        p_str = request.args.get("principal", "").strip()
        bdy_str = request.args.get("buy_div_yield", "").strip()
        sdy_str = request.args.get("sell_div_yield", "").strip()
        sz_buy_str = request.args.get("sz_buy_pe", "").strip()
        sz_sell_str = request.args.get("sz_sell_pe", "").strip()
        bd_str = request.args.get("buy_date", "").strip()
        bp_str = request.args.get("buy_price", "").strip()
        if not sym or not p_str:
            return jsonify({"ok": False, "msg": "缺少必填参数：symbol / principal"}), 400
        principal = float(p_str)
        buy_div_y = float(bdy_str) if bdy_str else None
        sell_div_y = float(sdy_str) if sdy_str else None
        sz_buy_pe = float(sz_buy_str) if sz_buy_str else None
        sz_sell_pe = float(sz_sell_str) if sz_sell_str else None
        buy_dt = bd_str if bd_str else None
        buy_pr = bp_str if bp_str else None
        res = _backtest_undervalue(sym, principal, sz_buy_pe, sz_sell_pe,
                                   buy_div_y, sell_div_y,
                                   buy_date=buy_dt, buy_price=buy_pr)
        return jsonify(res)
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "msg": f"{type(e).__name__}: {e}",
                        "trace": traceback.format_exc(limit=4)}), 500


@app.route("/api/stock_div_yield_hist")
def api_stock_div_yield_hist():
    """
    个股历史股息率接口：输入个股代码 → 按 V6 双重约束算法输出日度近一年动态股息率序列。
    数据来源：MySQL（stock_daily + stock_dividend）做增量更新缓存；缺失部分按 akshare+东财 实时抓取后自动入库。

    参数:
      ?symbol=000895     (必选, 个股代码 6 位数字; 支持带交易所后缀 000895.SZ/000895.SH 自动识别)
      &start_date=2020-01-01   (可选, 过滤展示起点；默认返回所有交易日)
      &end_date=2026-07-11     (可选, 过滤展示终点；默认返回所有交易日)

    返回: {
      ok, msg?,
      code, sec_name,
      dates: ["2020-01-02", ...],           # 所有交易日 (YYYY-MM-DD)
      close: [26.87, 27.12, ...],           # 不复权收盘价 (元/股)
      rolling_div_1y: [1.45, 1.45, ...],    # 近一年累计每股派现（分子）
      div_yield_pct: [5.40, 5.35, ...],     # 动态股息率% = rolling_div_1y / close * 100
      dividend_events: [{ex_date, per_share_dividend, per_share_gift, per_share_transfer, report_year}, ...],
      latest: {date, close, rolling_div_1y, div_yield_pct, max_yield, min_yield, avg_yield,
               max_close, min_close}  # 整段统计值（方便页头卡片展示）
    }
    """
    try:
        sym = request.args.get("symbol", "").strip()
        sd_str = request.args.get("start_date", "").strip()
        ed_str = request.args.get("end_date", "").strip()
        if not sym:
            return jsonify({"ok": False, "msg": "缺少必填参数：symbol (6位个股代码)"}), 400
        # 1. 拉取/增量更新 日K线 & 分红（自动入库 MySQL）
        close_s, _ohlcv = get_and_update_stock_daily(sym)
        if close_s is None or len(close_s) == 0:
            return jsonify({"ok": False, "msg": f"无法获取 {sym} 的历史日线数据（代码不存在/已退市/接口异常）"}), 422
        try:
            sec_name, _ = _ri_fetch_sec_name(sym)
        except Exception:
            sec_name = sym
        div_df = get_and_update_stock_dividend(sym)
        # 2. 应用 V6 算股息率（与低估回测同一算法，保证与个股低估回测页面口径完全一致）
        rolling_div_1y, div_yield_pct = _uv_calc_daily_div_yield(div_df, close_s)
        # 3. 可选的日期截断（仅在返回时做，不影响入库）
        if sd_str:
            try:
                sd_ts = pd.Timestamp(sd_str)
                mask = close_s.index >= sd_ts
                close_s = close_s.loc[mask]
                rolling_div_1y = rolling_div_1y.loc[mask]
                div_yield_pct = div_yield_pct.loc[mask]
            except Exception:
                pass
        if ed_str:
            try:
                ed_ts = pd.Timestamp(ed_str)
                mask = close_s.index <= ed_ts
                close_s = close_s.loc[mask]
                rolling_div_1y = rolling_div_1y.loc[mask]
                div_yield_pct = div_yield_pct.loc[mask]
            except Exception:
                pass
        if len(close_s) == 0:
            return jsonify({"ok": False, "msg": "所选日期区间内没有可用的交易数据"}), 422
        # 4. 组装分红事件摘要（按 ex_date 升序，给前端在图表上画 marker 用）
        events = []
        if len(div_df) > 0:
            # 仅在图表区间内的事件才返回
            first_d = close_s.index.min().date()
            last_d = close_s.index.max().date()
            for _, r in div_df.iterrows():
                try:
                    exd = r["ex_date"] if hasattr(r["ex_date"], "year") else pd.Timestamp(r["ex_date"]).date()
                except Exception:
                    continue
                if not (first_d <= exd <= last_d):
                    continue
                events.append({
                    "ex_date": exd.isoformat(),
                    "per_share_dividend": float(r.get("per_share_dividend") or 0.0),
                    "per_share_gift": float(r.get("per_share_gift") or 0.0),
                    "per_share_transfer": float(r.get("per_share_transfer") or 0.0),
                    "report_year": (r.get("report_year") or "") if "report_year" in r and pd.notna(r.get("report_year")) else "",
                })
            events.sort(key=lambda x: x["ex_date"])
        # 5. 统计值（用于页头卡片）
        dates_list = [d.strftime("%Y-%m-%d") for d in close_s.index.date]
        close_list = [round(float(v), 4) if pd.notna(v) else None for v in close_s.values]
        rd1y_list = [round(float(v), 4) if pd.notna(v) else None for v in rolling_div_1y.values]
        dyp_list = [round(float(v), 4) if pd.notna(v) else None for v in div_yield_pct.values]
        def _nan_aware_stats(arr_np):
            arr = [x for x in arr_np if x is not None and (isinstance(x, (int,float)) and not (isinstance(x,float) and pd.isna(x)))]
            if not arr:
                return None, None, None, None
            return min(arr), max(arr), sum(arr)/len(arr), arr[-1]
        d_min, d_max, d_avg, d_last = _nan_aware_stats(dyp_list)
        c_min, c_max, c_avg, c_last = _nan_aware_stats(close_list)
        rd1y_last = rd1y_list[-1] if rd1y_list else None
        latest_obj = {
            "date": dates_list[-1] if dates_list else None,
            "close": c_last,
            "rolling_div_1y": rd1y_last,
            "div_yield_pct": d_last,
            "max_yield": d_max,
            "min_yield": d_min,
            "avg_yield": d_avg,
            "max_close": c_max,
            "min_close": c_min,
        }
        code6 = _ri_normalize_symbol(sym).split(".")[0]
        return jsonify({
            "ok": True,
            "code": code6,
            "sec_name": sec_name or code6,
            "dates": dates_list,
            "close": close_list,
            "rolling_div_1y": rd1y_list,
            "div_yield_pct": dyp_list,
            "dividend_events": events,
            "latest": latest_obj,
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "msg": f"{type(e).__name__}: {e}",
                        "trace": traceback.format_exc(limit=6)}), 500


# ============================================================
# 4-bis. 告警引擎：每小时检查用户告警条件并发邮件
# ============================================================
def _is_a_share_trading_time(now=None) -> bool:
    """判断当前是否 A 股开盘时间：周一~周五 9:30~15:00（CST）"""
    now = now or datetime.now()
    if now.weekday() >= 5:  # 周六周日
        return False
    h, m = now.hour, now.minute
    minutes = h * 60 + m
    return 9 * 60 + 30 <= minutes <= 15 * 60


def _get_stock_pe_ttm(stock_code: str) -> float | None:
    """获取个股最新 PE(TTM)，通过 akshare stock_a_pe_and_pb"""
    try:
        import akshare as ak
        # 代码带交易所前缀：6开头=SH，其余=SZ
        prefix = "SH" if stock_code.startswith("6") else "SZ"
        sym = f"{prefix}{stock_code}"
        df = ak.stock_a_pe_and_pb(symbol=sym)
        if df is not None and len(df) > 0:
            latest = df.iloc[-1]
            # 列名可能是 pe_ttm 或 date,pe
            for col in ["pe_ttm", "pe", "PE(TTM)"]:
                if col in df.columns:
                    val = _safe_float(latest.get(col))
                    if val and val > 0:
                        return val
    except Exception as e:
        print(f"[告警] 获取 {stock_code} PE(TTM) 失败：{e}")
    return None


def _check_user_alerts(force=False):
    """遍历所有用户的告警配置，检查条件并发邮件"""
    if not EMAIL_ENABLED:
        return

    # 获取最新深证指数 PE
    sz_pe_row = index_latest_row("sz_market")
    sz_pe_val = _safe_float(sz_pe_row.get("pe_ttm")) if sz_pe_row else None

    # 获取最新金银价格
    pm_latest = latest_row() or {}
    gold_price = _safe_float(pm_latest.get("gold_price"))
    silver_price = _safe_float(pm_latest.get("silver_price"))
    ratio_val = round(gold_price / silver_price, 2) if gold_price and silver_price and silver_price > 0 else None

    today_str = datetime.now().strftime('%Y-%m-%d')

    with SessionLocal() as s:
        all_settings = s.query(AlertSetting).all()
        for alert in all_settings:
            try:
                user = s.get(User, alert.user_id)
                if not user:
                    continue
                stock_code = (alert.stock_code or "").strip()
                stock_name = alert.stock_name or stock_code

                # 获取个股股息率
                div_yield_val = None
                try:
                    close_s, _ = get_and_update_stock_daily(stock_code)
                    if close_s is not None and len(close_s) > 0:
                        div_df = get_and_update_stock_dividend(stock_code)
                        _, yld_s = _uv_calc_daily_div_yield(div_df, close_s)
                        if len(yld_s) > 0:
                            div_yield_val = float(yld_s.iloc[-1])
                except Exception as e:
                    print(f"[告警] 获取股息率失败 {stock_code}: {e}")

                # 获取个股 PE(TTM)
                pe_ttm_val = _get_stock_pe_ttm(stock_code) if alert.pettm_enabled else None

                # —— 条件判断 ——
                alerts_to_send = []
                has_szpe = alert.szpe_enabled and sz_pe_val is not None

                if has_szpe:
                    # 有深证PE告警：买入需深证PE<买入阈值 且（股息率或PE(TTM)满足买入）
                    szpe_buy_ok = alert.szpe_buy_threshold and sz_pe_val < alert.szpe_buy_threshold
                    szpe_sell_ok = alert.szpe_sell_threshold and sz_pe_val > alert.szpe_sell_threshold

                    if szpe_buy_ok:
                        sub_reasons = []
                        if alert.div_enabled and div_yield_val is not None and alert.div_buy_threshold:
                            if div_yield_val > alert.div_buy_threshold:
                                sub_reasons.append(f"股息率 {div_yield_val:.2f}% > 买入阈值 {alert.div_buy_threshold}%")
                        if alert.pettm_enabled and pe_ttm_val is not None and alert.pettm_buy_threshold:
                            if pe_ttm_val < alert.pettm_buy_threshold:
                                sub_reasons.append(f"PE(TTM) {pe_ttm_val:.1f} < 买入阈值 {alert.pettm_buy_threshold}")
                        if sub_reasons:
                            alerts_to_send.append({
                                'type': 'buy',
                                'subject': f"{stock_name}股息率买入信号 告警",
                                'body': f"您关注的股票【{stock_name}】今天{today_str}股息率{div_yield_val:.2f}%大于您配置的买入阈值{alert.div_buy_threshold}%了，请及时留意！"
                            })

                    # 卖出：深证PE>卖出阈值 或 股息率<卖出阈值 或 PE(TTM)>卖出阈值
                    if szpe_sell_ok:
                        alerts_to_send.append({
                            'type': 'sell',
                            'subject': f"{stock_name}深证PE卖出信号 告警",
                            'body': f"您关注的股票【{stock_name}】今天{today_str}深证PE{sz_pe_val:.1f}大于您配置的卖出阈值{alert.szpe_sell_threshold}了，请及时留意！"
                        })

                    if alert.div_enabled and div_yield_val is not None and alert.div_sell_threshold:
                        if div_yield_val < alert.div_sell_threshold:
                            alerts_to_send.append({
                                'type': 'sell',
                                'subject': f"{stock_name}股息率卖出信号 告警",
                                'body': f"您关注的股票【{stock_name}】今天{today_str}股息率{div_yield_val:.2f}%小于您配置的卖出阈值{alert.div_sell_threshold}%了，请及时留意！"
                            })

                    if alert.pettm_enabled and pe_ttm_val is not None and alert.pettm_sell_threshold:
                        if pe_ttm_val > alert.pettm_sell_threshold:
                            alerts_to_send.append({
                                'type': 'sell',
                                'subject': f"{stock_name}PE(TTM)卖出信号 告警",
                                'body': f"您关注的股票【{stock_name}】今天{today_str}PE(TTM){pe_ttm_val:.1f}大于您配置的卖出阈值{alert.pettm_sell_threshold}了，请及时留意！"
                            })
                else:
                    # 无深证PE告警：仅按股息率或PE(TTM)条件（满足任一即可）
                    if alert.div_enabled and div_yield_val is not None:
                        if alert.div_buy_threshold and div_yield_val > alert.div_buy_threshold:
                            alerts_to_send.append({
                                'type': 'buy',
                                'subject': f"{stock_name}股息率买入信号 告警",
                                'body': f"您关注的股票【{stock_name}】今天{today_str}股息率{div_yield_val:.2f}%大于您配置的买入阈值{alert.div_buy_threshold}%了，请及时留意！"
                            })
                        if alert.div_sell_threshold and div_yield_val < alert.div_sell_threshold:
                            alerts_to_send.append({
                                'type': 'sell',
                                'subject': f"{stock_name}股息率卖出信号 告警",
                                'body': f"您关注的股票【{stock_name}】今天{today_str}股息率{div_yield_val:.2f}%小于您配置的卖出阈值{alert.div_sell_threshold}%了，请及时留意！"
                            })

                    if alert.pettm_enabled and pe_ttm_val is not None:
                        if alert.pettm_buy_threshold and pe_ttm_val < alert.pettm_buy_threshold:
                            alerts_to_send.append({
                                'type': 'buy',
                                'subject': f"{stock_name}PE(TTM)买入信号 告警",
                                'body': f"您关注的股票【{stock_name}】今天{today_str}PE(TTM){pe_ttm_val:.1f}小于您配置的买入阈值{alert.pettm_buy_threshold}了，请及时留意！"
                            })
                        if alert.pettm_sell_threshold and pe_ttm_val > alert.pettm_sell_threshold:
                            alerts_to_send.append({
                                'type': 'sell',
                                'subject': f"{stock_name}PE(TTM)卖出信号 告警",
                                'body': f"您关注的股票【{stock_name}】今天{today_str}PE(TTM){pe_ttm_val:.1f}大于您配置的卖出阈值{alert.pettm_sell_threshold}了，请及时留意！"
                            })

                # 金银比告警（独立判断，不依赖股票代码）
                if alert.gold_silver_enabled and ratio_val is not None:
                    if alert.gold_silver_buy_threshold and ratio_val > alert.gold_silver_buy_threshold:
                        alerts_to_send.append({
                            'type': 'buy',
                            'subject': "白银低估买入告警",
                            'body': f"当前金银比是{ratio_val}，大于您配置的买入比值{alert.gold_silver_buy_threshold}，白银低估，黄金高估，可以卖出黄金买入白银！当前黄金的价格是{gold_price}美元/盎司，白银的价格是{silver_price}美元/盎司！"
                        })
                    if alert.gold_silver_sell_threshold and ratio_val < alert.gold_silver_sell_threshold:
                        alerts_to_send.append({
                            'type': 'sell',
                            'subject': "白银高估卖出告警",
                            'body': f"当前金银比是{ratio_val}，小于您配置的卖出比值{alert.gold_silver_sell_threshold}，白银高估，可以卖出白银买入黄金！当前黄金的价格是{gold_price}美元/盎司，白银的价格是{silver_price}美元/盎司！"
                        })

                # 发送邮件
                for alert_msg in alerts_to_send:
                    subject = alert_msg['subject']
                    body = alert_msg['body']
                    print(f"[告警] 准备发送: {subject}")
                    ok, msg = send_email(subject, body, to_addrs=user.email)
                    print(f"[告警] {user.username}({user.email}) {stock_code}: {'成功' if ok else '失败:'+msg}")

            except Exception as e:
                print(f"[告警] 用户 {alert.user_id} 检查异常：{e}")


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

    # Job 3：用户告警检查 — 开盘时间每小时检查一次（工作日 9:30~15:00）
    try:
        sched.add_job(
            _check_user_alerts,
            "cron",
            day_of_week="mon-fri",
            hour="9-15",
            minute=30,
            timezone="Asia/Shanghai",
            id="check_user_alerts_hourly",
            replace_existing=True,
        )
        job3_info = " · 用户告警：工作日每小时检查"
    except Exception as e_job3:
        job3_info = f" · 用户告警 job 添加失败: {type(e_job3).__name__}: {e_job3}"

    sched.start()
    print(f"[调度] 已启动 APScheduler："
          f"金银价每 {COLLECT_INTERVAL_HOURS}h 一次{job2_info}{job3_info}")
    return sched


def _parse_financial_value(val):
    """解析财务数据字符串，转换为数字"""
    if val is None or val == '' or pd.isna(val):
        return None
    val = str(val).strip()
    if val == 'False' or val == 'true' or val == 'True':
        return None
    try:
        if val.endswith('亿'):
            return float(val.replace('亿', '')) * 1e8
        elif val.endswith('万'):
            return float(val.replace('万', '')) * 1e4
        else:
            return float(val)
    except:
        return None


def _calc_growth_rate(data, periods):
    """计算同比增长率"""
    rates = {}
    for i in range(len(periods)):
        curr_period = periods[i]
        if i < len(periods) - 1:
            prev_period = periods[i + 1]
            curr_val = data.get(curr_period)
            prev_val = data.get(prev_period)
            if curr_val is not None and prev_val is not None and prev_val != 0:
                rates[curr_period] = (curr_val - prev_val) / prev_val * 100
            else:
                rates[curr_period] = None
        else:
            rates[curr_period] = None
    return rates


def _fetch_financial_data(code_list, years=5):
    """
    获取多只股票的完整财务报表数据（使用同花顺接口）
    years: 需要获取的年度报告期数量（5年、8年、10年、15年），实际查询years+1年以计算增长率
    """
    import akshare as ak
    
    query_years = years + 1
    
    result = {
        "stocks": [],
        "error": None
    }
    
    for code in code_list:
        try:
            sec_name, suffix = _ri_fetch_sec_name(code)
            if not sec_name:
                sec_name = code
            
            balance_data = {}
            income_data = {}
            cashflow_data = {}
            growth_data = {}
            annual_periods = []
            
            try:
                df_debt = ak.stock_financial_debt_ths(symbol=code)
                if df_debt is not None and len(df_debt) > 0:
                    debt_periods = []
                    for _, row in df_debt.iterrows():
                        period = str(row['报告期'])
                        if period.endswith('-12-31'):
                            debt_periods.append(period)
                    
                    annual_periods = sorted(debt_periods, reverse=True)[:query_years]
                    
                    field_mapping = {
                        '资产合计': '资产合计',
                        '货币资金': '货币资金',
                        '交易性金融资产': '交易性金融资产',
                        '应收账款': '应收账款',
                        '应收票据': '其中：应收票据',
                        '应收款项融资': None,
                        '合同资产': None,
                        '预付款项': '预付款项',
                        '其他应收款': '其他应收款',
                        '存货': '存货',
                        '固定资产合计': '固定资产合计',
                        '在建工程合计': '在建工程合计',
                        '工程物资': '工程物资',
                        '无形资产': '无形资产',
                        '商誉': '商誉',
                        '其他非流动金融资产': '其他非流动金融资产',
                        '长期股权投资': '长期股权投资',
                        '投资性房地产': None,
                        '短期借款': '短期借款',
                        '应付账款': '应付账款',
                        '应付票据': '其中：应付票据',
                        '预收款项': '预收款项',
                        '合同负债': '合同负债',
                        '一年内到期的非流动负债': '一年内到期的非流动负债',
                        '长期借款': '长期借款',
                        '应付债券': None,
                        '长期应付款': '其中：长期应付款',
                        '负债合计': '负债合计',
                        '所有者权益（或股东权益）合计': '所有者权益（或股东权益）合计',
                        '归属于母公司所有者权益合计': '归属于母公司所有者权益合计',
                        '流动资产合计': '流动资产合计',
                        '非流动资产合计': '非流动资产合计',
                        '流动负债合计': '流动负债合计',
                        '非流动负债合计': '非流动负债合计'
                    }
                    
                    for item, actual_field in field_mapping.items():
                        values = {}
                        for period in annual_periods:
                            row = df_debt[df_debt['报告期'] == period]
                            if len(row) > 0 and actual_field is not None:
                                val = row.iloc[0].get(actual_field)
                                values[period] = _parse_financial_value(val)
                            else:
                                values[period] = None
                        balance_data[item] = values
            except Exception as e:
                print(f'  [finance] stock_financial_debt_ths({code}) 失败: {e}')
            
            try:
                df_benefit = ak.stock_financial_benefit_ths(symbol=code)
                if df_benefit is not None and len(df_benefit) > 0:
                    if not annual_periods:
                        benefit_periods = []
                        for _, row in df_benefit.iterrows():
                            period = str(row['报告期'])
                            if period.endswith('-12-31'):
                                benefit_periods.append(period)
                        annual_periods = sorted(benefit_periods, reverse=True)[:query_years]
                    
                    income_field_mapping = {
                        '营业总收入': '一、营业总收入',
                        '营业收入': '其中：营业收入',
                        '营业成本': '其中：营业成本',
                        '营业税金及附加': '营业税金及附加',
                        '销售费用': '销售费用',
                        '管理费用': '管理费用',
                        '研发费用': '研发费用',
                        '财务费用': '财务费用',
                        '投资收益': '投资收益',
                        '营业利润': '三、营业利润',
                        '利润总额': '四、利润总额',
                        '净利润': '五、净利润',
                        '归属于母公司所有者的净利润': '归属于母公司所有者的净利润',
                        '扣除非经常性损益后的净利润': '扣除非经常性损益后的净利润',
                        '基本每股收益': '（一）基本每股收益',
                        '稀释每股收益': '（二）稀释每股收益',
                        '资产减值损失': '资产减值损失',
                        '信用减值损失': '信用减值损失',
                        '公允价值变动收益': '加：公允价值变动收益',
                        '资产处置收益': '资产处置收益',
                        '其他收益': '其他收益',
                        '营业外收入': '营业外收入',
                        '营业外支出': '营业外支出',
                        '所得税费用': '所得税费用'
                    }
                    
                    for item, actual_field in income_field_mapping.items():
                        values = {}
                        for period in annual_periods:
                            row = df_benefit[df_benefit['报告期'] == period]
                            if len(row) > 0:
                                val = row.iloc[0].get(actual_field)
                                values[period] = _parse_financial_value(val)
                            else:
                                values[period] = None
                        income_data[item] = values
            except Exception as e:
                print(f'  [finance] stock_financial_benefit_ths({code}) 失败: {e}')
            
            try:
                df_cash = ak.stock_financial_cash_ths(symbol=code)
                if df_cash is not None and len(df_cash) > 0:
                    if not annual_periods:
                        cash_periods = []
                        for _, row in df_cash.iterrows():
                            period = str(row['报告期'])
                            if period.endswith('-12-31'):
                                cash_periods.append(period)
                        annual_periods = sorted(cash_periods, reverse=True)[:query_years]
                    
                    cashflow_field_mapping = {
                        '经营活动产生的现金流量净额': '经营活动产生的现金流量净额',
                        '投资活动产生的现金流量净额': '投资活动产生的现金流量净额',
                        '筹资活动产生的现金流量净额': '筹资活动产生的现金流量净额',
                        '现金及现金等价物净增加额': '五、现金及现金等价物净增加额',
                        '销售商品、提供劳务收到的现金': '销售商品、提供劳务收到的现金',
                        '购买商品、接受劳务支付的现金': '购买商品、接受劳务支付的现金',
                        '支付给职工以及为职工支付的现金': '支付给职工以及为职工支付的现金',
                        '支付的各项税费': '支付的各项税费',
                        '收回投资收到的现金': '收回投资收到的现金',
                        '取得投资收益收到的现金': '取得投资收益收到的现金',
                        '购建固定资产、无形资产和其他长期资产支付的现金': '购建固定资产、无形资产和其他长期资产支付的现金',
                        '投资支付的现金': '投资支付的现金',
                        '吸收投资收到的现金': '吸收投资收到的现金',
                        '取得借款收到的现金': '取得借款收到的现金',
                        '偿还债务支付的现金': '偿还债务支付的现金',
                        '分配股利、利润或偿付利息支付的现金': '分配股利、利润或偿付利息支付的现金',
                        '期末现金及现金等价物余额': '六、期末现金及现金等价物余额'
                    }
                    
                    for item, actual_field in cashflow_field_mapping.items():
                        values = {}
                        for period in annual_periods:
                            row = df_cash[df_cash['报告期'] == period]
                            if len(row) > 0:
                                val = row.iloc[0].get(actual_field)
                                values[period] = _parse_financial_value(val)
                            else:
                                values[period] = None
                        cashflow_data[item] = values
            except Exception as e:
                print(f'  [finance] stock_financial_cash_ths({code}) 失败: {e}')
            
            if balance_data.get('资产合计'):
                growth_data['资产合计增长率'] = _calc_growth_rate(balance_data['资产合计'], annual_periods)
            if income_data.get('营业总收入'):
                growth_data['营业总收入增长率'] = _calc_growth_rate(income_data['营业总收入'], annual_periods)
            if income_data.get('净利润'):
                growth_data['净利润增长率'] = _calc_growth_rate(income_data['净利润'], annual_periods)
            if income_data.get('归属于母公司所有者的净利润'):
                growth_data['归母净利润增长率'] = _calc_growth_rate(income_data['归属于母公司所有者的净利润'], annual_periods)
            
            dividend_data = {}
            for period in annual_periods:
                dividend = cashflow_data.get('分配股利、利润或偿付利息支付的现金', {}).get(period)
                cashflow = cashflow_data.get('经营活动产生的现金流量净额', {}).get(period)
                if cashflow is not None and cashflow != 0:
                    dividend_data[period] = (dividend / cashflow * 100) if dividend else 0
                else:
                    dividend_data[period] = None
            growth_data['分红率'] = dividend_data
            
            display_periods = annual_periods[:years]
            
            result["stocks"].append({
                "code": code,
                "name": sec_name,
                "periods": display_periods,
                "balance_sheet": balance_data,
                "income_sheet": income_data,
                "cashflow_sheet": cashflow_data,
                "growth_data": growth_data
            })
            
        except Exception as e:
            result["stocks"].append({
                "code": code,
                "name": code,
                "error": str(e)
            })
    
    return result


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
