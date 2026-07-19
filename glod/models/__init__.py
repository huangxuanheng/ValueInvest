from sqlalchemy import Column, Integer, String, Text, Date, Float, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), nullable=False, unique=True, comment="用户名（唯一）")
    email = Column(String(128), nullable=False, unique=True, comment="邮箱（唯一，仅支持163/QQ）")
    password_enc = Column(Text, nullable=False, comment="RSA公钥加密后的密码密文（base64）")
    created_at = Column(Date, nullable=True, comment="注册日期")

    __table_args__ = ({"comment": "注册用户表（密码非对称加密存储）"},)


class AlertSetting(Base):
    __tablename__ = "alert_settings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, comment="关联 users.id")
    stock_code = Column(String(16), nullable=False, comment="股票代码 6 位")
    stock_name = Column(String(64), nullable=True, comment="股票名称（自动查询）")
    szpe_enabled = Column(Boolean, default=False, comment="是否启用深证PE告警")
    szpe_buy_threshold = Column(Float, nullable=True, comment="深证PE买入阈值")
    szpe_sell_threshold = Column(Float, nullable=True, comment="深证PE卖出阈值")
    div_enabled = Column(Boolean, default=False, comment="是否启用股息率告警")
    div_buy_threshold = Column(Float, nullable=True, comment="股息率买入阈值(%)")
    div_sell_threshold = Column(Float, nullable=True, comment="股息率卖出阈值(%)")
    pettm_enabled = Column(Boolean, default=False, comment="是否启用PE(TTM)告警")
    pettm_buy_threshold = Column(Float, nullable=True, comment="PE(TTM)买入阈值")
    pettm_sell_threshold = Column(Float, nullable=True, comment="PE(TTM)卖出阈值")
    gold_silver_enabled = Column(Boolean, default=False, comment="是否启用金银比告警")
    gold_silver_buy_threshold = Column(Float, nullable=True, comment="金银比买入阈值")
    gold_silver_sell_threshold = Column(Float, nullable=True, comment="金银比卖出阈值")
    hs300pe_enabled = Column(Boolean, default=False, comment="是否启用沪深300PE告警")
    hs300pe_buy_threshold = Column(Float, nullable=True, comment="沪深300PE买入阈值")
    hs300pe_sell_threshold = Column(Float, nullable=True, comment="沪深300PE卖出阈值")
    updated_at = Column(Date, nullable=True, comment="更新日期")

    __table_args__ = ({"comment": "用户告警配置表"},)


class DividendIndex(Base):
    __tablename__ = "dividend_indices"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, comment="日期")
    sz50 = Column(Float, nullable=True, comment="上证50股息率(%)")
    hs300 = Column(Float, nullable=True, comment="沪深300股息率(%)")
    zz500 = Column(Float, nullable=True, comment="中证500股息率(%)")
    szpe = Column(Float, nullable=True, comment="深证PE(TTM)")
    hs300pe = Column(Float, nullable=True, comment="沪深300PE(TTM)")
    cybpe = Column(Float, nullable=True, comment="创业板PE(TTM)")
    updated_at = Column(Date, nullable=True, comment="更新日期")

    __table_args__ = ({"comment": "股息率与估值指数表"},)


try:
    from urllib.parse import quote_plus as _url_quote
except Exception:
    from urllib import quote_plus as _url_quote

_MYSQL_USER = "admin"
_MYSQL_PASS_RAW = "st!:a&h=C2eu "
_MYSQL_PASS = _MYSQL_PASS_RAW.rstrip()
_MYSQL_HOST = "175.178.250.9"
_MYSQL_PORT = 3306
_MYSQL_DB = "finance_analysis"

DB_URL = (
    f"mysql+pymysql://{_MYSQL_USER}:{_url_quote(_MYSQL_PASS)}@{_MYSQL_HOST}:{_MYSQL_PORT}"
    f"/{_MYSQL_DB}?charset=utf8mb4"
)

engine = create_engine(DB_URL, pool_recycle=3600)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)