import pandas as pd
import requests
import os
import re
import socket
import time
import threading
import concurrent.futures
from functools import wraps
from ..utils.stock_info import fetch_sec_name
from .finance_service import fetch_financial_data

# 设置全局超时
socket.setdefaulttimeout(30)


def _run_with_timeout(func, timeout_seconds, *args, **kwargs):
    """
    使用线程强制超时运行函数。
    如果函数执行时间超过 timeout_seconds，会返回 None。
    注意：被超时终止的线程仍会在后台运行，但不会阻塞主流程。
    """
    result_holder = {'result': None, 'done': False, 'error': None}
    
    def _worker():
        try:
            result_holder['result'] = func(*args, **kwargs)
        except Exception as e:
            result_holder['error'] = e
        finally:
            result_holder['done'] = True
    
    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if not result_holder['done']:
        print(f'    [超时] 函数执行超过{timeout_seconds}秒，已放弃')
        return None
    
    if result_holder['error']:
        print(f'    [异常] 函数执行异常: {result_holder["error"]}')
        return None
    
    return result_holder['result']



class BuffettValuationService:
    """
    巴菲特估值服务
    计算留存比值指标：留存比值 = 市值增长 / 总留存收益
    """

    def __init__(self):
        pass

    def calculate(self, code: str, years: int = 5) -> dict:
        """
        计算巴菲特估值指标
        
        参数:
            code: 股票代码（6位数字）
            years: 连续年限
            
        返回:
            包含估值结果的字典
        """
        import akshare as ak
        
        print(f'[buffett] 开始计算巴菲特估值: code={code}, years={years}')
        
        sec_name, suffix = fetch_sec_name(code)
        if not sec_name:
            sec_name = code
        print(f'[buffett] 股票名称: {sec_name}')
        
        financial_data = fetch_financial_data([code], years)
        if not financial_data.get('stocks') or financial_data['stocks'][0].get('error'):
            raise RuntimeError('无法获取财务数据')
        
        stock_data = financial_data['stocks'][0]
        income_data = stock_data.get('income_statement', {})
        cashflow_data = stock_data.get('cashflow_statement', {})
        balance_data = stock_data.get('balance_sheet', {})
        annual_periods = stock_data.get('periods', [])
        
        net_profit_data = income_data.get('归属于母公司所有者的净利润', {})
        print(f'[buffett] 净利润数据: {list(net_profit_data.keys())}')
        print(f'[buffett] 年度报告期: {annual_periods}')
        
        # 从报告期中获取最早的年份作为价格数据起始年
        price_start_year = 2015
        if annual_periods:
            earliest = min(pd.Timestamp(p).year for p in annual_periods)
            price_start_year = max(earliest - 1, 2005)  # 提前一年以覆盖全年
        
        div_df = _run_with_timeout(lambda: self._fetch_dividend_data(code), 45)
        if div_df is None:
            div_df = pd.DataFrame()
            print(f'[buffett] 分红数据获取超时，使用空数据')
        
        total_shares_data = _run_with_timeout(lambda: self._fetch_total_shares_data(code, annual_periods), 60)
        if total_shares_data is None:
            total_shares_data = {}
            print(f'[buffett] 总股本数据获取超时，使用空数据')
        
        fq_close_dict = _run_with_timeout(lambda: self._fetch_fq_price_data(code, price_start_year), 90)
        if fq_close_dict is None:
            fq_close_dict = {}
            print(f'[buffett] 后复权价格获取超时，使用空数据')
        
        non_fq_close_dict = _run_with_timeout(lambda: self._fetch_non_fq_price_data(code, price_start_year), 60)
        if non_fq_close_dict is None:
            non_fq_close_dict = {}
            print(f'[buffett] 不复权价格获取超时，使用空数据')
        
        yearly_data, total_net_profit, total_dividend, total_retained_earnings, _ = \
            self._calculate_yearly_data(annual_periods, net_profit_data, div_df, total_shares_data)
        
        yearly_data.sort(key=lambda x: x['year'])
        
        start_year = yearly_data[0]['year'] if yearly_data else None
        end_year = yearly_data[-1]['year'] if yearly_data else None
        
        # 使用原始的 total_shares_data 计算市值，而不是 adjusted_shares_data
        yearly_market_cap, yearly_non_fq_price, yearly_price_date = self._calculate_yearly_market_cap(annual_periods, non_fq_close_dict, total_shares_data)

        # 折旧与摊销、维持性资本支出：不再远程查询，由用户手动填入
        # 远程计算数据不准确，改为返回空字典，用户填入的值通过API覆盖逻辑保存到数据库
        depreciation_amortization_data = {}
        print(f'[buffett] 折旧与摊销：跳过远程查询，由用户手动填入')

        capex_data = cashflow_data.get('购建固定资产、无形资产和其他长期资产支付的现金', {})

        # 扩张性资本支出：已停用远程查询
        expansion_capex_data = {}
        print(f'[buffett] 扩张性资本支出：跳过远程查询')

        # 维持性资本支出：跳过远程计算，由用户手动填入
        maintenance_capex_data = {}
        print(f'[buffett] 维持性资本支出：跳过远程查询，由用户手动填入')

        shareholder_earnings_data = self._calculate_shareholder_earnings(annual_periods, net_profit_data, depreciation_amortization_data, maintenance_capex_data)

        # 计算起始市值和当前市值（用不复权价 × 总股本）
        # 注意：必须直接使用 yearly_non_fq_price 和 total_shares_data 字典，
        # 因为此时 yearly_data 的 item 尚未被赋值 non_fq_price 字段（赋值在下方循环中执行）
        start_market_cap = None
        current_market_cap = None
        start_fq_price = None
        current_fq_price = None
        start_fq_date = None
        current_fq_date = None

        if yearly_data and len(yearly_data) > 0:
            first_period = yearly_data[0]['period']
            last_period = yearly_data[-1]['period']

            # 获取总股本：优先使用对应期间数据，若无则回退到第一个可用值
            def _get_shares_for_period(period):
                shares = total_shares_data.get(period)
                if not shares or shares <= 0:
                    for v in total_shares_data.values():
                        if v and v > 0:
                            return v
                return shares

            # 从多个来源获取不复权价：优先 yearly_non_fq_price → 反推 → non_fq_close_dict 直接查找
            def _get_non_fq_price(period):
                # 来源1：yearly_non_fq_price 字典（已通过 _calculate_yearly_market_cap 计算）
                price = yearly_non_fq_price.get(period)
                if price is not None and price > 0:
                    return price

                # 来源2：从 yearly_market_cap 和 total_shares 反推
                market_cap = yearly_market_cap.get(period)
                shares = _get_shares_for_period(period)
                if market_cap is not None and shares and shares > 0:
                    derived = market_cap * 100000000 / shares
                    print(f'[buffett] 从市值反推不复权价: period={period}, market_cap={market_cap}亿, shares={shares}, price={derived}')
                    return derived

                # 来源3：从 non_fq_close_dict 直接查找最近交易日的价格
                if non_fq_close_dict:
                    year = period[:4]
                    try:
                        next_year = int(year) + 1
                        target_date = pd.Timestamp(f"{next_year}-05-01")
                        sorted_dates = sorted(non_fq_close_dict.keys())
                        for d in reversed(sorted_dates):
                            if d <= target_date:
                                price = non_fq_close_dict[d]
                                if price and price > 0:
                                    print(f'[buffett] 从non_fq_close_dict查找不复权价: period={period}, date={d}, price={price}')
                                    return price
                    except Exception:
                        pass

                print(f'[buffett] 无法获取不复权价: period={period}')
                return None

            # 起始市值 = 起始不复权价 × 起始总股本（亿元）
            # 起始市值采用"次年5月1日前最近交易日"的价格，与年报口径对齐
            start_price = _get_non_fq_price(first_period)
            start_shares = _get_shares_for_period(first_period)
            if start_price is not None and start_shares and start_shares > 0:
                start_market_cap = start_price * start_shares / 100000000
                print(f'[buffett] 起始市值计算: price={start_price}, shares={start_shares}, market_cap={start_market_cap}亿')

            # 当前市值 = 查询当天的最近交易日不复权价 × 当前总股本（亿元）
            # 从 non_fq_close_dict 取最大日期（最近交易日）的价格，周末/节假日自动取上一交易日
            current_price = None
            current_price_date = None
            if non_fq_close_dict:
                sorted_dates = sorted(non_fq_close_dict.keys())
                for d in reversed(sorted_dates):
                    price_val = non_fq_close_dict[d]
                    if price_val and price_val > 0:
                        current_price = price_val
                        current_price_date = d
                        break

            current_shares = _get_shares_for_period(last_period)
            if current_price is not None and current_shares and current_shares > 0:
                current_market_cap = current_price * current_shares / 100000000
                print(f'[buffett] 当前市值计算(最近交易日口径): date={current_price_date}, price={current_price}, shares={current_shares}, market_cap={current_market_cap}亿')
            else:
                current_market_cap = None
                print(f'[buffett] 当前市值无法计算: current_price={current_price}, current_shares={current_shares}')

        # 留存比值 = 市值增长 / 总留存收益 = (当前市值 - 起始市值) / 总留存收益
        retained_growth_rate = self._calculate_retained_growth_rate_v2(
            start_market_cap, current_market_cap, total_dividend, total_retained_earnings)

        for i, item in enumerate(yearly_data):
            period = item['period']
            item['market_cap'] = yearly_market_cap.get(period)
            item['non_fq_price'] = yearly_non_fq_price.get(period)
            item['price_date'] = yearly_price_date.get(period)
            item['total_shares'] = total_shares_data.get(period)
            item['depreciation_amortization'] = depreciation_amortization_data.get(period)
            item['capex'] = capex_data.get(period) / 100000000 if capex_data.get(period) else None
            item['expansion_capex'] = expansion_capex_data.get(period)
            item['maintenance_capex'] = maintenance_capex_data.get(period)
            item['shareholder_earnings'] = shareholder_earnings_data.get(period)

        # 市值增长 = 当前市值 - 起始市值
        market_cap_growth = None
        if start_market_cap is not None and current_market_cap is not None:
            market_cap_growth = current_market_cap - start_market_cap

        print(f'[buffett] 起始市值(不复权): {start_market_cap}, 当前市值(不复权): {current_market_cap}')
        print(f'[buffett] 总净利润: {total_net_profit}, 总分红: {total_dividend}, 总留存收益: {total_retained_earnings}')
        if market_cap_growth is not None:
            print(f'[buffett] 市值增长: {market_cap_growth}亿')
        if retained_growth_rate is not None:
            print(f'[buffett] 留存比值: {retained_growth_rate}%')

        return {
            'stock_code': code,
            'stock_name': sec_name,
            'years': years,
            'start_year': start_year,
            'end_year': end_year,
            'yearly_data': yearly_data,
            'total_net_profit': total_net_profit,
            'total_dividend': total_dividend,
            'total_retained_earnings': total_retained_earnings,
            'start_fq_price': start_fq_price,
            'current_fq_price': current_fq_price,
            'start_fq_date': start_fq_date,
            'current_fq_date': current_fq_date,
            'start_market_cap': start_market_cap,
            'current_market_cap': current_market_cap,
            'market_cap_growth': market_cap_growth,
            'retained_growth_rate': retained_growth_rate
        }

    @staticmethod
    def _parse_dividend_desc(desc: str) -> float:
        """从分红方案说明中解析10股派息金额，如 '10派6.8元(含税)' -> 6.8"""
        import re
        if not desc:
            return 0.0
        # 匹配 "10派X" 或 "10派X元" 格式
        m = re.search(r'派([\d.]+)', str(desc))
        if m:
            return float(m.group(1))
        return 0.0

    @staticmethod
    def _extract_year_from_date(date_str: str) -> str:
        """从日期字符串中提取年份，如 '2022-12-26' -> '2022'"""
        if not date_str or date_str == 'nan':
            return ''
        s = str(date_str).strip()
        if len(s) >= 4:
            return s[:4]
        return ''

    @staticmethod
    def _normalize_record(record: dict) -> dict:
        """规范化分红记录，修复报告时间为NaN的问题"""
        rt = record.get('报告时间', '')
        if not rt or rt == 'nan' or rt == 'None':
            # 尝试从股权登记日推断年份
            year = BuffettValuationService._extract_year_from_date(record.get('股权登记日', ''))
            if year:
                # 从实施方案分红说明推断类型
                desc = record.get('实施方案分红说明', '')
                if '年报' in desc or '年度' in desc:
                    record['报告时间'] = f'{year}年报'
                elif '半年' in desc or '中期' in desc:
                    record['报告时间'] = f'{year}半年报'
                elif '三季' in desc:
                    record['报告时间'] = f'{year}三季报'
                elif '四季' in desc:
                    record['报告时间'] = f'{year}四季报'
                else:
                    record['报告时间'] = f'{year}年'
        return record

    @staticmethod
    def _is_report_time_valid(record: dict) -> bool:
        """检查报告时间是否有效（不是nan或空）"""
        rt = record.get('报告时间', '')
        if not rt or rt == 'nan' or rt == 'None':
            return False
        return len(str(rt).strip()) >= 4

    def _fetch_dividend_data(self, code: str):
        """获取分红数据 - 同时从巨潮、东方财富、同花顺三个接口获取，合并去重"""
        import akshare as ak
        
        all_records = []
        
        # 数据源1: 巨潮资讯 (stock_dividend_cninfo)
        try:
            df1 = ak.stock_dividend_cninfo(symbol=code)
            if df1 is not None and len(df1) > 0:
                for _, row in df1.iterrows():
                    reg_date_raw = row.get('股权登记日', '')
                    reg_date_str = ''
                    if pd.notna(reg_date_raw):
                        try:
                            from datetime import datetime as dt
                            if isinstance(reg_date_raw, dt):
                                reg_date_str = reg_date_raw.strftime('%Y-%m-%d')
                            elif hasattr(reg_date_raw, 'strftime'):
                                reg_date_str = reg_date_raw.strftime('%Y-%m-%d')
                            else:
                                reg_date_str = str(reg_date_raw)
                        except Exception:
                            reg_date_str = str(reg_date_raw) if pd.notna(reg_date_raw) else ''
                    
                    report_time = row.get('报告时间', '')
                    report_time_str = str(report_time) if pd.notna(report_time) else ''
                    
                    record = {
                        'source': 'cninfo',
                        '报告时间': report_time_str,
                        '分红类型': str(row.get('分红类型', '')),
                        '派息比例': float(row.get('派息比例', 0) or 0),
                        '股权登记日': reg_date_str,
                        '实施方案分红说明': str(row.get('实施方案分红说明', '')),
                    }
                    # 修复报告时间为NaN的记录
                    record = self._normalize_record(record)
                    
                    if record['派息比例'] > 0:
                        all_records.append(record)
                print(f'  [buffett] stock_dividend_cninfo: {len(df1)} 条')
        except Exception as e:
            print(f'  [buffett] stock_dividend_cninfo({code}) 失败: {e}')
        
        # 数据源2: 东方财富 (stock_fhps_detail_em)
        try:
            df2 = ak.stock_fhps_detail_em(symbol=code)
            if df2 is not None and len(df2) > 0:
                em_count = 0
                for _, row in df2.iterrows():
                    cash_div = row.get('现金分红-现金分红比例')
                    if cash_div is not None and pd.notna(cash_div) and float(cash_div) > 0:
                        period = row.get('报告期')
                        if period:
                            period_str = str(period)
                            try:
                                from datetime import datetime as dt
                                if isinstance(period, dt):
                                    period_str = period.strftime('%Y-%m-%d')
                                elif hasattr(period, 'strftime'):
                                    period_str = period.strftime('%Y-%m-%d')
                            except Exception:
                                pass
                            
                            reg_date_raw = row.get('股权登记日', '')
                            reg_date_str = ''
                            if pd.notna(reg_date_raw):
                                try:
                                    if isinstance(reg_date_raw, dt):
                                        reg_date_str = reg_date_raw.strftime('%Y-%m-%d')
                                    elif hasattr(reg_date_raw, 'strftime'):
                                        reg_date_str = reg_date_raw.strftime('%Y-%m-%d')
                                    else:
                                        reg_date_str = str(reg_date_raw)
                                except Exception:
                                    reg_date_str = str(reg_date_raw) if pd.notna(reg_date_raw) else ''
                            
                            progress = str(row.get('方案进度', ''))
                            if '实施' in progress or progress == '':
                                record = {
                                    'source': 'em',
                                    '报告时间': period_str,
                                    '分红类型': '现金分红',
                                    '派息比例': float(cash_div),
                                    '股权登记日': reg_date_str,
                                    '实施方案分红说明': str(row.get('现金分红-现金分红比例描述', '')),
                                }
                                record = self._normalize_record(record)
                                
                                # 去重：如果已存在相同派息比例+股权登记日，优先保留报告时间有效的
                                is_dup = False
                                for i, existing in enumerate(all_records):
                                    if (abs(existing['派息比例'] - record['派息比例']) < 0.001 and
                                        existing['股权登记日'] == record['股权登记日']):
                                        # 如果现有记录的报告时间无效，用新记录替换
                                        if not self._is_report_time_valid(existing) and self._is_report_time_valid(record):
                                            all_records[i] = record
                                        is_dup = True
                                        break
                                if not is_dup:
                                    all_records.append(record)
                                    em_count += 1
                print(f'  [buffett] stock_fhps_detail_em: {len(df2)} 条, 新增 {em_count} 条')
        except Exception as e:
            print(f'  [buffett] stock_fhps_detail_em({code}) 失败: {e}')
        
        # 数据源3: 同花顺 (stock_fhps_detail_ths) - 最完整，含中期/特别分红
        try:
            df3 = ak.stock_fhps_detail_ths(symbol=code)
            if df3 is not None and len(df3) > 0:
                ths_count = 0
                for _, row in df3.iterrows():
                    progress = str(row.get('方案进度', ''))
                    if '实施' not in progress:
                        continue
                    
                    desc = str(row.get('分红方案说明', ''))
                    if '不分配' in desc or '不转增' in desc:
                        continue
                    
                    per_share_div = self._parse_dividend_desc(desc)
                    if per_share_div <= 0:
                        continue
                    
                    period = str(row.get('报告期', ''))
                    reg_date = row.get('A股股权登记日', '')
                    reg_date_str = ''
                    if pd.notna(reg_date):
                        try:
                            from datetime import datetime as dt
                            if isinstance(reg_date, dt):
                                reg_date_str = reg_date.strftime('%Y-%m-%d')
                            elif hasattr(reg_date, 'strftime'):
                                reg_date_str = reg_date.strftime('%Y-%m-%d')
                            else:
                                reg_date_str = str(reg_date)
                        except Exception:
                            reg_date_str = str(reg_date) if pd.notna(reg_date) else ''
                    
                    record = {
                        'source': 'ths',
                        '报告时间': period,
                        '分红类型': '现金分红',
                        '派息比例': per_share_div,
                        '股权登记日': reg_date_str,
                        '实施方案分红说明': desc,
                    }
                    record = self._normalize_record(record)
                    
                    # 去重：优先保留报告时间有效的记录
                    is_dup = False
                    for i, existing in enumerate(all_records):
                        if (abs(existing['派息比例'] - record['派息比例']) < 0.001 and
                            existing['股权登记日'] == record['股权登记日']):
                            # 如果现有记录的报告时间无效，用新记录（同花顺）替换
                            if not self._is_report_time_valid(existing) and self._is_report_time_valid(record):
                                all_records[i] = record
                                print(f'    [ths] 替换无效记录: {record["报告时间"]}')
                            is_dup = True
                            break
                    if not is_dup:
                        all_records.append(record)
                        ths_count += 1
                print(f'  [buffett] stock_fhps_detail_ths: {len(df3)} 条, 新增 {ths_count} 条')
        except Exception as e:
            print(f'  [buffett] stock_fhps_detail_ths({code}) 失败: {e}')
        
        # 构建合并后的 DataFrame
        if all_records:
            merged_df = pd.DataFrame(all_records)
            print(f'  [buffett] 合并后共 {len(merged_df)} 条分红记录')
            return merged_df
        
        return pd.DataFrame()

    def _fetch_total_shares_data(self, code: str, annual_periods: list):
        """获取总股本数据（股数，单位：股）- 顺序调用多个接口"""
        import akshare as ak
        
        # 如果没有指定期间，使用默认期间
        if not annual_periods:
            from datetime import datetime
            current_year = datetime.now().year
            annual_periods = [f"{current_year}-12-31"]
        
        def parse_shares_value(val):
            """解析总股本值，支持多种格式"""
            if val is None or pd.isna(val):
                return None
            
            MIN_SHARES = 1e6
            MAX_SHARES = 5e10
            
            def validate_and_return(num):
                if num and MIN_SHARES <= num < MAX_SHARES:
                    return num
                return None
            
            if isinstance(val, (int, float)):
                num = float(val)
                if num <= 0:
                    return None
                if MIN_SHARES <= num < MAX_SHARES:
                    return num
                if num < 100:
                    return validate_and_return(num * 100000000)
                elif num < 1e6:
                    return validate_and_return(num * 10000)
                return None
            
            s = str(val).strip()
            if not s:
                return None
            
            try:
                if '亿' in s:
                    num = float(s.replace('亿', '').replace(',', ''))
                    return validate_and_return(num * 100000000)
                elif '万' in s:
                    num = float(s.replace('万', '').replace(',', ''))
                    return validate_and_return(num * 10000)
                else:
                    num = float(s.replace(',', ''))
                    if num <= 0:
                        return None
                    if MIN_SHARES <= num < MAX_SHARES:
                        return num
                    if num < 100:
                        return validate_and_return(num * 100000000)
                    elif num < 1e6:
                        return validate_and_return(num * 10000)
                    return None
            except Exception:
                return None

        # 方法1（首选）: stock_financial_debt_ths - 获取真实总股本（实收资本）
        # 已验证：海天味业返回 58.52 亿，与年报一致
        # 优势：能返回历史各年的总股本，按报告期对应
        try:
            print(f'  [buffett] 尝试 stock_financial_debt_ths (实收资本)...')
            df_debt = ak.stock_financial_debt_ths(symbol=code)
            if df_debt is not None and len(df_debt) > 0:
                total_shares_data = {}
                for _, row in df_debt.iterrows():
                    period = str(row['报告期'])
                    if period.endswith('-12-31'):
                        capital_val = row.get('实收资本（或股本）', 0)
                        if capital_val and not pd.isna(capital_val):
                            shares = parse_shares_value(capital_val)
                            if shares and 0 < shares < 5e10:
                                total_shares_data[period] = shares

                if total_shares_data:
                    print(f'  [buffett] stock_financial_debt_ths 成功: {len(total_shares_data)}条记录')
                    for p, s in sorted(total_shares_data.items()):
                        print(f'    {p}: {s/1e8:.4f} 亿股')
                    # 填充缺失的年份：用最近的可用值
                    for period in annual_periods:
                        if period not in total_shares_data:
                            all_periods = sorted(total_shares_data.keys())
                            default_shares = total_shares_data[all_periods[-1]] if all_periods else list(total_shares_data.values())[0]
                            total_shares_data[period] = default_shares
                    return total_shares_data
        except Exception as e:
            print(f'  [buffett] stock_financial_debt_ths 失败: {e}')

        # 方法2（兜底）: stock_zh_a_daily (新浪) - 获取流通股本（不准确，仅作兜底）
        # 注意：outstanding_share 实际是"流通股本"，会小于"总股本"
        try:
            print(f'  [buffett] 尝试 stock_zh_a_daily (新浪，流通股本兜底)...')
            # 获取最近的交易日数据
            suffix = 'sz' if code.startswith('0') or code.startswith('3') else 'sh'
            today = pd.Timestamp.now()
            start_date = (today - pd.Timedelta(days=10)).strftime('%Y%m%d')
            end_date = today.strftime('%Y%m%d')

            df_daily = ak.stock_zh_a_daily(
                symbol=f'{suffix}{code}',
                start_date=start_date,
                end_date=end_date,
                adjust=''
            )

            if df_daily is not None and len(df_daily) > 0:
                # 查找 outstanding_share 字段
                if 'outstanding_share' in df_daily.columns:
                    shares_val = df_daily['outstanding_share'].iloc[-1]
                    if shares_val and not pd.isna(shares_val):
                        shares_float = float(shares_val)
                        if shares_float > 0 and shares_float < 5e10:
                            total_shares_data = {}
                            for period in annual_periods:
                                total_shares_data[period] = shares_float
                            print(f'  [buffett] stock_zh_a_daily 成功(流通股本，不准确): {shares_float/1e8:.2f}亿股')
                            return total_shares_data
        except Exception as e:
            print(f'  [buffett] stock_zh_a_daily 失败: {e}')
        
        # 所有方法都失败
        print(f'  [buffett] 所有总股本接口均失败')
        return {}

    def _fetch_non_fq_price_data(self, code: str, start_year: int = 2015):
        """获取未复权价格数据（用于计算实际市值），限制日期范围"""
        import akshare as ak
        import time
        import random
        
        non_fq_close_dict = {}
        start_date_fmt = f"{start_year}0101"
        start_date_iso = f"{start_year}-01-01"
        
        def add_suffix(code):
            if code.startswith('6'):
                return f'sh{code}'
            else:
                return f'sz{code}'
        
        def try_fetch_tx():
            """通过腾讯接口获取不复权价格"""
            code_tx = add_suffix(code)
            try:
                df_price = ak.stock_zh_a_hist_tx(
                    symbol=code_tx,
                    start_date=start_date_fmt,
                    end_date=time.strftime("%Y%m%d"),
                    adjust=""
                )
                if df_price is not None and len(df_price) > 0:
                    date_col = '日期' if '日期' in df_price.columns else 'date'
                    close_col = '收盘' if '收盘' in df_price.columns else 'close'
                    df_price['日期'] = pd.to_datetime(df_price[date_col])
                    non_fq_close_dict.update(df_price.set_index('日期')[close_col].to_dict())
                    print(f'  [buffett] 腾讯不复权({code_tx}): {len(non_fq_close_dict)} 条')
                    return True
            except Exception as e:
                print(f'  [buffett] 腾讯不复权 失败: {e}')
            return False
        
        def try_fetch_sina():
            """通过新浪接口获取不复权价格"""
            code_sina = add_suffix(code)
            try:
                df_price = ak.stock_zh_a_daily(
                    symbol=code_sina,
                    start_date=start_date_fmt,
                    end_date=time.strftime("%Y%m%d"),
                    adjust=""
                )
                if df_price is not None and len(df_price) > 0:
                    date_col = '日期' if '日期' in df_price.columns else 'date'
                    close_col = '收盘' if '收盘' in df_price.columns else 'close'
                    df_price['日期'] = pd.to_datetime(df_price[date_col])
                    non_fq_close_dict.update(df_price.set_index('日期')[close_col].to_dict())
                    print(f'  [buffett] 新浪不复权({code_sina}): {len(non_fq_close_dict)} 条')
                    return True
            except Exception as e:
                print(f'  [buffett] 新浪不复权 失败: {e}')
            return False
        
        # 带超时的调用
        if not _run_with_timeout(try_fetch_tx, 8):
            _run_with_timeout(try_fetch_sina, 8)

        if not non_fq_close_dict:
            _run_with_timeout(try_fetch_sina, 8)
        
        if not non_fq_close_dict:
            print(f'  [buffett] 所有不复权价格接口均失败')
        
        return non_fq_close_dict

    def _fetch_fq_price_data(self, code: str, start_year: int = 2015):
        """获取后复权价格数据（用于计算持有收益），限制日期范围减少请求量"""
        import akshare as ak
        import time
        import random
        
        fq_close_dict = {}
        start_date_fmt = f"{start_year}0101"
        start_date_iso = f"{start_year}-01-01"
        
        def add_suffix(code):
            if code.startswith('6'):
                return f'sh{code}'
            else:
                return f'sz{code}'
        
        def try_fetch(method, symbol, adjust, desc, date_format="YYYYMMDD", has_period=True):
            nonlocal fq_close_dict
            for retry in range(2):
                try:
                    if date_format == "YYYYMMDD":
                        if has_period:
                            df_fq = method(symbol=symbol, period="daily", 
                                          start_date=start_date_fmt, 
                                          end_date=time.strftime("%Y%m%d"), 
                                          adjust=adjust)
                        else:
                            df_fq = method(symbol=symbol, 
                                          start_date=start_date_fmt, 
                                          end_date=time.strftime("%Y%m%d"), 
                                          adjust=adjust)
                    else:
                        if has_period:
                            df_fq = method(symbol=symbol, period="daily", 
                                          start_date=start_date_iso, 
                                          end_date=time.strftime("%Y-%m-%d"), 
                                          adjust=adjust)
                        else:
                            df_fq = method(symbol=symbol, 
                                          start_date=start_date_iso, 
                                          end_date=time.strftime("%Y-%m-%d"), 
                                          adjust=adjust)
                    if df_fq is not None and len(df_fq) > 0:
                        date_col = '日期' if '日期' in df_fq.columns else 'date'
                        close_col = '收盘' if '收盘' in df_fq.columns else 'close'
                        df_fq['日期'] = pd.to_datetime(df_fq[date_col])
                        fq_close_dict = df_fq.set_index('日期')[close_col].to_dict()
                        print(f'  [buffett] {desc}: {len(fq_close_dict)} 条')
                        return True
                except Exception as e:
                    print(f'  [buffett] {desc} 第{retry+1}次失败: {e}')
                    if retry < 1:
                        time.sleep(random.uniform(1, 2))
            return False
        
        # 使用超时机制包裹API调用
        def fetch_via_tx():
            return try_fetch(ak.stock_zh_a_hist_tx, add_suffix(code), "hfq", 
                           f"腾讯后复权({add_suffix(code)})", date_format="YYYYMMDD", has_period=False)
        
        def fetch_via_em():
            return try_fetch(ak.stock_zh_a_hist, code, "hfq", "东财后复权", 
                           date_format="YYYY-MM-DD", has_period=True)
        
        def fetch_via_sina():
            code_sina = add_suffix(code)
            return try_fetch(ak.stock_zh_a_daily, code_sina, "hfq", 
                           f"新浪后复权({code_sina})", date_format="YYYYMMDD", has_period=False)
        
        def fetch_qfq_em():
            return try_fetch(ak.stock_zh_a_hist, code, "qfq", "东财前复权", 
                           date_format="YYYY-MM-DD", has_period=True)
        
        def fetch_qfq_sina():
            code_sina = add_suffix(code)
            return try_fetch(ak.stock_zh_a_daily, code_sina, "qfq", 
                           f"新浪前复权({code_sina})", date_format="YYYYMMDD", has_period=False)
        
        def fetch_qfq_tx():
            code_tx = add_suffix(code)
            return try_fetch(ak.stock_zh_a_hist_tx, code_tx, "qfq", 
                           f"腾讯前复权({code_tx})", date_format="YYYYMMDD", has_period=False)
        
        if _run_with_timeout(fetch_via_tx, 8) and fq_close_dict:
            pass
        elif not fq_close_dict and _run_with_timeout(fetch_via_em, 8) and fq_close_dict:
            pass
        elif not fq_close_dict and _run_with_timeout(fetch_via_sina, 8) and fq_close_dict:
            pass
        elif not fq_close_dict and _run_with_timeout(fetch_qfq_em, 8) and fq_close_dict:
            pass
        elif not fq_close_dict and _run_with_timeout(fetch_qfq_sina, 8) and fq_close_dict:
            pass
        elif not fq_close_dict and _run_with_timeout(fetch_qfq_tx, 8) and fq_close_dict:
            pass
        
        if not fq_close_dict:
            print(f'  [buffett] 所有复权价格接口均失败，无法获取复权价格')
        
        return fq_close_dict

    def _fetch_latest_non_fq_price(self, code: str):
        """获取最新不复权收盘价（单位：元）"""
        import akshare as ak
        import time
        import random
        
        def add_suffix(c):
            if c.startswith('6'):
                return 'sh' + c
            elif c.startswith('0') or c.startswith('3'):
                return 'sz' + c
            return c
        
        try_fetch_methods = [
            (ak.stock_zh_a_hist_tx, add_suffix(code), "", "腾讯不复权", "YYYYMMDD", False),
            (ak.stock_zh_a_hist, code, "", "东财不复权", "YYYY-MM-DD", True),
            (ak.stock_zh_a_daily, add_suffix(code), "", f"新浪不复权({add_suffix(code)})", "YYYYMMDD", False),
        ]
        
        for method, symbol, adjust, desc, date_format, has_period in try_fetch_methods:
            for retry in range(2):
                try:
                    if date_format == "YYYYMMDD":
                        if has_period:
                            df_kline = method(symbol=symbol, period="daily", 
                                          start_date="20260101", 
                                          end_date=time.strftime("%Y%m%d"), 
                                          adjust=adjust)
                        else:
                            df_kline = method(symbol=symbol, 
                                          start_date="20260101", 
                                          end_date=time.strftime("%Y%m%d"), 
                                          adjust=adjust)
                    else:
                        if has_period:
                            df_kline = method(symbol=symbol, period="daily", 
                                          start_date="2026-01-01", 
                                          end_date=time.strftime("%Y-%m-%d"), 
                                          adjust=adjust)
                        else:
                            df_kline = method(symbol=symbol, 
                                          start_date="2026-01-01", 
                                          end_date=time.strftime("%Y-%m-%d"), 
                                          adjust=adjust)
                    if df_kline is not None and len(df_kline) > 0:
                        date_col = '日期' if '日期' in df_kline.columns else 'date'
                        close_col = '收盘' if '收盘' in df_kline.columns else 'close'
                        df_kline['日期'] = pd.to_datetime(df_kline[date_col])
                        df_kline = df_kline.sort_values(by='日期')
                        last_row = df_kline.iloc[-1]
                        close_price = float(last_row[close_col])
                        if close_price > 0:
                            print(f'  [buffett] {desc} 获取不复权收盘价成功: {close_price}')
                            return close_price
                except Exception as e:
                    print(f'  [buffett] {desc} 第{retry+1}次失败: {e}')
                    if retry < 1:
                        time.sleep(random.uniform(1, 2))
        
        return None

    def _fetch_non_fq_price_at_date(self, code: str, year: str):
        """获取指定年份的不复权收盘价（用于计算起始市值）"""
        import akshare as ak
        import time
        import random
        
        def add_suffix(c):
            if c.startswith('6'):
                return 'sh' + c
            elif c.startswith('0') or c.startswith('3'):
                return 'sz' + c
            return c
        
        try_fetch_methods = [
            (ak.stock_zh_a_hist_tx, add_suffix(code), "", "腾讯不复权", "YYYYMMDD", False),
            (ak.stock_zh_a_hist, code, "", "东财不复权", "YYYY-MM-DD", True),
            (ak.stock_zh_a_daily, add_suffix(code), "", f"新浪不复权({add_suffix(code)})", "YYYYMMDD", False),
        ]
        
        target_year = int(year)
        start_date_str = f"{target_year}-01-01"
        end_date_str = f"{target_year + 1}-12-31"
        
        for method, symbol, adjust, desc, date_format, has_period in try_fetch_methods:
            for retry in range(2):
                try:
                    if date_format == "YYYYMMDD":
                        start_date = start_date_str.replace('-', '')
                        end_date = end_date_str.replace('-', '')
                        if has_period:
                            df_kline = method(symbol=symbol, period="daily", 
                                          start_date=start_date, 
                                          end_date=end_date, 
                                          adjust=adjust)
                        else:
                            df_kline = method(symbol=symbol, 
                                          start_date=start_date, 
                                          end_date=end_date, 
                                          adjust=adjust)
                    else:
                        if has_period:
                            df_kline = method(symbol=symbol, period="daily", 
                                          start_date=start_date_str, 
                                          end_date=end_date_str, 
                                          adjust=adjust)
                        else:
                            df_kline = method(symbol=symbol, 
                                          start_date=start_date_str, 
                                          end_date=end_date_str, 
                                          adjust=adjust)
                    if df_kline is not None and len(df_kline) > 0:
                        date_col = '日期' if '日期' in df_kline.columns else 'date'
                        close_col = '收盘' if '收盘' in df_kline.columns else 'close'
                        df_kline['日期'] = pd.to_datetime(df_kline[date_col])
                        
                        target_date = pd.Timestamp(f"{target_year + 1}-05-01")
                        sorted_dates = sorted(df_kline['日期'].tolist())
                        close_price = None
                        for d in reversed(sorted_dates):
                            if d <= target_date:
                                candidate = float(df_kline.loc[df_kline['日期'] == d, close_col].iloc[0])
                                if candidate and candidate > 0:
                                    close_price = candidate
                                    break
                        
                        if close_price and close_price > 0:
                            print(f'  [buffett] {desc} 获取{year}年不复权收盘价成功: {close_price}')
                            return close_price
                except Exception as e:
                    print(f'  [buffett] {desc} 获取{year}年不复权收盘价第{retry+1}次失败: {e}')
                    if retry < 1:
                        time.sleep(random.uniform(1, 2))
        
        return None

    def _calculate_yearly_data(self, annual_periods: list, net_profit_data: dict, 
                              div_df: pd.DataFrame, total_shares_data: dict):
        """计算年度数据"""
        yearly_data = []
        total_net_profit = 0
        total_dividend = 0
        total_retained_earnings = 0
        
        # 记录修正后的股本数据（仅用于分红计算）
        dividend_shares_data = total_shares_data.copy()
        
        if len(div_df) > 0 and total_shares_data:
            for period in annual_periods:
                year = period[:4]
                shares = total_shares_data.get(period, 0)
                if shares > 0:
                    yearly_dividend = 0
                    total_per_share_div = 0
                    for _, row in div_df.iterrows():
                        report_year_val = row.get('报告时间', row.get('report_year', row.get('year', '')))
                        if report_year_val:
                            try:
                                report_year_str = str(report_year_val)
                                if '年报' in report_year_str:
                                    report_year = report_year_str.replace('年报', '')[:4]
                                else:
                                    report_year = report_year_str[:4]
                                if report_year == year:
                                    per_share_div = float(row.get('派息比例', row.get('per_share_dividend', row.get('dividend_per_share', 0))))
                                    if per_share_div > 0:
                                        total_per_share_div += per_share_div
                                        yearly_dividend += per_share_div * shares / 10
                            except Exception:
                                pass
                    
                    if total_per_share_div > 0:
                        dividend_yi = yearly_dividend / 100000000
                        implied_dividend_yi = total_per_share_div * (shares / 10) / 100000000
                        net_profit = net_profit_data.get(period)
                        if net_profit is not None:
                            net_profit_yi = net_profit / 100000000
                            payout_ratio = dividend_yi / net_profit_yi if net_profit_yi > 0 else 0
                            
                            if payout_ratio < 0.1 and net_profit_yi > 10 and shares < 10000000000:
                                # 仅修正分红计算用的股本数据
                                adjusted_shares = shares * 10
                                dividend_shares_data[period] = adjusted_shares
                                corrected_dividend = total_per_share_div * adjusted_shares / 10 / 100000000
                                print(f'  [buffett] 修正分红股本 {period}: 原={shares/100000000}亿股, 修正后={adjusted_shares/100000000}亿股 (派息率={payout_ratio:.2%}, 修正后分红={corrected_dividend:.2f}亿)')
        
        for period in annual_periods:
            year = period[:4]
            net_profit = net_profit_data.get(period)
            
            yearly_dividend = 0
            if len(div_df) > 0:
                for _, row in div_df.iterrows():
                    report_year_val = row.get('报告时间', row.get('report_year', row.get('year', '')))
                    if report_year_val:
                        try:
                            report_year_str = str(report_year_val)
                            if '年报' in report_year_str:
                                report_year = report_year_str.replace('年报', '')[:4]
                            else:
                                report_year = report_year_str[:4]
                            if report_year == year:
                                per_share_div = float(row.get('派息比例', row.get('per_share_dividend', row.get('dividend_per_share', 0))))
                                # 使用修正后的股本数据计算分红
                                shares = dividend_shares_data.get(period, 0)
                                if per_share_div > 0 and shares > 0:
                                    yearly_dividend += per_share_div * shares / 10
                        except Exception:
                            pass
            
            dividend_amount = yearly_dividend / 100000000 if yearly_dividend > 0 else None
            retained_earnings = None
            if net_profit is not None:
                net_profit_yi = net_profit / 100000000
                retained_earnings = net_profit_yi - (dividend_amount or 0)
            
            yearly_data.append({
                'year': year,
                'period': period,
                'net_profit': net_profit_yi if net_profit is not None else None,
                'dividend': dividend_amount,
                'retained_earnings': retained_earnings,
                'total_shares': total_shares_data.get(period, 0)  # 使用原始股本数据
            })
            
            if net_profit is not None:
                total_net_profit += net_profit_yi
            if yearly_dividend > 0:
                total_dividend += dividend_amount
            if retained_earnings is not None:
                total_retained_earnings += retained_earnings
        
        return yearly_data, total_net_profit, total_dividend, total_retained_earnings, total_shares_data

    def _calculate_market_cap(self, start_year: str, end_year: str, 
                            fq_close_dict: dict, total_shares_data: dict):
        """计算起始市值和当前市值（后复权，使用年报次年5月1日的价格）"""
        start_market_cap = None
        current_market_cap = None
        start_fq_price = None
        current_fq_price = None
        start_fq_date = None
        current_fq_date = None
        
        if start_year and end_year and fq_close_dict and total_shares_data:
            # 获取总股本（所有年份应该相同）
            current_shares = list(total_shares_data.values())[0]
            
            start_next_year = int(start_year) + 1
            start_date_str = f"{start_next_year}-05-01"
            try:
                start_date = pd.Timestamp(start_date_str)
                sorted_dates = sorted(fq_close_dict.keys())
                start_price = None
                start_price_date = None
                for d in reversed(sorted_dates):
                    if d <= start_date:
                        start_price = fq_close_dict[d]
                        start_price_date = d
                        break
                
                start_fq_price = start_price
                start_fq_date = str(start_price_date) if start_price_date else None
                
                if start_price and start_price > 0 and current_shares > 0:
                    start_market_cap = start_price * current_shares / 100000000
            except Exception as e:
                print(f'  [buffett] 计算起始市值失败: {e}')
            
            from datetime import date
            today = date.today()
            end_date_str = today.strftime("%Y-%m-%d")
            try:
                end_date = pd.Timestamp(end_date_str)
                sorted_dates = sorted(fq_close_dict.keys())
                current_price = None
                current_price_date = None
                for d in reversed(sorted_dates):
                    if d <= end_date:
                        current_price = fq_close_dict[d]
                        current_price_date = d
                        break
                
                current_fq_price = current_price
                current_fq_date = str(current_price_date) if current_price_date else None
                
                if current_price and current_price > 0 and current_shares > 0:
                    current_market_cap = current_price * current_shares / 100000000
            except Exception as e:
                print(f'  [buffett] 计算当前市值失败: {e}')
        
        return start_market_cap, current_market_cap, start_fq_price, current_fq_price, start_fq_date, current_fq_date

    def _fetch_current_market_cap(self, code: str):
        """直接从东方财富获取当前市值（单位：亿）"""
        import akshare as ak
        try:
            df_spot = ak.stock_zh_a_spot_em()
            mask = df_spot['代码'] == code
            if mask.any():
                row = df_spot[mask].iloc[0]
                market_cap_val = row.get('总市值', row.get('流通市值', 0))
                if market_cap_val and not pd.isna(market_cap_val):
                    try:
                        market_cap_float = float(market_cap_val)
                        if market_cap_float > 0 and market_cap_float < 1e18:
                            return market_cap_float
                    except Exception:
                        pass
        except Exception as e:
            print(f'  [buffett] _fetch_current_market_cap({code}) 失败: {e}')
        return None

    def _calculate_growth_rates(self, start_market_cap: float, current_market_cap: float,
                              total_retained_earnings: float):
        """计算市值增长和留存比值（旧版，保留兼容）"""
        market_cap_growth = None
        retained_growth_rate = None

        if start_market_cap is not None and current_market_cap is not None:
            market_cap_growth = current_market_cap - start_market_cap

        if market_cap_growth is not None and total_retained_earnings is not None and total_retained_earnings != 0:
            retained_growth_rate = (market_cap_growth / total_retained_earnings) * 100

        return market_cap_growth, retained_growth_rate

    def _calculate_retained_growth_rate_v2(self, start_market_cap, current_market_cap,
                                           total_dividend, total_retained_earnings):
        """计算留存比值 = 市值增长 / 总留存收益 = (当前市值 - 起始市值) / 总留存收益"""
        if (start_market_cap is not None and current_market_cap is not None and
            total_retained_earnings is not None and total_retained_earnings != 0):
            return ((current_market_cap - start_market_cap) / total_retained_earnings) * 100
        return None

    def _calculate_yearly_market_cap(self, annual_periods: list, price_dict: dict, total_shares_data: dict):
        """计算每年的市值：使用年报次年5月1日（或最近交易日）的后复权收盘价 × 当年总股本"""
        yearly_market_cap = {}
        yearly_fq_price = {}
        yearly_price_date = {}
        
        print(f'  [buffett] _calculate_yearly_market_cap: price_dict size={len(price_dict)}, total_shares_data size={len(total_shares_data)}')
        
        if price_dict and total_shares_data:
            for period in annual_periods:
                year = period[:4]
                try:
                    next_year = int(year) + 1
                    target_date = pd.Timestamp(f"{next_year}-05-01")
                    sorted_dates = sorted(price_dict.keys())
                    price = None
                    price_date = None
                    for d in reversed(sorted_dates):
                        if d <= target_date:
                            price = price_dict[d]
                            price_date = d
                            break
                    
                    shares = total_shares_data.get(period, 0)
                    if shares == 0 and len(total_shares_data) > 0:
                        shares = list(total_shares_data.values())[0]
                    
                    print(f'  [buffett] 年度市值计算 {period}: target_date={target_date}, actual_date={price_date}, price={price}, shares={shares}')
                    
                    if price and price > 0 and shares > 0 and shares < 1e18:
                        market_cap = price * shares / 100000000
                        if market_cap > 0 and market_cap < 1e8:
                            yearly_market_cap[period] = market_cap
                            yearly_fq_price[period] = price
                            yearly_price_date[period] = str(price_date) if price_date else None
                            print(f'  [buffett] 年度市值计算成功 {period}: date={yearly_price_date[period]}, price={price}, shares={shares}, market_cap={market_cap} 亿')
                        else:
                            print(f'  [buffett] 市值计算异常 {period}: price={price}, shares={shares}, market_cap={market_cap}')
                    else:
                        print(f'  [buffett] 市值计算跳过 {period}: price={price}, shares={shares}')
                except Exception as e:
                    print(f'  [buffett] 计算年度市值 {period} 失败: {e}')
        
        print(f'  [buffett] yearly_market_cap 结果: {list(yearly_market_cap.keys())}')
        return yearly_market_cap, yearly_fq_price, yearly_price_date

    def _calculate_depreciation_amortization(self, annual_periods: list, cashflow_data: dict):
        """计算折旧与摊销 = 固定资产折旧+无形资产摊销+长期待摊费用摊销"""
        depreciation_amortization_data = {}
        
        depreciation_data = cashflow_data.get('固定资产折旧、油气资产折耗、生产性生物资产折旧', {})
        intangible_amortization_data = cashflow_data.get('无形资产摊销', {})
        long_term_amortization_data = cashflow_data.get('长期待摊费用摊销', {})
        
        for period in annual_periods:
            depreciation = depreciation_data.get(period, 0) or 0
            intangible_amortization = intangible_amortization_data.get(period, 0) or 0
            long_term_amortization = long_term_amortization_data.get(period, 0) or 0
            
            total = depreciation + intangible_amortization + long_term_amortization
            if total > 0:
                depreciation_amortization_data[period] = total / 100000000
            else:
                depreciation_amortization_data[period] = None
        
        return depreciation_amortization_data

    def _calculate_expansion_capex(self, code: str, annual_periods: list, balance_data: dict):
        """计算扩张性资本支出"""
        expansion_capex_data = {}
        
        try:
            import akshare as ak
            df_debt = ak.stock_financial_debt_ths(symbol=code)
            if df_debt is not None and len(df_debt) > 0:
                construction_data = balance_data.get('在建工程合计', {})
                prev_value = None
                
                for i, period in enumerate(sorted(annual_periods)):
                    current_value = construction_data.get(period)
                    if current_value is not None and prev_value is not None:
                        expansion = current_value - prev_value
                        if expansion > 0:
                            expansion_capex_data[period] = expansion / 100000000
                        else:
                            expansion_capex_data[period] = None
                    else:
                        expansion_capex_data[period] = None
                    prev_value = current_value
        except Exception as e:
            print(f'  [buffett] 计算扩张性资本支出失败: {e}')
        
        return expansion_capex_data

    def _calculate_maintenance_capex(self, annual_periods: list, capex_data: dict, expansion_capex_data: dict):
        """计算维持性资本支出 = 购建固定资产支付的现金 - 扩张性资本支出"""
        maintenance_capex_data = {}
        
        for period in annual_periods:
            capex = capex_data.get(period)
            expansion_capex = expansion_capex_data.get(period)
            
            if capex is not None:
                capex_yi = capex / 100000000
                if expansion_capex is not None:
                    maintenance_capex = capex_yi - expansion_capex
                    if maintenance_capex < 0:
                        maintenance_capex = None
                else:
                    maintenance_capex = capex_yi
                
                maintenance_capex_data[period] = maintenance_capex
            else:
                maintenance_capex_data[period] = None
        
        return maintenance_capex_data

    def _calculate_shareholder_earnings(self, annual_periods: list, net_profit_data: dict, 
                                       depreciation_amortization_data: dict, maintenance_capex_data: dict):
        """计算股东盈余 = 归属于母公司净利润 + 折旧与摊销 - 维持性资本支出"""
        shareholder_earnings_data = {}
        
        for period in annual_periods:
            net_profit = net_profit_data.get(period)
            depreciation_amortization = depreciation_amortization_data.get(period)
            maintenance_capex = maintenance_capex_data.get(period)
            
            if net_profit is not None:
                net_profit_yi = net_profit / 100000000
                depreciation_amortization = depreciation_amortization or 0
                maintenance_capex = maintenance_capex or 0
                
                shareholder_earnings = net_profit_yi + depreciation_amortization - maintenance_capex
                shareholder_earnings_data[period] = shareholder_earnings
            else:
                shareholder_earnings_data[period] = None
        
        return shareholder_earnings_data

    def _download_annual_report_pdf(self, code: str, year: str) -> str:
        """从巨潮资讯网下载年报PDF"""
        pdf_path = None
        
        pdf_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'annual_reports')
        os.makedirs(pdf_dir, exist_ok=True)
        pdf_filename = f"{code}_{year}_annual_report.pdf"
        pdf_path = os.path.join(pdf_dir, pdf_filename)
        
        if os.path.exists(pdf_path):
            print(f'  [buffett] 年报PDF已存在: {pdf_path}')
            return pdf_path
        
        try:
            cninfo_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
            
            seid = 'sh' if code.startswith('6') else 'sz'
            
            params = {
                'pageNum': '1',
                'pageSize': '30',
                'tabName': 'fulltext',
                'column': seid,
                'searchkey': f'{year}年年度报告',
                'secid': f'{seid}.{code}',
                'sortName': '',
                'sortType': '',
                'limit': '',
                'seDate': f'{year}-01-01~{year}-12-31',
                'category': 'category_ndbg_szsh',
                'trade': '',
                'seCategory': '',
                'reportType': '',
                'subcolumnName': '',
                'extend': '',
                'filter': ''
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            }
            
            response = requests.post(cninfo_url, data=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('announcements'):
                    for announcement in data['announcements']:
                        title = str(announcement.get('title', ''))
                        if f'{year}年年度报告' in title or f'{year}年报' in title:
                            pdf_link = announcement.get('adjunctUrl', '')
                            if pdf_link and pdf_link.endswith('.pdf'):
                                pdf_download_url = f'http://www.cninfo.com.cn/{pdf_link}'
                                
                                download_headers = headers.copy()
                                download_headers['Referer'] = f'http://www.cninfo.com.cn/new/Detail?plate=szsh&orgId=gssz000{code}&stock={code}&tabname=fulltext'
                                
                                pdf_response = requests.get(pdf_download_url, headers=download_headers, timeout=60)
                                if pdf_response.status_code == 200:
                                    with open(pdf_path, 'wb') as f:
                                        f.write(pdf_response.content)
                                    print(f'  [buffett] 下载年报PDF成功: {pdf_path}')
                                    return pdf_path
                                else:
                                    print(f'  [buffett] 下载PDF失败，状态码: {pdf_response.status_code}')
        except Exception as e:
            print(f'  [buffett] 下载年报PDF失败 {year}: {e}')
        
        return None

    def _parse_pdf_for_depreciation(self, pdf_path: str) -> dict:
        """解析PDF年报提取折旧摊销明细（返回单位：元）"""
        result = {
            'depreciation': None,
            'intangible_amortization': None,
            'long_term_amortization': None
        }
        
        def parse_amount(text, key_patterns):
            for pattern in key_patterns:
                match = re.search(pattern, text)
                if match:
                    val = float(match.group(1).replace(',', ''))
                    unit = match.group(2) if len(match.groups()) > 1 else '元'
                    if unit == '亿':
                        return val * 100000000
                    elif unit == '万元':
                        return val * 10000
                    else:
                        return val
            return None
        
        try:
            import pdfplumber
            
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        if result['depreciation'] is None:
                            dep_patterns = [
                                r'固定资产折旧[^\d]*([\d,.]+)\s*(万元|亿|元)',
                                r'固定资产折旧[^\d]*([\d,.]+)'
                            ]
                            dep_val = parse_amount(text, dep_patterns)
                            if dep_val and dep_val > 0:
                                result['depreciation'] = dep_val
                        
                        if result['intangible_amortization'] is None:
                            int_patterns = [
                                r'无形资产摊销[^\d]*([\d,.]+)\s*(万元|亿|元)',
                                r'无形资产摊销[^\d]*([\d,.]+)'
                            ]
                            int_val = parse_amount(text, int_patterns)
                            if int_val and int_val > 0:
                                result['intangible_amortization'] = int_val
                        
                        if result['long_term_amortization'] is None:
                            lta_patterns = [
                                r'长期待摊费用摊销[^\d]*([\d,.]+)\s*(万元|亿|元)',
                                r'长期待摊费用摊销[^\d]*([\d,.]+)'
                            ]
                            lta_val = parse_amount(text, lta_patterns)
                            if lta_val and lta_val > 0:
                                result['long_term_amortization'] = lta_val
                    
                    if all(v is not None for v in result.values()):
                        break
        except Exception as e:
            print(f'  [buffett] 解析PDF折旧数据失败: {e}')
        
        return result

    def _parse_pdf_for_construction_projects(self, pdf_path: str) -> float:
        """解析PDF年报提取重要在建工程项目本期增加合计额（返回单位：元）"""
        total_addition = None
        
        try:
            import pdfplumber
            
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        if '重要在建工程' in text or '在建工程项目' in text:
                            tables = page.extract_tables()
                            for table in tables:
                                if table and len(table) > 2:
                                    for row in table:
                                        row_str = str(row).lower()
                                        if '合计' in row_str or '增加' in row_str:
                                            for cell in row:
                                                if cell:
                                                    cell_str = str(cell)
                                                    num_match = re.search(r'([\d,.]+)\s*(万元|亿)', cell_str)
                                                    if num_match:
                                                        val = float(num_match.group(1).replace(',', ''))
                                                        unit = num_match.group(2)
                                                        if unit == '亿':
                                                            val = val * 100000000
                                                        elif unit == '万元':
                                                            val = val * 10000
                                                        if total_addition is None or val > total_addition:
                                                            total_addition = val
                            
                            if total_addition is not None:
                                break
                            
                            lines = text.split('\n')
                            for line in lines:
                                if '本期增加' in line or '增加合计' in line:
                                    num_match = re.search(r'([\d,.]+)\s*(万元|亿)', line)
                                    if num_match:
                                        val = float(num_match.group(1).replace(',', ''))
                                        unit = num_match.group(2)
                                        if unit == '亿':
                                            val = val * 100000000
                                        elif unit == '万元':
                                            val = val * 10000
                                        if total_addition is None or val > total_addition:
                                            total_addition = val
                            break
        except Exception as e:
            print(f'  [buffett] 解析PDF在建工程数据失败: {e}')
        
        return total_addition

    def _calculate_expansion_capex_from_pdf(self, code: str, annual_periods: list) -> dict:
        """从PDF年报计算扩张性资本支出（单位：亿元）"""
        expansion_capex_data = {}
        
        for period in annual_periods:
            year = period[:4]
            pdf_path = self._download_annual_report_pdf(code, year)
            if pdf_path and os.path.exists(pdf_path):
                total_addition = self._parse_pdf_for_construction_projects(pdf_path)
                if total_addition is not None:
                    expansion_capex_data[period] = total_addition / 100000000
        
        return expansion_capex_data

    def _supplement_data_from_pdf(self, code: str, annual_periods: list, cashflow_data: dict, 
                                  depreciation_amortization_data: dict, expansion_capex_data: dict):
        """从PDF年报补充缺失的折旧摊销和扩张性资本支出数据"""
        for period in annual_periods:
            year = period[:4]
            
            needs_supplement = False
            if depreciation_amortization_data.get(period) is None:
                needs_supplement = True
            if expansion_capex_data.get(period) is None:
                needs_supplement = True
            
            if needs_supplement:
                pdf_path = self._download_annual_report_pdf(code, year)
                if pdf_path and os.path.exists(pdf_path):
                    if depreciation_amortization_data.get(period) is None:
                        dep_result = self._parse_pdf_for_depreciation(pdf_path)
                        if dep_result['depreciation'] is not None:
                            dep_sum = dep_result['depreciation'] or 0
                            dep_sum += dep_result['intangible_amortization'] or 0
                            dep_sum += dep_result['long_term_amortization'] or 0
                            if dep_sum > 0:
                                depreciation_amortization_data[period] = dep_sum / 100000000
                                print(f'  [buffett] 从PDF补充折旧与摊销 {period}: {dep_sum/100000000} 亿')
                    
                    if expansion_capex_data.get(period) is None:
                        total_addition = self._parse_pdf_for_construction_projects(pdf_path)
                        if total_addition is not None:
                            expansion_capex_data[period] = total_addition / 100000000
                            print(f'  [buffett] 从PDF补充扩张性资本支出 {period}: {total_addition/100000000} 亿')
