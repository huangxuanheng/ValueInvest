import pandas as pd
import requests
import os
import re
from ..utils.stock_info import fetch_sec_name
from .finance_service import fetch_financial_data


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
        
        div_df = self._fetch_dividend_data(code)
        
        total_shares_data = self._fetch_total_shares_data(code, annual_periods)
        
        print(f'  [buffett] total_shares_data keys: {list(total_shares_data.keys())}')
        
        fq_close_dict = self._fetch_fq_price_data(code)
        
        non_fq_close_dict = self._fetch_non_fq_price_data(code)
        
        yearly_data, total_net_profit, total_dividend, total_retained_earnings, adjusted_shares_data = \
            self._calculate_yearly_data(annual_periods, net_profit_data, div_df, total_shares_data)
        
        yearly_data.sort(key=lambda x: x['year'])
        
        start_year = yearly_data[0]['year'] if yearly_data else None
        end_year = yearly_data[-1]['year'] if yearly_data else None
        
        yearly_market_cap, yearly_non_fq_price = self._calculate_yearly_market_cap(annual_periods, non_fq_close_dict, adjusted_shares_data)
        
        depreciation_amortization_data = self._calculate_depreciation_amortization(annual_periods, cashflow_data)
        
        capex_data = cashflow_data.get('购建固定资产、无形资产和其他长期资产支付的现金', {})
        
        expansion_capex_data = self._calculate_expansion_capex(code, annual_periods, balance_data)
        
        self._supplement_data_from_pdf(code, annual_periods, cashflow_data, depreciation_amortization_data, expansion_capex_data)
        
        maintenance_capex_data = self._calculate_maintenance_capex(annual_periods, capex_data, expansion_capex_data)
        
        shareholder_earnings_data = self._calculate_shareholder_earnings(annual_periods, net_profit_data, depreciation_amortization_data, maintenance_capex_data)
        
        start_market_cap, current_market_cap, start_fq_price, current_fq_price, start_fq_date, current_fq_date = \
            self._calculate_market_cap(start_year, end_year, fq_close_dict, adjusted_shares_data)
        
        market_cap_growth, retained_growth_rate = \
            self._calculate_growth_rates(start_market_cap, current_market_cap, total_retained_earnings)
        
        for i, item in enumerate(yearly_data):
            period = item['period']
            item['market_cap'] = yearly_market_cap.get(period)
            item['non_fq_price'] = yearly_non_fq_price.get(period)
            item['total_shares'] = adjusted_shares_data.get(period)
            item['depreciation_amortization'] = depreciation_amortization_data.get(period)
            item['capex'] = capex_data.get(period) / 100000000 if capex_data.get(period) else None
            item['expansion_capex'] = expansion_capex_data.get(period)
            item['maintenance_capex'] = maintenance_capex_data.get(period)
            item['shareholder_earnings'] = shareholder_earnings_data.get(period)
        
        print(f'[buffett] 起始市值: {start_market_cap}, 当前市值: {current_market_cap}')
        print(f'[buffett] 总净利润: {total_net_profit}, 总分红: {total_dividend}, 总留存收益: {total_retained_earnings}')
        if market_cap_growth is not None:
            print(f'[buffett] 市值增长: {market_cap_growth}')
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
            'start_market_cap': start_market_cap,
            'current_market_cap': current_market_cap,
            'start_fq_price': start_fq_price,
            'current_fq_price': current_fq_price,
            'start_fq_date': start_fq_date,
            'current_fq_date': current_fq_date,
            'market_cap_growth': market_cap_growth,
            'retained_growth_rate': retained_growth_rate
        }

    def _fetch_dividend_data(self, code: str):
        """获取分红数据"""
        import akshare as ak
        
        div_df = None
        try:
            div_df = ak.stock_dividend_cninfo(symbol=code)
            print(f'  [buffett] stock_dividend_cninfo 获取到 {len(div_df) if div_df is not None else 0} 条分红记录')
        except Exception as e:
            print(f'  [buffett] stock_dividend_cninfo({code}) 失败: {e}')
        
        if div_df is None:
            div_df = pd.DataFrame()
        
        return div_df

    def _fetch_total_shares_data(self, code: str, annual_periods: list):
        """获取总股本数据（股数，单位：股）"""
        import akshare as ak
        import time
        
        total_shares_data = {}
        
        def parse_shares_value(val):
            if val is None or pd.isna(val):
                return None
            s = str(val).strip()
            if not s:
                return None
            try:
                if '亿' in s:
                    num = float(s.replace('亿', '').replace(',', ''))
                    return num * 100000000
                elif '万' in s:
                    num = float(s.replace('万', '').replace(',', ''))
                    return num * 10000
                else:
                    return float(s.replace(',', ''))
            except Exception:
                return None
        
        def try_api(max_retries=3, delay=2):
            for attempt in range(max_retries):
                try:
                    df_info = ak.stock_individual_info_em(symbol=code)
                    if df_info is not None and len(df_info) > 0:
                        shares_val = df_info.get('总股本', 0)
                        if shares_val and not pd.isna(shares_val):
                            parsed_val = parse_shares_value(shares_val)
                            if parsed_val is not None and parsed_val > 0 and parsed_val < 1e18:
                                print(f'  [buffett] stock_individual_info_em 获取总股本成功(第{attempt+1}次): {parsed_val}')
                                return parsed_val
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                    else:
                        print(f'  [buffett] stock_individual_info_em({code}) 失败(重试{max_retries}次): {e}')
            return None
        
        shares_from_info = try_api()
        if shares_from_info:
            for period in annual_periods:
                total_shares_data[period] = shares_from_info
            return total_shares_data
        
        def try_spot_api(max_retries=3, delay=2):
            for attempt in range(max_retries):
                try:
                    df_spot = ak.stock_zh_a_spot_em()
                    mask = df_spot['代码'] == code
                    if mask.any():
                        row = df_spot[mask].iloc[0]
                        shares_val = row.get('总股本', row.get('流通股本', 0))
                        if shares_val and not pd.isna(shares_val):
                            try:
                                shares_float = float(shares_val)
                                if shares_float > 0:
                                    shares_in_shares = shares_float * 100000000
                                    print(f'  [buffett] stock_zh_a_spot_em 获取总股本成功(第{attempt+1}次): {shares_in_shares}')
                                    return shares_in_shares
                            except Exception:
                                pass
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                    else:
                        print(f'  [buffett] stock_zh_a_spot_em({code}) 失败(重试{max_retries}次): {e}')
            return None
        
        shares_from_spot = try_spot_api()
        if shares_from_spot:
            for period in annual_periods:
                total_shares_data[period] = shares_from_spot
            return total_shares_data
        
        if not total_shares_data:
            try:
                df_benefit = ak.stock_financial_benefit_ths(symbol=code)
                if df_benefit is not None and len(df_benefit) > 0:
                    for _, row in df_benefit.iterrows():
                        period = str(row['报告期'])
                        if period.endswith('-12-31'):
                            shares_val = row.get('总股本', 0)
                            if shares_val and not pd.isna(shares_val):
                                try:
                                    shares_float = float(shares_val)
                                    if shares_float > 0 and shares_float < 1e18:
                                        total_shares_data[period] = shares_float
                                except Exception:
                                    pass
                print(f'  [buffett] stock_financial_benefit_ths 获取到 {len(total_shares_data)} 条总股本记录')
            except Exception as e:
                print(f'  [buffett] stock_financial_benefit_ths({code}) 失败: {e}')
        
        if not total_shares_data:
            try:
                df_debt = ak.stock_financial_debt_ths(symbol=code)
                if df_debt is not None and len(df_debt) > 0:
                    for _, row in df_debt.iterrows():
                        period = str(row['报告期'])
                        if period.endswith('-12-31'):
                            capital_val = row.get('实收资本（或股本）', 0)
                            if capital_val and not pd.isna(capital_val):
                                s = str(capital_val).strip()
                                try:
                                    if '亿' in s:
                                        num = float(s.replace('亿', '').replace(',', ''))
                                        shares = num * 100000000
                                    elif '万' in s:
                                        num = float(s.replace('万', '').replace(',', ''))
                                        shares = num * 10000
                                    else:
                                        shares = float(s.replace(',', ''))
                                    
                                    if shares > 0 and shares < 50000000000:
                                        total_shares_data[period] = shares
                                        print(f'  [buffett] stock_financial_debt_ths {period}: 实收资本={capital_val}, 总股本={shares}')
                                except Exception:
                                    pass
            except Exception as e:
                print(f'  [buffett] stock_financial_debt_ths({code}) 失败: {e}')
        
        if not total_shares_data and len(annual_periods) > 0:
            try:
                df_sina = ak.stock_financial_report_sina(symbol=f"{code}.SH")
                if df_sina is None or len(df_sina) == 0:
                    df_sina = ak.stock_financial_report_sina(symbol=f"{code}.SZ")
                if df_sina is not None and len(df_sina) > 0:
                    for _, row in df_sina.iterrows():
                        period = str(row['report_date'])
                        if period.endswith('-12-31'):
                            shares_val = row.get('total_shares', 0)
                            if shares_val and not pd.isna(shares_val):
                                try:
                                    shares_float = float(shares_val)
                                    if shares_float > 0 and shares_float < 1e18:
                                        total_shares_data[period] = shares_float
                                except Exception:
                                    pass
                print(f'  [buffett] stock_financial_report_sina 获取到 {len(total_shares_data)} 条总股本记录')
            except Exception as e:
                print(f'  [buffett] stock_financial_report_sina({code}) 失败: {e}')
        
        print(f'  [buffett] 最终总股本数据: {list(total_shares_data.keys())}, 示例值: {list(total_shares_data.values())[:3]}')
        return total_shares_data

    def _fetch_non_fq_price_data(self, code: str):
        """获取未复权价格数据（用于计算实际市值）"""
        import akshare as ak
        import time
        import random
        
        non_fq_close_dict = {}
        
        def add_suffix(code):
            if code.startswith('6'):
                return f'sh{code}'
            else:
                return f'sz{code}'
        
        def try_fetch(func, *args, **kwargs):
            nonlocal non_fq_close_dict
            desc = kwargs.pop('desc', '')
            date_format = kwargs.pop('date_format', 'YYYY-MM-DD')
            has_period = kwargs.pop('has_period', True)
            
            for retry in range(3):
                try:
                    if has_period:
                        df_price = func(*args, **kwargs)
                    else:
                        df_price = func(*args)
                    if df_price is not None and len(df_price) > 0:
                        date_col = '日期' if '日期' in df_price.columns else 'date'
                        close_col = '收盘' if '收盘' in df_price.columns else 'close'
                        df_price['日期'] = pd.to_datetime(df_price[date_col])
                        non_fq_close_dict = df_price.set_index('日期')[close_col].to_dict()
                        print(f'  [buffett] {desc}: {len(non_fq_close_dict)} 条')
                        return True
                except Exception as e:
                    print(f'  [buffett] {desc} 第{retry+1}次失败: {e}')
                    if retry < 2:
                        time.sleep(random.uniform(3, 6))
            return False
        
        try_fetch(ak.stock_zh_a_hist, code, period="daily", 
                  start_date="2000-01-01", end_date=time.strftime("%Y-%m-%d"), adjust="",
                  desc="东财未复权价格", date_format="YYYY-MM-DD", has_period=True)
        
        if not non_fq_close_dict:
            code_sina = add_suffix(code)
            try_fetch(ak.stock_zh_a_daily, code_sina, 
                      desc=f"新浪未复权({code_sina})", date_format="YYYYMMDD", has_period=False)
        
        if not non_fq_close_dict:
            code_tx = add_suffix(code)
            try_fetch(ak.stock_zh_a_hist_tx, code_tx, 
                      desc=f"腾讯未复权({code_tx})", date_format="YYYYMMDD", has_period=False)
        
        return non_fq_close_dict

    def _fetch_fq_price_data(self, code: str):
        """获取后复权价格数据（用于计算持有收益）"""
        import akshare as ak
        import time
        import random
        
        fq_close_dict = {}
        
        def add_suffix(code):
            if code.startswith('6'):
                return f'sh{code}'
            else:
                return f'sz{code}'
        
        def try_fetch(method, symbol, adjust, desc, date_format="YYYYMMDD", has_period=True):
            nonlocal fq_close_dict
            for retry in range(3):
                try:
                    if date_format == "YYYYMMDD":
                        if has_period:
                            df_fq = method(symbol=symbol, period="daily", 
                                          start_date="20000101", 
                                          end_date=time.strftime("%Y%m%d"), 
                                          adjust=adjust)
                        else:
                            df_fq = method(symbol=symbol, 
                                          start_date="20000101", 
                                          end_date=time.strftime("%Y%m%d"), 
                                          adjust=adjust)
                    else:
                        if has_period:
                            df_fq = method(symbol=symbol, period="daily", 
                                          start_date="2000-01-01", 
                                          end_date=time.strftime("%Y-%m-%d"), 
                                          adjust=adjust)
                        else:
                            df_fq = method(symbol=symbol, 
                                          start_date="2000-01-01", 
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
                    if retry < 2:
                        time.sleep(random.uniform(3, 6))
            return False
        
        try_fetch(ak.stock_zh_a_hist, code, "hfq", "东财后复权", date_format="YYYY-MM-DD", has_period=True)
        
        if not fq_close_dict:
            code_tx = add_suffix(code)
            try_fetch(ak.stock_zh_a_hist_tx, code_tx, "hfq", f"腾讯后复权({code_tx})", date_format="YYYYMMDD", has_period=False)
        
        if not fq_close_dict:
            code_sina = add_suffix(code)
            try_fetch(ak.stock_zh_a_daily, code_sina, "hfq", f"新浪后复权({code_sina})", date_format="YYYYMMDD", has_period=False)
        
        if not fq_close_dict:
            try_fetch(ak.stock_zh_a_hist, code, "qfq", "东财前复权", date_format="YYYY-MM-DD", has_period=True)
        
        if not fq_close_dict:
            code_sina = add_suffix(code)
            try_fetch(ak.stock_zh_a_daily, code_sina, "qfq", f"新浪前复权({code_sina})", date_format="YYYYMMDD", has_period=False)
        
        if not fq_close_dict:
            code_tx = add_suffix(code)
            try_fetch(ak.stock_zh_a_hist_tx, code_tx, "qfq", f"腾讯前复权({code_tx})", date_format="YYYYMMDD", has_period=False)
        
        if not fq_close_dict:
            print(f'  [buffett] 所有复权价格接口均失败，无法获取复权价格')
        
        return fq_close_dict

    def _calculate_yearly_data(self, annual_periods: list, net_profit_data: dict, 
                              div_df: pd.DataFrame, total_shares_data: dict):
        """计算年度数据"""
        yearly_data = []
        total_net_profit = 0
        total_dividend = 0
        total_retained_earnings = 0
        
        adjusted_shares_data = total_shares_data.copy()
        
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
                                adjusted_shares = shares * 10
                                adjusted_shares_data[period] = adjusted_shares
                                corrected_dividend = total_per_share_div * adjusted_shares / 10 / 100000000
                                print(f'  [buffett] 修正总股本 {period}: 原={shares/100000000}亿股, 修正后={adjusted_shares/100000000}亿股 (派息率={payout_ratio}, 修正后分红={corrected_dividend}亿)')
        
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
                                shares = adjusted_shares_data.get(period, 0)
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
                'total_shares': adjusted_shares_data.get(period, 0)
            })
            
            if net_profit is not None:
                total_net_profit += net_profit_yi
            if yearly_dividend > 0:
                total_dividend += dividend_amount
            if retained_earnings is not None:
                total_retained_earnings += retained_earnings
        
        return yearly_data, total_net_profit, total_dividend, total_retained_earnings, adjusted_shares_data

    def _calculate_market_cap(self, start_year: str, end_year: str, 
                            fq_close_dict: dict, total_shares_data: dict):
        """计算起始市值和当前市值（后复权，使用年报次年5月1日的价格）"""
        start_market_cap = None
        current_market_cap = None
        start_fq_price = None
        current_fq_price = None
        start_fq_date = None
        current_fq_date = None
        
        print(f'  [buffett] _calculate_market_cap: start_year={start_year}, end_year={end_year}, fq_close_dict size={len(fq_close_dict)}')
        
        if start_year and end_year and fq_close_dict and total_shares_data:
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
                
                shares_start = total_shares_data.get(f"{start_year}-12-31", 0)
                if shares_start == 0 and len(total_shares_data) > 0:
                    shares_start = list(total_shares_data.values())[0]
                
                start_fq_price = start_price
                start_fq_date = str(start_price_date) if start_price_date else None
                print(f'  [buffett] 起始市值计算: start_date={start_date_str}, actual_date={start_fq_date}, start_price={start_price}, shares_start={shares_start}')
                
                if start_price and start_price > 0 and shares_start > 0:
                    start_market_cap = start_price * shares_start / 100000000
                    print(f'  [buffett] 起始市值计算成功: {start_market_cap} 亿')
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
                
                shares_current = total_shares_data.get(f"{end_year}-12-31", 0)
                if shares_current == 0 and len(total_shares_data) > 0:
                    shares_current = list(total_shares_data.values())[-1]
                
                current_fq_price = current_price
                current_fq_date = str(current_price_date) if current_price_date else None
                print(f'  [buffett] 当前市值计算: today={end_date_str}, actual_date={current_fq_date}, current_price={current_price}, shares_current={shares_current}')
                
                if current_price and current_price > 0 and shares_current > 0:
                    current_market_cap = current_price * shares_current / 100000000
                    print(f'  [buffett] 当前市值计算成功: {current_market_cap} 亿')
            except Exception as e:
                print(f'  [buffett] 计算当前市值失败: {e}')
        
        return start_market_cap, current_market_cap, start_fq_price, current_fq_price, start_fq_date, current_fq_date

    def _calculate_growth_rates(self, start_market_cap: float, current_market_cap: float, 
                              total_retained_earnings: float):
        """计算市值增长和留存比值"""
        market_cap_growth = None
        retained_growth_rate = None
        
        if start_market_cap is not None and current_market_cap is not None:
            market_cap_growth = current_market_cap - start_market_cap
        
        if market_cap_growth is not None and total_retained_earnings is not None and total_retained_earnings != 0:
            retained_growth_rate = (market_cap_growth / total_retained_earnings) * 100
        
        return market_cap_growth, retained_growth_rate

    def _calculate_yearly_market_cap(self, annual_periods: list, price_dict: dict, total_shares_data: dict):
        """计算每年的市值：使用年报次年5月1日（或最近交易日）的后复权收盘价 × 当年总股本"""
        yearly_market_cap = {}
        yearly_fq_price = {}
        
        print(f'  [buffett] _calculate_yearly_market_cap: price_dict size={len(price_dict)}, total_shares_data size={len(total_shares_data)}')
        
        if price_dict and total_shares_data:
            for period in annual_periods:
                year = period[:4]
                try:
                    next_year = int(year) + 1
                    target_date = pd.Timestamp(f"{next_year}-05-01")
                    sorted_dates = sorted(price_dict.keys())
                    price = None
                    for d in reversed(sorted_dates):
                        if d <= target_date:
                            price = price_dict[d]
                            break
                    
                    shares = total_shares_data.get(period, 0)
                    if shares == 0 and len(total_shares_data) > 0:
                        shares = list(total_shares_data.values())[0]
                    
                    print(f'  [buffett] 年度市值计算 {period}: price={price}, shares={shares}')
                    
                    if price and price > 0 and shares > 0 and shares < 1e18:
                        market_cap = price * shares / 100000000
                        if market_cap > 0 and market_cap < 1e8:
                            yearly_market_cap[period] = market_cap
                            yearly_fq_price[period] = price
                            print(f'  [buffett] 年度市值计算成功 {period}: price={price}, shares={shares}, market_cap={market_cap} 亿')
                        else:
                            print(f'  [buffett] 市值计算异常 {period}: price={price}, shares={shares}, market_cap={market_cap}')
                    else:
                        print(f'  [buffett] 市值计算跳过 {period}: price={price}, shares={shares}')
                except Exception as e:
                    print(f'  [buffett] 计算年度市值 {period} 失败: {e}')
        
        print(f'  [buffett] yearly_market_cap 结果: {list(yearly_market_cap.keys())}')
        return yearly_market_cap, yearly_fq_price

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
