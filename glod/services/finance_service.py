import pandas as pd

def _parse_financial_value(val):
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

def fetch_financial_data(code_list, years=5):
    import akshare as ak
    from ..utils.stock_info import fetch_sec_name
    
    query_years = years + 1
    
    result = {"stocks": [], "error": None}
    
    for code in code_list:
        try:
            sec_name, suffix = fetch_sec_name(code)
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
                        '期末现金及现金等价物余额': '六、期末现金及现金等价物余额',
                        '固定资产折旧、油气资产折耗、生产性生物资产折旧': '固定资产折旧、油气资产折耗、生产性生物资产折旧',
                        '无形资产摊销': '无形资产摊销',
                        '长期待摊费用摊销': '长期待摊费用摊销'
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
            
            try:
                df_sina_cash = ak.stock_financial_report_sina(symbol=code)
                if df_sina_cash is not None and len(df_sina_cash) > 0:
                    sina_cash_field_mapping = {
                        '固定资产折旧、油气资产折耗、生产性生物资产折旧': ['固定资产折旧、油气资产折耗、生产性生物资产折旧', '固定资产折旧'],
                        '无形资产摊销': ['无形资产摊销'],
                        '长期待摊费用摊销': ['长期待摊费用摊销']
                    }
                    
                    for item, field_list in sina_cash_field_mapping.items():
                        if item not in cashflow_data or all(v is None for v in cashflow_data[item].values()):
                            values = {}
                            for period in annual_periods:
                                val = None
                                for field in field_list:
                                    row = df_sina_cash[df_sina_cash['报告期'] == period]
                                    if len(row) > 0:
                                        val = row.iloc[0].get(field)
                                        if val is not None and not pd.isna(val):
                                            break
                                values[period] = _parse_financial_value(val)
                            cashflow_data[item] = values
                print(f'  [finance] stock_financial_report_sina({code}) 补充现金流量表数据成功')
            except Exception as e:
                print(f'  [finance] stock_financial_report_sina({code}) 失败: {e}')
            
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
                cashflow_val = cashflow_data.get('经营活动产生的现金流量净额', {}).get(period)
                if cashflow_val is not None and cashflow_val != 0:
                    dividend_data[period] = (dividend / cashflow_val * 100) if dividend else 0
                else:
                    dividend_data[period] = None
            growth_data['分红率'] = dividend_data
            
            display_periods = annual_periods[:years]
            
            result["stocks"].append({
                "code": code,
                "name": sec_name,
                "periods": display_periods,
                "balance_sheet": balance_data,
                "income_statement": income_data,
                "cash_flow": cashflow_data,
                "growth_data": growth_data
            })
        except Exception as e:
            print(f'  [finance] fetch_financial_data({code}) 失败: {e}')
            result["stocks"].append({
                "code": code,
                "name": code,
                "periods": [],
                "balance_sheet": {},
                "income_statement": {},
                "cash_flow": {},
                "growth_data": {},
                "error": str(e)
            })
    
    return result