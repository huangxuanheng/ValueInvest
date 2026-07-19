def fetch_sec_name(code):
    import akshare as ak
    try:
        df = ak.stock_info_a_code_name()
        row = df[df['code'] == code]
        if len(row) > 0:
            return row.iloc[0]['name'], ''
    except Exception:
        pass
    
    try:
        df = ak.stock_zh_a_spot_em()
        row = df[df['代码'] == code]
        if len(row) > 0:
            return row.iloc[0]['名称'], ''
    except Exception:
        pass
    
    return None, ''