from flask import Blueprint, request, jsonify, render_template

from ..services.finance_service import fetch_financial_data

finance_bp = Blueprint('finance', __name__)

@finance_bp.route("/chart/finance_input")
def finance_input():
    return render_template("finance_input.html")

@finance_bp.route("/api/financial_data", methods=["GET"])
def api_financial_data():
    codes = request.args.get('codes', '')
    years = int(request.args.get('years', 5))
    
    code_list = [c.strip() for c in codes.split(',') if c.strip()]
    
    if not code_list:
        return jsonify({"error": "请输入股票代码"}), 400
    
    if len(code_list) > 6:
        return jsonify({"error": "最多支持6只股票"}), 400
    
    result = fetch_financial_data(code_list, years)
    return jsonify(result)