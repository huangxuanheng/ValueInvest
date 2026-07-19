import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

try:
    from glod.models import Base, engine
    Base.metadata.create_all(bind=engine)
except ImportError as e:
    print(f"[WARN] 无法导入 glod.models: {e}")
    print(f"  项目根目录: {PROJECT_ROOT}")
    print(f"  glod目录是否存在: {os.path.exists(os.path.join(PROJECT_ROOT, 'glod'))}")
    print(f"  glod/models目录是否存在: {os.path.exists(os.path.join(PROJECT_ROOT, 'glod', 'models'))}")
    Base = None
    engine = None

try:
    from glod.routes.auth import auth_bp
except ImportError:
    auth_bp = None
    print("[WARN] 无法导入 glod.routes.auth")

try:
    from glod.routes.finance import finance_bp
except ImportError:
    finance_bp = None
    print("[WARN] 无法导入 glod.routes.finance")

from web_app import app as web_app_instance

if auth_bp:
    web_app_instance.register_blueprint(auth_bp)
if finance_bp:
    web_app_instance.register_blueprint(finance_bp)

app = web_app_instance

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)