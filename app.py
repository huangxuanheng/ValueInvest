import sys
import os
import time

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

print(f"[{time.strftime('%H:%M:%S')}] 启动 app.py，项目根目录: {PROJECT_ROOT}")

try:
    print(f"[{time.strftime('%H:%M:%S')}] 导入 glod.models...")
    from glod.models import Base, engine
    try:
        Base.metadata.create_all(bind=engine)
        print(f"[{time.strftime('%H:%M:%S')}] glod.models 导入成功")
    except Exception as db_err:
        print(f"[WARN] 数据库建表失败: {db_err}")
        print("  部分功能可能无法使用，但服务仍会启动")
except ImportError as e:
    print(f"[WARN] 无法导入 glod.models: {e}")
    Base = None
    engine = None

try:
    print(f"[{time.strftime('%H:%M:%S')}] 导入 glod.routes.auth...")
    from glod.routes.auth import auth_bp
    print(f"[{time.strftime('%H:%M:%S')}] glod.routes.auth 导入成功")
except ImportError:
    auth_bp = None
    print("[WARN] 无法导入 glod.routes.auth")

try:
    print(f"[{time.strftime('%H:%M:%S')}] 导入 glod.routes.finance...")
    from glod.routes.finance import finance_bp
    print(f"[{time.strftime('%H:%M:%S')}] glod.routes.finance 导入成功")
except ImportError:
    finance_bp = None
    print("[WARN] 无法导入 glod.routes.finance")

print(f"[{time.strftime('%H:%M:%S')}] 导入 web_app...")
from web_app import app as web_app_instance, start_scheduler
print(f"[{time.strftime('%H:%M:%S')}] web_app 导入成功")

if auth_bp:
    web_app_instance.register_blueprint(auth_bp)
if finance_bp:
    web_app_instance.register_blueprint(finance_bp)

app = web_app_instance

if __name__ == "__main__":
    print(f"[{time.strftime('%H:%M:%S')}] 启动调度器...")
    sched = start_scheduler()
    print(f"[{time.strftime('%H:%M:%S')}] 调度器启动成功")
    
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5000"))
    print(f"[{time.strftime('%H:%M:%S')}] 启动 Flask 服务，监听 {host}:{port}")
    
    try:
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    except KeyboardInterrupt:
        print("[主程序] 收到终止信号")
    finally:
        if sched:
            try:
                sched.shutdown(wait=False)
                print("[调度] APScheduler 已停止")
            except Exception:
                pass
