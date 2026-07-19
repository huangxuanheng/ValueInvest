import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from glod.models import Base, engine
from glod.routes.auth import auth_bp
from glod.routes.finance import finance_bp

Base.metadata.create_all(bind=engine)

from web_app import app as web_app_instance

web_app_instance.register_blueprint(auth_bp)
web_app_instance.register_blueprint(finance_bp)

app = web_app_instance

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)