from flask import Flask

def create_app():
    app = Flask(__name__, template_folder='../templates')
    
    app.config['SECRET_KEY'] = 'your-secret-key-here'
    
    return app