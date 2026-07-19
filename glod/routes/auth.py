from flask import Blueprint, request, jsonify, session, render_template
from datetime import datetime
import hashlib
import base64

from ..models import User, SessionLocal
from ..utils.rsa import rsa_encrypt_password

auth_bp = Blueprint('auth', __name__)

def _current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    with SessionLocal() as s:
        u = s.get(User, uid)
        if u:
            return u
    return None

@auth_bp.route("/login")
def page_login():
    return render_template("login.html")

@auth_bp.route("/register")
def page_register():
    return render_template("register.html")

@auth_bp.route("/api/register", methods=["POST"])
def api_register():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not username or len(username) < 2:
        return jsonify({"ok": False, "msg": "用户名至少 2 个字符"})
    if not email or not (email.endswith("@163.com") or email.endswith("@qq.com")):
        return jsonify({"ok": False, "msg": "邮箱只支持 163 邮箱或 QQ 邮箱"})
    if len(password) < 6:
        return jsonify({"ok": False, "msg": "密码至少 6 个字符"})

    with SessionLocal() as s:
        existing = s.query(User).filter(
            (User.username == username) | (User.email == email)
        ).first()
        if existing:
            if existing.username == username:
                return jsonify({"ok": False, "msg": "用户名已存在"})
            else:
                return jsonify({"ok": False, "msg": "邮箱已被注册"})

        enc = rsa_encrypt_password(password)
        if not enc:
            return jsonify({"ok": False, "msg": "密码加密失败（RSA密钥未就绪）"})
        user = User(username=username, email=email, password_enc=enc,
                    created_at=datetime.now().date())
        s.add(user)
        s.commit()
    return jsonify({"ok": True, "msg": "注册成功"})

@auth_bp.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"ok": False, "msg": "请输入用户名和密码"})

    with SessionLocal() as s:
        user = s.query(User).filter(User.username == username).first()
        if not user:
            return jsonify({"ok": False, "msg": "用户名不存在"})
        
        enc = user.password_enc
        try:
            decoded = base64.b64decode(enc)
            parts = decoded.split(b'$')
            if len(parts) == 4 and parts[1] == b'HL':
                salt = parts[2]
                stored_key = parts[3]
                key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
                if key == stored_key:
                    session["user_id"] = user.id
                    session["username"] = user.username
                    return jsonify({"ok": True, "msg": "登录成功", "username": username})
        except Exception as e:
            print(f"[HASH] 验证失败：{e}")
        
        return jsonify({"ok": False, "msg": "密码错误"})

@auth_bp.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True, "msg": "已退出登录"})

@auth_bp.route("/api/current_user")
def api_current_user():
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "logged_in": False})
    return jsonify({"ok": True, "logged_in": True, "username": u.username, "email": u.email})