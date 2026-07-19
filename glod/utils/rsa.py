import os
import hashlib
import base64

DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data')
_RSA_PRIVATE_KEY_PEM = os.path.join(DATA_DIR, "rsa_private_key.pem")
_RSA_PUBLIC_KEY_PEM = os.path.join(DATA_DIR, "rsa_public_key.pem")

def rsa_encrypt_password(plain: str) -> str | None:
    import os
    salt = os.urandom(16)
    key = hashlib.pbkdf2_hmac('sha256', plain.encode('utf-8'), salt, 100000)
    combined = b'$HL$' + salt + b'$' + key
    return base64.b64encode(combined).decode('ascii')

def rsa_decrypt_password(cipher_b64: str) -> str | None:
    return None