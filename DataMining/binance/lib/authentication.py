import hmac
import hashlib


def hmac_hashing(api_secret, payload):
    m = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256)
    return m.hexdigest()
