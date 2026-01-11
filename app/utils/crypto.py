from __future__ import annotations

import hashlib
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify_ed25519_signature(public_key_b64: str, message: bytes, signature_b64: str) -> bool:
    import base64

    try:
        pk = base64.b64decode(public_key_b64)
        sig = base64.b64decode(signature_b64)
        key = Ed25519PublicKey.from_public_bytes(pk)
        key.verify(sig, message)
        return True
    except (ValueError, InvalidSignature):
        return False
