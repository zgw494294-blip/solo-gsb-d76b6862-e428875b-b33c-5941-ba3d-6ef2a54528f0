"""检索翻页游标的编解码与签名。

游标对客户端不透明：wire 形态为
``v1.<urlsafe-base64 payload>.<urlsafe-base64 HMAC-SHA256>``，
payload 是一段紧凑 JSON，固定首请求时的查询词与已提交批次范围：

    {"v":1, "q":查询词, "m":快照上界 batches.rowid,
     "b":上一页末行的 batches.rowid, "s":上一页末行的 seq}

签名密钥是首次需要时在数据目录生成的随机文件（``search_cursor.key``，
权限 0600），不随游标泄露；服务重启后密钥仍在，故旧游标可续用。密钥丢失
或内容被改时旧游标失效（统一表现为 422）。

任何损坏、字段缺失/多余/类型不对、签名不匹配都抛 CursorError，由路由
映射为 HTTP 422。
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import threading
from typing import Any

from . import config

CURSOR_VERSION = 1
# 查询词最长 200 字符，整体游标仍有明确上界，超长输入直接按非法处理
MAX_CURSOR_CHARS = 2000
_KEY_FILE_NAME = "search_cursor.key"
_KEY_BYTES = 32

_lock = threading.Lock()
_cached_key: bytes | None = None


class CursorError(ValueError):
    """游标损坏、签名不匹配或字段不合法。"""


def _key_path() -> str:
    directory = os.path.dirname(config.DB_PATH) or "."
    return os.path.join(directory, _KEY_FILE_NAME)


def _write_new_key(path: str) -> bytes:
    """以 0600 权限原子创建随机密钥；文件已存在（并发创建）则改用现有文件。"""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    key = os.urandom(_KEY_BYTES)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, key)
    finally:
        os.close(fd)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return key


def _load_key() -> bytes:
    global _cached_key
    if _cached_key is not None:
        return _cached_key
    with _lock:
        if _cached_key is not None:
            return _cached_key
        path = _key_path()
        try:
            with open(path, "rb") as handle:
                key = handle.read()
            if len(key) != _KEY_BYTES:
                # 密钥文件被破坏：用明确名称换一个新密钥，旧游标随之失效
                path_bak = path + f".bad-{os.getpid()}"
                try:
                    os.replace(path, path_bak)
                except OSError:
                    pass
                raise CursorError("invalid signing key")
        except FileNotFoundError:
            try:
                key = _write_new_key(path)
            except FileExistsError:
                with open(path, "rb") as handle:
                    key = handle.read()
                if len(key) != _KEY_BYTES:
                    raise CursorError("invalid signing key")
        _cached_key = key
        return key


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    # validate=True：任何非字母表字符（含内嵌空白）都拒绝，而非静默丢弃
    try:
        padding = "=" * (-len(text) % 4)
        return base64.urlsafe_b64decode(text + padding)
    except (binascii.Error, ValueError) as exc:
        raise CursorError("invalid base64 in cursor") from exc


def _sign(payload_b64: str, key: bytes) -> str:
    digest = hmac.new(key, payload_b64.encode("ascii"), hashlib.sha256).digest()
    return _b64encode(digest)


def encode_cursor(
    query: str, max_rowid: int, last_batch_rowid: int, last_seq: int
) -> str:
    """生成带签名的不透明游标字符串。"""
    payload = json.dumps(
        {
            "v": CURSOR_VERSION,
            "q": query,
            "m": max_rowid,
            "b": last_batch_rowid,
            "s": last_seq,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    payload_b64 = _b64encode(payload)
    key = _load_key()
    return f"v{CURSOR_VERSION}.{payload_b64}.{_sign(payload_b64, key)}"


def decode_cursor(token: str) -> dict[str, Any]:
    """校验并解析游标，返回 {q, m, b, s}；任何非法情形抛 CursorError。"""
    if not isinstance(token, str) or not (1 <= len(token) <= MAX_CURSOR_CHARS):
        raise CursorError("cursor length out of range")
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != f"v{CURSOR_VERSION}":
        raise CursorError("malformed cursor")
    _, payload_b64, sig = parts

    key = _load_key()
    expected = _sign(payload_b64, key)
    if not hmac.compare_digest(sig, expected):
        raise CursorError("cursor signature mismatch")

    raw = _b64decode(payload_b64)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CursorError("cursor payload is not valid JSON") from exc

    if not isinstance(payload, dict) or set(payload.keys()) != {"v", "q", "m", "b", "s"}:
        raise CursorError("cursor fields missing or unexpected")
    if payload["v"] != CURSOR_VERSION:
        raise CursorError("unsupported cursor version")

    query = payload["q"]
    if not isinstance(query, str) or not (1 <= len(query) <= 200):
        raise CursorError("cursor query length out of range")

    for name in ("m", "b", "s"):
        value = payload[name]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise CursorError(f"cursor field {name} must be a non-negative integer")
    max_rowid = payload["m"]
    last_batch_rowid = payload["b"]
    last_seq = payload["s"]
    # 位置点必须落在快照范围内（m==0 时只有 b==0/s==0 的初始态合法）
    if last_batch_rowid > max_rowid or not (
        (last_batch_rowid == 0 and last_seq == 0) or last_batch_rowid > 0
    ):
        raise CursorError("cursor position outside snapshot")

    return {
        "q": query,
        "m": max_rowid,
        "b": last_batch_rowid,
        "s": last_seq,
    }
