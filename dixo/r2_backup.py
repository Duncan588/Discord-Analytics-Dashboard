"""Cloudflare R2 / restic backup status for the Admin panel (level-1 only).

Reads /root/.config/r2-backup.env and /var/log/db-backup.log; never exposes
RESTIC_PASSWORD or AWS_SECRET_ACCESS_KEY in return values.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

ENV_PATH = Path("/root/.config/r2-backup.env")
PASS_PATH = Path("/root/.config/restic-r2.pass")
LOG_PATH = Path("/var/log/db-backup.log")
BUCKET_DEFAULT = "discord-dashboard-backup"
# R2 free tier storage allowance (bytes); soft quota when CF billing API unavailable.
R2_FREE_STORAGE_BYTES = 10 * 1024 * 1024 * 1024

_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
_CACHE_TTL = 30.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return s


def _mask_key(key: str | None) -> str | None:
    if not key:
        return None
    key = key.strip()
    if len(key) <= 8:
        return key[:1] + "…" + key[-1:]
    return f"{key[:4]}…{key[-4:]}"


def _human_bytes(n: int | float | None) -> str | None:
    if n is None:
        return None
    n = float(n)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    if i == 0:
        return f"{int(n)} {units[i]}"
    return f"{n:.2f} {units[i]}"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if not ENV_PATH.is_file():
        return env
    for line in ENV_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip("\"'")
    return env


def account_id_from_repo(repo: str | None) -> str | None:
    if not repo:
        return None
    m = re.search(r"https?://([a-f0-9]{32})\.r2\.cloudflarestorage\.com", repo)
    return m.group(1) if m else None


def bucket_from_repo(repo: str | None) -> str | None:
    if not repo:
        return None
    m = re.search(r"r2\.cloudflarestorage\.com/([^/\s]+)", repo)
    return m.group(1) if m else None


def _run(cmd: list[str], env: dict[str, str] | None = None, timeout: int = 60) -> tuple[int, str, str]:
    full = os.environ.copy()
    full["HOME"] = "/root"
    full["XDG_CACHE_HOME"] = "/root/.cache"
    if env:
        full.update(env)
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=full,
            check=False,
        )
        return p.returncode, p.stdout or "", p.stderr or ""
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"
    except FileNotFoundError as e:
        return 127, "", str(e)


def parse_backup_log() -> dict[str, Any]:
    out: dict[str, Any] = {
        "last_start": None,
        "last_ok": None,
        "last_ok_duration_s": None,
        "last_fail": None,
        "last_fail_detail": None,
        "recent_lines": [],
    }
    if not LOG_PATH.is_file():
        return out
    try:
        lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return out
    out["recent_lines"] = lines[-30:]
    start_re = re.compile(r"^(\S+)\s+start\s*$")
    ok_re = re.compile(r"^(\S+)\s+ok in (\d+)s\s*$")
    fail_re = re.compile(r"^(\S+)\s+FAILED\b(.*)$")
    for line in lines:
        m = start_re.match(line)
        if m:
            out["last_start"] = _parse_iso(m.group(1))
            continue
        m = ok_re.match(line)
        if m:
            out["last_ok"] = _parse_iso(m.group(1))
            out["last_ok_duration_s"] = int(m.group(2))
            continue
        m = fail_re.match(line)
        if m:
            out["last_fail"] = _parse_iso(m.group(1))
            out["last_fail_detail"] = (m.group(2) or "").strip()[:300]
    return out


def systemd_timer_status() -> dict[str, Any]:
    info: dict[str, Any] = {
        "timer_active": None,
        "next_run": None,
        "last_trigger": None,
        "service_active": None,
        "service_result": None,
        "service_exec_main_status": None,
    }
    rc, out, _ = _run(
        [
            "systemctl",
            "show",
            "db-backup.timer",
            "--property=ActiveState,NextElapseUSecRealtime,LastTriggerUSec",
            "--no-pager",
        ],
        timeout=10,
    )
    if rc == 0:
        props = {}
        for line in out.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                props[k] = v
        info["timer_active"] = props.get("ActiveState")
        nxt = props.get("NextElapseUSecRealtime") or ""
        last = props.get("LastTriggerUSec") or ""
        if nxt and nxt not in ("n/a", "0", ""):
            info["next_run"] = nxt
        if last and last not in ("n/a", "0", ""):
            info["last_trigger"] = last

    rc, out, _ = _run(
        [
            "systemctl",
            "show",
            "db-backup.service",
            "--property=ActiveState,Result,ExecMainStatus,ExecMainStartTimestamp,ExecMainExitTimestamp",
            "--no-pager",
        ],
        timeout=10,
    )
    if rc == 0:
        props = {}
        for line in out.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                props[k] = v
        info["service_active"] = props.get("ActiveState")
        info["service_result"] = props.get("Result")
        info["service_exec_main_status"] = props.get("ExecMainStatus")
        info["service_last_start"] = props.get("ExecMainStartTimestamp") or None
        info["service_last_exit"] = props.get("ExecMainExitTimestamp") or None
    return info


def restic_snapshots(env: dict[str, str], limit: int = 12) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "error": None, "snapshots": [], "count": 0}
    if not env.get("RESTIC_REPOSITORY"):
        result["error"] = "RESTIC_REPOSITORY 未配置"
        return result
    rc, out, err = _run(
        ["restic", "snapshots", "--host", "prod-v20", "--json"],
        env=env,
        timeout=90,
    )
    if rc != 0:
        result["error"] = (err or out or f"restic exit {rc}")[:400]
        return result
    try:
        snaps = json.loads(out) if out.strip() else []
    except json.JSONDecodeError:
        result["error"] = "无法解析 restic snapshots JSON"
        return result
    if not isinstance(snaps, list):
        snaps = []
    snaps.sort(key=lambda s: s.get("time") or "", reverse=True)
    result["count"] = len(snaps)
    slim = []
    for s in snaps[:limit]:
        slim.append(
            {
                "id": (s.get("short_id") or (s.get("id") or "")[:8]),
                "time": _parse_iso(s.get("time")),
                "tags": s.get("tags") or [],
                "paths": s.get("paths") or [],
                "hostname": s.get("hostname"),
            }
        )
    result["snapshots"] = slim
    result["ok"] = True
    return result


def restic_stats(env: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "error": None}
    rc, out, err = _run(
        ["restic", "stats", "--mode", "raw-data", "--json"],
        env=env,
        timeout=120,
    )
    if rc != 0:
        rc2, out2, err2 = _run(["restic", "stats", "--mode", "raw-data"], env=env, timeout=120)
        if rc2 != 0:
            result["error"] = (err or err2 or out or out2 or f"exit {rc}")[:400]
            return result
        m = re.search(r"Total Size:\s+([0-9.]+)\s+(\w+)", out2)
        result["ok"] = True
        result["total_size_human"] = f"{m.group(1)} {m.group(2)}" if m else None
        result["raw_text"] = True
        return result
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        result["error"] = "无法解析 restic stats JSON"
        return result
    total = data.get("total_size")
    result.update(
        {
            "ok": True,
            "total_size_bytes": total,
            "total_size_human": _human_bytes(total),
            "total_file_count": data.get("total_file_count"),
            "snapshots_count": data.get("snapshots_count") or data.get("total_blob_count"),
        }
    )
    return result


def s3_bucket_usage(env: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "error": None}
    ak = env.get("AWS_ACCESS_KEY_ID")
    sk = env.get("AWS_SECRET_ACCESS_KEY")
    repo = env.get("RESTIC_REPOSITORY") or ""
    if not ak or not sk or not repo:
        result["error"] = "R2 S3 凭证或仓库未配置"
        return result
    account = account_id_from_repo(repo)
    bucket = bucket_from_repo(repo) or BUCKET_DEFAULT
    if not account:
        result["error"] = "无法从 RESTIC_REPOSITORY 解析 account id"
        return result
    endpoint = f"https://{account}.r2.cloudflarestorage.com"
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        result["error"] = "boto3 未安装"
        return result
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name="auto",
            config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
        )
        total = 0
        count = 0
        token = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents") or []:
                total += int(obj.get("Size") or 0)
                count += 1
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        result.update(
            {
                "ok": True,
                "bucket": bucket,
                "object_count": count,
                "used_bytes": total,
                "used_human": _human_bytes(total),
            }
        )
        return result
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"[:400]
        return result


def cf_r2_metrics(env: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "error": None, "available": False}
    token = env.get("CF_API_TOKEN") or env.get("CLOUDFLARE_API_TOKEN")
    account = env.get("CF_ACCOUNT_ID") or account_id_from_repo(env.get("RESTIC_REPOSITORY"))
    if not token:
        result["error"] = "未配置 CF_API_TOKEN（可选；配置后可显示账户级用量）"
        return result
    if not account:
        result["error"] = "缺少 CF_ACCOUNT_ID"
        return result
    result["available"] = True
    url = f"https://api.cloudflare.com/client/v4/accounts/{account}/r2/metrics"
    try:
        req = Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "dixo-admin-r2/1",
            },
            method="GET",
        )
        with urlopen(req, timeout=20) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"[:400]
        return result
    if not body.get("success"):
        result["error"] = str(body.get("errors") or "CF API failed")[:400]
        return result
    std = ((body.get("result") or {}).get("standard") or {}).get("published") or {}
    used = int(std.get("payloadSize") or 0) + int(std.get("metadataSize") or 0)
    result.update(
        {
            "ok": True,
            "used_bytes": used,
            "used_human": _human_bytes(used),
            "object_count": int(std.get("objects") or 0),
            "scope": "account_all_buckets",
        }
    )
    return result


def credentials_public_view(env: dict[str, str]) -> dict[str, Any]:
    ak = env.get("AWS_ACCESS_KEY_ID")
    sk = env.get("AWS_SECRET_ACCESS_KEY")
    repo = env.get("RESTIC_REPOSITORY")
    pass_file = env.get("RESTIC_PASSWORD_FILE") or str(PASS_PATH)
    pass_ok = Path(pass_file).is_file() and Path(pass_file).stat().st_size > 0
    return {
        "env_file": str(ENV_PATH),
        "env_file_exists": ENV_PATH.is_file(),
        "access_key_id_masked": _mask_key(ak),
        "access_key_configured": bool(ak),
        "secret_configured": bool(sk),
        "restic_password_configured": pass_ok,
        "restic_password_file": pass_file if pass_ok else None,
        "repository": repo,
        "bucket": bucket_from_repo(repo) or BUCKET_DEFAULT,
        "account_id": account_id_from_repo(repo),
        "cf_api_token_configured": bool(env.get("CF_API_TOKEN") or env.get("CLOUDFLARE_API_TOKEN")),
    }


def overall_status(force: bool = False) -> dict[str, Any]:
    now = time.time()
    if not force and _CACHE["data"] is not None and (now - _CACHE["ts"]) < _CACHE_TTL:
        cached = dict(_CACHE["data"])
        cached["cached"] = True
        return cached

    env = load_env()
    log = parse_backup_log()
    timer = systemd_timer_status()
    creds = credentials_public_view(env)
    if creds["access_key_configured"] and creds["secret_configured"]:
        snaps = restic_snapshots(env)
    else:
        snaps = {"ok": False, "error": "凭证未配置", "snapshots": [], "count": 0}
    stats = restic_stats(env) if snaps.get("ok") else {"ok": False, "error": snaps.get("error")}
    s3 = s3_bucket_usage(env) if creds["secret_configured"] else {"ok": False, "error": "secret 未配置"}
    cf = cf_r2_metrics(env)

    storage: dict[str, Any] = {
        "used_bytes": None,
        "used_human": None,
        "quota_bytes": R2_FREE_STORAGE_BYTES,
        "quota_human": _human_bytes(R2_FREE_STORAGE_BYTES),
        "quota_label": "R2 免费额度参考（10 GiB 存储）",
        "remaining_bytes": None,
        "remaining_human": None,
        "source": None,
        "object_count": None,
        "note": None,
        "used_pct": None,
    }
    if cf.get("ok"):
        storage["used_bytes"] = cf["used_bytes"]
        storage["used_human"] = cf["used_human"]
        storage["object_count"] = cf.get("object_count")
        storage["source"] = "cloudflare_r2_metrics"
        storage["note"] = "账户级所有 R2 桶合计（Cloudflare API）"
    elif s3.get("ok"):
        storage["used_bytes"] = s3["used_bytes"]
        storage["used_human"] = s3["used_human"]
        storage["object_count"] = s3.get("object_count")
        storage["source"] = "s3_list_objects"
        storage["note"] = f"仅桶 {s3.get('bucket')} 的对象合计（S3 API）；非账户配额官方值"
    elif stats.get("ok") and stats.get("total_size_bytes") is not None:
        storage["used_bytes"] = stats["total_size_bytes"]
        storage["used_human"] = stats.get("total_size_human")
        storage["source"] = "restic_stats"
        storage["note"] = "restic 仓库压缩后体积"
    elif stats.get("ok") and stats.get("total_size_human"):
        storage["used_human"] = stats["total_size_human"]
        storage["source"] = "restic_stats_text"
        storage["note"] = "restic 仓库体积（文本解析）"

    if storage["used_bytes"] is not None:
        rem = max(0, R2_FREE_STORAGE_BYTES - int(storage["used_bytes"]))
        storage["remaining_bytes"] = rem
        storage["remaining_human"] = _human_bytes(rem)
        storage["used_pct"] = round(100.0 * int(storage["used_bytes"]) / R2_FREE_STORAGE_BYTES, 2)

    running = timer.get("service_active") in ("activating", "active")
    health = "unknown"
    if running:
        health = "running"
    elif log.get("last_fail") and (not log.get("last_ok") or str(log["last_fail"]) > str(log["last_ok"])):
        health = "failed"
    elif log.get("last_ok"):
        health = "ok"
    elif not creds["env_file_exists"]:
        health = "unconfigured"

    data = {
        "cached": False,
        "generated_at": _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "health": health,
        "credentials": creds,
        "log": {
            "path": str(LOG_PATH),
            "last_start": log.get("last_start"),
            "last_ok": log.get("last_ok"),
            "last_ok_duration_s": log.get("last_ok_duration_s"),
            "last_fail": log.get("last_fail"),
            "last_fail_detail": log.get("last_fail_detail"),
            "recent_lines": log.get("recent_lines") or [],
        },
        "schedule": {
            "timer": "db-backup.timer",
            "service": "db-backup.service",
            "calendar": "每天 04:17 UTC（北京时间 12:17）",
            "prune_policy": "keep-daily 7, keep-weekly 4, keep-monthly 3 + prune",
            "timer_active": timer.get("timer_active"),
            "next_run": timer.get("next_run"),
            "last_trigger": timer.get("last_trigger"),
            "service_active": timer.get("service_active"),
            "service_result": timer.get("service_result"),
            "service_last_start": timer.get("service_last_start"),
            "service_last_exit": timer.get("service_last_exit"),
            "running": running,
        },
        "snapshots": snaps,
        "restic_stats": {k: v for k, v in stats.items()},
        "storage": storage,
        "cf_metrics": {
            "ok": bool(cf.get("ok")),
            "configured": bool(cf.get("available")),
            "error": None if cf.get("ok") else cf.get("error"),
        },
        "s3_usage": {
            "ok": bool(s3.get("ok")),
            "error": None if s3.get("ok") else s3.get("error"),
            "bucket": s3.get("bucket"),
            "used_human": s3.get("used_human"),
            "object_count": s3.get("object_count"),
        },
    }
    _CACHE["ts"] = now
    _CACHE["data"] = data
    return data


def invalidate_cache() -> None:
    _CACHE["ts"] = 0.0
    _CACHE["data"] = None


def test_credentials() -> dict[str, Any]:
    env = load_env()
    if not env.get("AWS_ACCESS_KEY_ID") or not env.get("AWS_SECRET_ACCESS_KEY"):
        return {"ok": False, "error": "R2 凭证未配置"}
    rc, out, err = _run(
        ["restic", "snapshots", "--latest", "1", "--json"],
        env=env,
        timeout=60,
    )
    if rc != 0:
        return {"ok": False, "error": (err or out or f"exit {rc}")[:400]}
    try:
        snaps = json.loads(out) if out.strip() else []
    except json.JSONDecodeError:
        snaps = []
    latest = snaps[0] if isinstance(snaps, list) and snaps else None
    return {
        "ok": True,
        "latest": {
            "id": (latest or {}).get("short_id"),
            "time": _parse_iso((latest or {}).get("time")),
            "tags": (latest or {}).get("tags"),
        }
        if latest
        else None,
    }


def start_backup() -> dict[str, Any]:
    st = systemd_timer_status()
    if st.get("service_active") in ("active", "activating"):
        return {"ok": False, "error": "备份已在运行中", "running": True}
    rc, out, err = _run(["systemctl", "start", "db-backup.service"], timeout=15)
    if rc != 0:
        return {"ok": False, "error": (err or out or f"systemctl exit {rc}")[:400]}
    invalidate_cache()
    return {"ok": True, "message": "已触发 db-backup.service"}


def save_credentials(
    access_key_id: str,
    secret_access_key: str,
    restic_password: str | None = None,
    cf_api_token: str | None = None,
) -> dict[str, Any]:
    access_key_id = (access_key_id or "").strip()
    secret_access_key = (secret_access_key or "").strip()
    if not access_key_id or not secret_access_key:
        return {"ok": False, "error": "Access Key ID 与 Secret 均必填"}
    existing = load_env()
    repo = existing.get("RESTIC_REPOSITORY") or os.getenv("RESTIC_REPOSITORY") or (
        "s3:https://<CF_ACCOUNT_ID>.r2.cloudflarestorage.com/<bucket>"
    )
    pass_file = existing.get("RESTIC_PASSWORD_FILE") or str(PASS_PATH)
    lines = [
        f"AWS_ACCESS_KEY_ID={access_key_id}",
        f"AWS_SECRET_ACCESS_KEY={secret_access_key}",
        f"RESTIC_REPOSITORY={repo}",
        f"RESTIC_PASSWORD_FILE={pass_file}",
    ]
    token = (cf_api_token or "").strip()
    if token:
        lines.append(f"CF_API_TOKEN={token}")
    elif existing.get("CF_API_TOKEN"):
        lines.append(f"CF_API_TOKEN={existing['CF_API_TOKEN']}")
    elif existing.get("CLOUDFLARE_API_TOKEN"):
        lines.append(f"CLOUDFLARE_API_TOKEN={existing['CLOUDFLARE_API_TOKEN']}")
    ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(ENV_PATH, 0o600)
    if restic_password is not None and restic_password.strip():
        p = Path(pass_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(restic_password.strip() + "\n", encoding="utf-8")
        os.chmod(p, 0o600)
    invalidate_cache()
    return {"ok": True, "message": "凭证已保存", "access_key_id_masked": _mask_key(access_key_id)}
