"""Environment-file helpers shared by the web app and background services."""

import os
from pathlib import Path


def load_local_env(path):
    """Load simple KEY=VALUE pairs without overriding process environment."""
    apply_local_env(path, override=False)


def apply_local_env(path, override=False):
    """Load simple KEY=VALUE pairs into os.environ.

    override=False（默认）：只填充缺失的 key，进程已有值优先。
    override=True：文件中的值无条件覆盖进程环境 —— 用于 /restart 完整重启
    前刷新配置，保证 systemd EnvironmentFile 注入的旧值不会挡住用户刚改的
    .env 新值。
    """
    path = Path(path)
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8") as stream:
            for raw in stream:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                if not key:
                    continue
                if not override and key in os.environ:
                    continue
                os.environ[key] = value
    except OSError:
        # Callers that need to report this condition can do so around this
        # helper; missing .env is valid for some standalone tools.
        return


def env_keys(path):
    """Return variable names declared in a dotenv-style file."""
    path = Path(path)
    keys = set()
    try:
        with path.open("r", encoding="utf-8") as stream:
            for raw in stream:
                line = raw.strip()
                if line and not line.startswith("#") and "=" in line:
                    keys.add(line.split("=", 1)[0].strip())
    except OSError:
        pass
    return keys

