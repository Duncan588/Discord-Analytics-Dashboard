"""Environment-file helpers shared by the web app and background services."""

import os
from pathlib import Path


def load_local_env(path):
    """Load simple KEY=VALUE pairs without overriding process environment."""
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
                if key and key not in os.environ:
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

