""".env 读取工具。"""
import os


def _iter_env_lines(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                yield key.strip(), value.strip().strip("\"'")
    except OSError:
        return


def load_local_env(path):
    """把 .env 中的配置写入 os.environ，已存在的环境变量优先。"""
    for key, value in _iter_env_lines(os.fspath(path)):
        if key:
            os.environ.setdefault(key, value)


def env_keys(path):
    """返回 .env 文件中定义的配置项名称。"""
    return {key for key, _ in _iter_env_lines(os.fspath(path)) if key}
