"""时间处理工具。数据库统一保存 UTC ISO 字符串，展示时转换为北京时间。"""
from datetime import datetime, timedelta, timezone

LOCAL_TZ = timezone(timedelta(hours=8))


def utc_now_iso():
    """数据库写入使用的当前时间（UTC ISO 字符串）。"""
    return datetime.now(timezone.utc).isoformat()


def parse_utc_datetime(value):
    """把数据库/Discord 的时间字符串解析成带时区的 datetime；无法解析时返回 None。"""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            # 兼容 SQLite CURRENT_TIMESTAMP 产生的旧格式
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def to_local_datetime(value):
    """解析时间并转换到本地展示时区；无法解析时返回 None。"""
    parsed = parse_utc_datetime(value)
    return parsed.astimezone(LOCAL_TZ) if parsed else None
