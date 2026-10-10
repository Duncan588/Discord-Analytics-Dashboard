"""asset("css/dixo.css") -> /static/css/dixo.css?v=<content hash>; replaces hand-written ?v= strings."""
import hashlib
import os

_cache = {}


def init_app(app):
    static_dir = app.static_folder

    def asset(rel):
        path = os.path.join(static_dir, rel)
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return f"/static/{rel}"
        hit = _cache.get(rel)
        if not hit or hit[0] != mtime:
            with open(path, "rb") as fh:
                hit = (mtime, hashlib.sha1(fh.read()).hexdigest()[:10])
            _cache[rel] = hit
        return f"/static/{rel}?v={hit[1]}"

    app.jinja_env.globals["asset"] = asset
