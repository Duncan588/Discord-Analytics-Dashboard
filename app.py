"""Entry point (systemd runs `python app.py`).

All code lives in the dixo package: dixo/core.py (app object, config, helpers, services)
and dixo/routes/*.py (Blueprints). This module re-exports core names so existing
`import app` users (run_local_web.py, tools, tests, generate_fake_data.py) keep working.
"""
from dixo import core as _core
import dixo.routes  # noqa: F401  (registers Blueprints)

globals().update({k: v for k, v in vars(_core).items() if not k.startswith("__")})

if __name__ == "__main__":
    restart_signal = getattr(signal, "SIGUSR1", signal.SIGTERM)
    signal.signal(restart_signal, restart_application)
    start_background_services()
    threading.Thread(target=_service_watchdog, name="service-watchdog", daemon=True).start()
    try:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG", "0") == "1", use_reloader=False)
    finally:
        stop_background_services()
