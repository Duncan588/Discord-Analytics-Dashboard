"""Dixo package: new, testable modules extracted from / added beside app.py.

app.py remains the process entry point (systemd runs `python app.py`).
Modules here must never `import app` (avoid import cycles); app.py wires them up.
"""
