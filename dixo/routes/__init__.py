"""Register all legacy-page Blueprints on the core Flask app."""
from dixo.core import app
from dixo.routes import activity, admin, auth, pages, social

for _m in (auth, activity, pages, admin, social):
    app.register_blueprint(_m.bp)
