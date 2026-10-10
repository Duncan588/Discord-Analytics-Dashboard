"""Every URL rule + method set that existed before the Blueprint split must still exist
(baseline captured from the monolithic app.py at commit b5af860)."""
import json
import os

BASELINE = os.path.join(os.path.dirname(__file__), "route_map_baseline.json")


def test_all_old_rules_still_registered(client):
    app = client.dashboard.app
    now = {(r.rule, tuple(sorted(m for m in r.methods if m not in ("HEAD", "OPTIONS")))) for r in app.url_map.iter_rules()}
    old = json.load(open(BASELINE, encoding="utf-8"))
    missing = [(rule, methods) for rule, methods, _ in old if (rule, tuple(methods)) not in now]
    assert not missing, missing


def test_old_urls_resolve_to_same_function_names(client):
    """Endpoint names gained a blueprint prefix; the view function name is unchanged."""
    app = client.dashboard.app
    adapter = app.url_map.bind("localhost")
    for rule, methods, endpoint in json.load(open(BASELINE, encoding="utf-8")):
        if "<" in rule:
            continue
        ep, _ = adapter.match(rule, method=methods[0])
        assert ep.split(".")[-1] == endpoint.split(".")[-1], (rule, ep, endpoint)


def test_no_routes_left_in_entrypoint():
    src = open(os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.py"), encoding="utf-8").read()
    assert "@app.route" not in src and len(src.splitlines()) < 40
