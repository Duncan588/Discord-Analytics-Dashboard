"""Headless click-audit of every interactive control (regression for dead buttons).

Runs only when playwright + chromium are installed:  pytest tests/e2e -q
"""
import os, socket, sqlite3, subprocess, sys, time
import pytest

pw = pytest.importorskip("playwright.sync_api")
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GUILD = "900000000000000001"
ACT = "frame_id=f&instance_id=i&platform=mobile"


def _port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


@pytest.fixture(scope="module")
def server(tmp_path_factory, demo_db):
    port = _port(); ck = tmp_path_factory.mktemp("e2e") / "cookie"
    proc = subprocess.Popen([sys.executable, os.path.join(ROOT, "tests", "e2e", "serve_demo.py"), str(port), str(ck)],
                            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base = f"http://127.0.0.1:{port}"
    for _ in range(240):
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            if ck.exists() and ck.read_text().count("\n"):
                break
        except OSError:
            pass
        time.sleep(0.25)
    name, val = ck.read_text().split("\n", 1)
    yield base, name, val
    proc.terminate(); proc.wait(10)


@pytest.fixture(scope="module")
def browser():
    with pw.sync_playwright() as p:
        try:
            b = p.chromium.launch()
        except Exception as e:  # browsers not installed
            pytest.skip(f"chromium unavailable: {e}")
        yield p, b
        b.close()


def _page(browser, server, mobile=False):
    p, b = browser; base, name, val = server
    ctx = b.new_context(**(p.devices["iPhone 13"] if mobile else {}))
    ctx.add_cookies([{"name": name, "value": val, "url": base}])
    pg = ctx.new_page(); pg.errors = []
    pg.on("pageerror", lambda e: pg.errors.append(str(e)))
    return pg


def _top_user():
    s = sqlite3.connect(os.path.join(ROOT, "data", "servers", GUILD, "summary.db"))
    return s.execute("SELECT user_id FROM user_stats ORDER BY msg_count DESC LIMIT 1").fetchone()[0]


@pytest.mark.parametrize("mobile", [False, True])
def test_home_deep_analysis_controls(browser, server, mobile):
    base = server[0]; pg = _page(browser, server, mobile)
    pg.goto(base + "/" + (("?" + ACT) if mobile else "")); pg.wait_for_selector("#analytics[data-dixo-booted]")
    pg.wait_for_function("!document.getElementById('analytics').classList.contains('is-loading')")
    for pr, frag in [("30", "since="), ("365", "since="), ("all", "analytics?"), ("90", "since=")]:
        with pg.expect_response(lambda r: "/api/v1/analytics" in r.url) as resp:
            pg.locator(f'#analytics [data-preset="{pr}"]').tap() if mobile else pg.click(f'#analytics [data-preset="{pr}"]')
        assert frag in resp.value.url and resp.value.ok
        assert "on" in pg.get_attribute(f'#analytics [data-preset="{pr}"]', "class").split()
    LBL = "Chart.getChart(document.getElementById('anMsgs')).data.labels.length"
    pg.click('#analytics [data-preset="30"]')
    pg.wait_for_function("!document.getElementById('analytics').classList.contains('is-loading')")
    n30 = pg.evaluate(LBL)
    pg.click('#analytics [data-preset="all"]')
    pg.wait_for_function("!document.getElementById('analytics').classList.contains('is-loading')")
    assert pg.evaluate(LBL) > n30, "chart did not repaint for new range"
    # rapid switching: newest range must win (out-of-order guard)
    pg.click('#analytics [data-preset="30"]'); pg.click('#analytics [data-preset="all"]')
    pg.wait_for_function("!document.getElementById('analytics').classList.contains('is-loading')")
    assert "on" in pg.get_attribute('#analytics [data-preset="all"]', "class").split()
    for g in ["week", "month", "year", "day"]:
        pg.click(f'#analytics [data-grain="{g}"]')
        assert "on" in pg.get_attribute(f'#analytics [data-grain="{g}"]', "class").split()
    # custom range + apply
    pg.click("#analytics .seg button:has-text('自定义'), #analytics .seg button:has-text('Custom')")
    pg.fill("#analytics input[type=date] >> nth=0", "2026-08-01")
    with pg.expect_response(lambda r: "/api/v1/analytics" in r.url and "since=2026-08-01" in r.url):
        pg.click("#analytics .btn-primary")
    # tier filter + pager
    for t in ["returned", "faded", "rare", "new", "always_active"]:
        pg.click(f'[data-tag-filter="{t}"]')
        assert "on" in pg.get_attribute(f'[data-tag-filter="{t}"]', "class").split()
    assert not pg.errors, pg.errors


def test_home_trend_and_modals(browser, server):
    base = server[0]; pg = _page(browser, server)
    pg.goto(base + "/"); pg.wait_for_timeout(800)
    for label in ["30", "90"]:
        btn = pg.locator(f".seg button:has-text('{label}')").first
        btn.click(); assert "on" in btn.get_attribute("class")
    pg.locator(".seg button:has-text('24'), .seg button:has-text('按小时'), .seg button:has-text('Hourly')").first.click()
    pg.locator("a:has-text('›')").first.click(); pg.wait_for_timeout(300)
    assert pg.locator(".modal:visible").count() >= 1
    pg.keyboard.press("Escape"); pg.mouse.click(5, 5)
    assert not pg.errors, pg.errors


def test_theme_and_language(browser, server):
    base = server[0]; pg = _page(browser, server)
    pg.goto(base + "/")
    before = pg.evaluate("document.documentElement.dataset.theme || ''")
    pg.click("[data-theme-toggle]")
    assert pg.evaluate("document.documentElement.dataset.theme || ''") != before
    lang = pg.evaluate("document.documentElement.lang")
    pg.click("details.menu summary"); pg.click("details.menu [data-language-toggle]")
    pg.wait_for_function(f"document.documentElement.lang !== '{lang}'")
    assert not pg.errors, pg.errors


@pytest.mark.parametrize("mobile", [False, True])
def test_profile_tabs_and_sort(browser, server, mobile):
    base = server[0]; pg = _page(browser, server, mobile); uid = _top_user()
    pg.goto(f"{base}/user/{uid}" + (("?" + ACT) if mobile else ""))
    for label, want in [("发言", "tab=messages"), ("最热", "sort=hot"), ("发帖", "tab=threads"), ("最新", "sort=time")]:
        link = pg.locator(f"#records .tabs a:has-text('{label}'), #records .seg a:has-text('{label}')").first
        with pg.expect_navigation():
            link.tap() if mobile else link.click()
        assert want in pg.url
        assert label in " ".join(pg.locator("#records a.on").all_inner_texts())
        if mobile:
            assert "instance_id=i" in pg.url  # Activity context kept
    seg = pg.locator(".seg button").nth(1); seg.click(); assert "on" in seg.get_attribute("class")
    assert not pg.errors, pg.errors


def test_servers_report_admin(browser, server):
    base = server[0]; pg = _page(browser, server); uid = _top_user()
    pg.goto(base + "/servers")
    with pg.expect_navigation():
        pg.locator("a.server-card").first.click()
    assert "/servers" not in pg.url  # card tap selected a server and left the picker
    r = pg.goto(f"{base}/report")
    if r and r.ok and pg.locator("main a.btn-primary").count():  # admin may have no messages in demo DB
        with pg.expect_navigation():
            pg.locator("main a.btn-primary").last.click()
        assert "/report" not in pg.url
    pg.goto(base + "/admin")
    msgs = []
    pg.on("dialog", lambda d: (msgs.append(d.message), d.dismiss()))
    form = pg.locator("form[data-confirm-key] button[type=submit]")
    if form.count():
        url = pg.url; form.first.click(); pg.wait_for_timeout(300)
        assert msgs, "confirm dialog did not fire"
        assert pg.url == url  # dismissed → no submit
    cb = pg.locator("input[type=checkbox]").first
    if cb.count():
        v = cb.is_checked(); cb.click(); assert cb.is_checked() != v
    assert not pg.errors, pg.errors
