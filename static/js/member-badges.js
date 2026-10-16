/* 退群 / 销号 标记：给所有指向 /user/<id> 的链接和个人页标题追加小徽章（只做标记，不隐藏）。 */
(function () {
  var cache = {}, pending = {}, timer = null;
  var CSS = ".ms-badge{display:inline-block;margin-left:6px;padding:0 6px;border-radius:8px;font-size:11px;line-height:17px;vertical-align:middle;font-weight:600;white-space:nowrap}" +
    ".ms-left{background:rgba(245,158,11,.15);color:#d97706;border:1px solid rgba(245,158,11,.4)}" +
    ".ms-deleted{background:rgba(107,114,128,.15);color:#6b7280;border:1px solid rgba(107,114,128,.4)}";
  var st = document.createElement("style"); st.textContent = CSS; document.head.appendChild(st);
  function uidOf(a) { var m = (a.getAttribute("href") || "").match(/\/user\/(\d{5,25})(?:[/?#]|$)/); return m ? m[1] : null; }
  function badge(info) {
    var s = document.createElement("span");
    s.className = "ms-badge " + (info.status === "deleted" ? "ms-deleted" : "ms-left");
    s.textContent = info.label; s.setAttribute("data-ms-badge", "1");
    if (info.left_at) s.title = "退群时间 " + info.left_at;
    return s;
  }
  function decorate(el, uid) {
    var info = cache[uid];
    if (!info || el.getAttribute("data-ms-done")) return;
    el.setAttribute("data-ms-done", "1");
    el.appendChild(badge(info));
  }
  function targets() {
    var out = [];
    document.querySelectorAll('a[href*="/user/"]').forEach(function (a) {
      if (a.closest(".menu-panel") || a.getAttribute("data-ms-done")) return;
      // 只给带名字的链接加徽章：纯头像链接（排行里头像和名字是两个链接）会导致同一人出现两次。
      if (!(a.textContent || "").trim() || a.querySelector("[data-ms-badge]")) return;
      var u = uidOf(a); if (u) out.push([a, u]);
    });
    var pm = location.pathname.match(/^\/user\/(\d{5,25})/);
    if (pm) {
      var h = document.querySelector("main h1, main .profile-name, main h2");
      if (h && !h.getAttribute("data-ms-done")) out.push([h, pm[1]]);
    }
    return out;
  }
  function run() {
    timer = null;
    var t = targets(), need = [];
    t.forEach(function (p) { if (!(p[1] in cache) && !pending[p[1]]) { pending[p[1]] = 1; need.push(p[1]); } });
    var apply = function () { targets().forEach(function (p) { decorate(p[0], p[1]); }); };
    if (!need.length) return apply();
    var chunks = [];
    for (var i = 0; i < need.length; i += 300) chunks.push(need.slice(i, i + 300));
    Promise.all(chunks.map(function (c) {
      return fetch("/api/v1/member-status?ids=" + c.join(","), { credentials: "same-origin" })
        .then(function (r) { return r.ok ? r.json() : { data: {} }; })
        .then(function (j) { c.forEach(function (u) { cache[u] = (j.data || {})[u] || null; delete pending[u]; }); })
        .catch(function () { c.forEach(function (u) { delete pending[u]; }); });
    })).then(apply);
  }
  function schedule() { if (!timer) timer = setTimeout(run, 250); }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", schedule); else schedule();
  new MutationObserver(function (ms) {
    for (var i = 0; i < ms.length; i++) if (ms[i].addedNodes.length) { schedule(); break; }
  }).observe(document.documentElement, { childList: true, subtree: true });
})();
