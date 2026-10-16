/* 个人主页：只画本人的关系网（本人居中 + 互动最多的人，大小 = 互动量）。数据 /api/v1/ego/<uid>，黑名单已在服务端剔除。 */
(function () {
  var box = document.getElementById("ego"); if (!box) return;
  var uid = box.dataset.uid, NS = "http://www.w3.org/2000/svg", FB = "https://cdn.discordapp.com/embed/avatars/0.png";
  function el(t, a, p) { var e = document.createElementNS(NS, t); for (var k in a) e.setAttribute(k, a[k]); if (p) p.appendChild(e); return e; }
  function draw(d) {
    box.innerHTML = "";
    if (!d || !d.partners || !d.partners.length) { box.innerHTML = '<div class="empty" style="height:100%;display:grid;place-items:center">关系网数据还没有生成</div>'; return; }
    var W = box.clientWidth || 600, H = Math.max(260, box.clientHeight || 300), cx = W / 2, cy = H / 2;
    var svg = el("svg", { viewBox: "0 0 " + W + " " + H, role: "img", "aria-label": "个人关系网" }, box);
    var defs = el("defs", {}, svg);
    var ps = d.partners, mx = ps[0].weight || 1, n = ps.length;
    var R = Math.min(W * 0.42, H * 0.40), rMin = Math.max(14, Math.min(W, H) * 0.045), rMax = Math.max(26, Math.min(W, H) * 0.11);
    var rad = function (w) { return rMin + (rMax - rMin) * Math.sqrt(w / mx); };
    var rc = Math.max(30, Math.min(W, H) * 0.13);
    el("circle", { cx: cx, cy: cy, r: R, class: "ring" }, svg);
    var nodes = ps.map(function (p, i) {
      var a = -Math.PI / 2 + i * 2 * Math.PI / n, rr = R * (i % 2 ? 0.92 : 1.0) * (W > H * 1.6 ? 1 : 1);
      var x = cx + Math.cos(a) * rr * (W > H ? Math.min(1.7, W / H) : 1), y = cy + Math.sin(a) * rr;
      x = Math.max(rad(p.weight) + 4, Math.min(W - rad(p.weight) - 4, x)); y = Math.max(rad(p.weight) + 4, Math.min(H - rad(p.weight) - 18, y));
      return { p: p, x: x, y: y, r: rad(p.weight) };
    });
    nodes.forEach(function (o) { el("line", { x1: cx, y1: cy, x2: o.x, y2: o.y, class: "lk", "stroke-width": 1 + 5 * Math.sqrt(o.p.weight / mx) }, svg); });
    function bubble(u, x, y, r, i, label, sub) {
      var id = "ego-av-" + i;
      var cp = el("clipPath", { id: id }, defs); el("circle", { cx: x, cy: y, r: r }, cp);
      var a = el("a", { href: "/user/" + u.user_id }, svg);
      var t = el("title", {}, a); t.textContent = (u.name || "") + (sub ? " · " + sub : "");
      el("circle", { cx: x, cy: y, r: r + 2, fill: "var(--white)", stroke: "var(--hairline)", "stroke-width": 1.5, class: "bg" }, a);
      var img = el("image", { x: x - r, y: y - r, width: 2 * r, height: 2 * r, "clip-path": "url(#" + id + ")", preserveAspectRatio: "xMidYMid slice" }, a);
      img.setAttributeNS("http://www.w3.org/1999/xlink", "href", u.avatar_url || FB);
      img.addEventListener("error", function () { img.setAttributeNS("http://www.w3.org/1999/xlink", "href", FB); });
      if (label) { var tx = el("text", { x: x, y: y + r + 14, "text-anchor": "middle" }, a); tx.textContent = String(u.name || "").slice(0, 10); }
    }
    nodes.forEach(function (o, i) { bubble(o.p, o.x, o.y, o.r, i, true, "互动 " + Number(o.p.weight).toLocaleString("zh-CN")); });
    bubble(d.center, cx, cy, rc, "c", false);
  }
  var data = null;
  fetch("/api/v1/ego/" + encodeURIComponent(uid), { credentials: "same-origin" }).then(function (r) { return r.json(); })
    .then(function (j) { data = j.data; draw(data); }).catch(function () { draw(null); });
  var t; window.addEventListener("resize", function () { clearTimeout(t); t = setTimeout(function () { if (data) draw(data); }, 150); });
})();
