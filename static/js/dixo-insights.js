/* Insights page + homepage topic card + personal-page insights section. Dark-style aware, no deps beyond Chart.js. */
(function () {
  'use strict';
  var PAL = ['#2997ff','#30d158','#ff9f0a','#ff453a','#bf5af2','#64d2ff','#ffd60a','#ac8e68','#ff375f','#5e5ce6','#66d4cf','#a2845e'];
  var esc = function (s) { return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) { return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]; }); };
  var get = function (u) { return fetch(u, { credentials: 'same-origin' }).then(function (r) { return r.json(); }); };
  function md(s) { // tiny, escaped markdown: ### headings, - lists, **bold**
    return esc(s || '').split(/\n/).map(function (l) {
      l = l.replace(/\*\*(.+?)\*\*/g, '<b>$1</b>');
      if (/^#{1,4}\s/.test(l)) return '<h3>' + l.replace(/^#+\s/, '') + '</h3>';
      if (/^\s*[-*]\s/.test(l)) return '<li>' + l.replace(/^\s*[-*]\s/, '') + '</li>';
      return l.trim() ? '<p>' + l + '</p>' : '';
    }).join('');
  }
  window.dixoMd = md;
  function whenChart(fn) { if (window.Chart) return fn(); var n = 0, t = setInterval(function () { if (window.Chart || ++n > 120) { clearInterval(t); if (window.Chart) fn(); } }, 50); }
  function topicChart(canvas, d, topN, monthsBack) {
    var yms = d.months.slice(-(monthsBack || d.months.length));
    var tot = yms.map(function (ym) { var s = 0; d.topics.forEach(function (t) { s += (d.series[t.id] || {})[ym] || 0; }); return s || 1; });
    var top = d.topics.slice(0, topN || 8);
    var ds = top.map(function (t, i) { return { label: t.name, data: yms.map(function (ym, k) { return +(100 * ((d.series[t.id] || {})[ym] || 0) / tot[k]).toFixed(1); }),
      backgroundColor: PAL[i % PAL.length] + 'cc', borderWidth: 0, fill: true, pointRadius: 0, tension: .3 }; });
    whenChart(function () {
      new Chart(canvas, { type: 'line', data: { labels: yms, datasets: ds }, options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
        plugins: { legend: { labels: { boxWidth: 10 } }, tooltip: { callbacks: { label: function (c) { return c.dataset.label + ': ' + c.raw + '%'; } } } },
        scales: { y: { stacked: true, ticks: { callback: function (v) { return v + '%'; } }, grid: { color: 'rgba(127,127,127,.12)' } }, x: { grid: { display: false }, ticks: { maxTicksLimit: 8 } } } } });
    });
  }
  // ---------- homepage card
  var home = document.getElementById('insHomeTopics');
  if (home) get('/api/v1/insights/topics').then(function (j) {
    if (!j.ready || !j.data) { home.closest('[data-ins-card]').style.display = 'none'; return; }
    topicChart(home, j.data, 8, 24);
  });
  // ---------- personal page
  var mine = document.querySelector('[data-ins-member]');
  if (mine) get('/api/v1/insights/member/' + mine.getAttribute('data-ins-member')).then(function (j) {
    if (!j.ready || !j.data || (!j.data.years.length && !j.data.dossier)) { mine.style.display = 'none'; return; }
    var d = j.data, h = '';
    if (d.circle) h += '<p class="muted">' + esc(mine.getAttribute('data-l-circle')) + '：<b>' + esc(d.circle.name) + '</b> — ' + esc(d.circle.desc) + '</p>';
    if (d.years.length) h += '<div class="ins-grid">' + d.years.map(function (y) {
      return '<div class="ins-tile"><h3>' + y.year + ' · ' + y.msgs + ' 条 · #' + y.rank + '</h3>' +
        '<p>常用词：' + esc((y.phrases || []).join('、')) + '</p>' +
        '<p>最佳搭子：' + esc((y.buddies || []).slice(0, 3).map(function (b) { return b.name; }).join('、') || '—') + '</p>' +
        '<p>最忙的一天：' + esc(y.busiest_day.day) + '（' + y.busiest_day.msgs + ' 条）</p>' +
        (y.happiest_day ? '<p>最开心：' + esc(y.happiest_day.day) + ' · 最低落：' + esc(y.gloomiest_day.day) + '</p>' : '') +
        '<p>常驻帖子：' + esc((y.top_threads || []).slice(0, 2).map(function (t) { return t.title; }).join(' / ')) + '</p>' +
        (y.best_msg ? '<p class="faint">高赞：“' + esc(y.best_msg.text) + '”（' + y.best_msg.reacts + '）</p>' : '') + '</div>';
    }).join('') + '</div>';
    if (d.dossier) h += '<details style="margin-top:var(--space-3)"><summary class="h-card" style="cursor:pointer">' + esc(mine.getAttribute('data-l-dossier')) + '</summary><div class="md">' + md(d.dossier.md) + '</div></details>';
    mine.querySelector('[data-ins-body]').innerHTML = h;
  });
  // ---------- insights page
  if (!document.getElementById('ins-tabs')) return;
  var loaded = {};
  document.querySelectorAll('#ins-tabs [data-tab]').forEach(function (b) {
    b.addEventListener('click', function () {
      document.querySelectorAll('#ins-tabs [data-tab]').forEach(function (x) { x.classList.toggle('on', x === b); });
      document.querySelectorAll('[data-pane]').forEach(function (p) { p.hidden = p.getAttribute('data-pane') !== b.getAttribute('data-tab'); });
      load(b.getAttribute('data-tab'));
    });
  });
  function load(tab) {
    if (loaded[tab]) return; loaded[tab] = 1;
    if (tab === 'topics') get('/api/v1/insights/topics').then(function (j) {
      var d = j.data; topicChart(document.getElementById('insTopicChart'), d, 10, 0);
      var li = function (t) { return '<li><span class="truncate">' + esc(t.name) + '</span><span class="faint">' + (t.trend > 0 ? '+' : '') + (t.trend * 100).toFixed(0) + '%</span></li>'; };
      var byT = d.topics.filter(function (t) { return t.msgs > 2000; }).slice().sort(function (a, b) { return b.trend - a.trend; });
      document.getElementById('insRising').innerHTML = byT.slice(0, 8).map(li).join('');
      document.getElementById('insFading').innerHTML = byT.slice(-8).reverse().map(li).join('');
      document.getElementById('insTopicList').innerHTML = d.topics.map(function (t) {
        return '<div class="ins-tile"><h3>' + esc(t.name) + '</h3><p class="faint">' + t.threads + ' 帖 · ' + t.msgs + ' 条</p><p>' + esc(t.desc) + '</p><p class="faint">' + esc((t.examples || []).slice(0, 3).join(' / ')) + '</p></div>';
      }).join('');
    });
    if (tab === 'network') get('/api/v1/insights/network').then(function (j) { drawNet(j.data); });
    if (tab === 'memes') { var q = document.getElementById('insMemeQ'), t; var run = function () { get('/api/v1/insights/memes?q=' + encodeURIComponent(q.value)).then(function (j) {
      document.getElementById('insMemes').innerHTML = (j.data || []).map(function (m) {
        var ex = (m.examples || []).filter(function (x) { return x && x !== m.term; }).slice(0, 2);
        return '<div class="ins-meme"><b>' + esc(m.term) + '</b> <span class="faint">' + (m.kind === 'phrase' ? '语录 · ' : '') + m.total + ' 次 · ' + m.authors + ' 人用过 · 高峰 ' + esc(m.peak_ym) + '</span>' +
          (m.meaning ? '<p>' + esc(m.meaning) + '</p>' : '') + spark(m.monthly) +
          '<p class="faint">起源：' + esc(m.first_day) + ' · ' + esc(m.first_author) + (m.first_thread ? ' · 《' + esc(m.first_thread) + '》' : '') + '</p>' +
          (m.first_text ? '<div class="ins-q">首次：“' + esc(m.first_text) + '”</div>' : '') +
          ex.map(function (x) { return '<div class="ins-q">“' + esc(x) + '”</div>'; }).join('') + '</div>';
      }).join('') || '<p class="faint">—</p>'; }); };
      q.addEventListener('input', function () { clearTimeout(t); t = setTimeout(run, 250); }); run(); }
    if (tab === 'months') get('/api/v1/insights/months').then(function (j) {
      if (!j.data || !j.data.months || !j.data.months.length) { document.getElementById('insMonth').innerHTML = '<p class="faint">—</p>'; return; }
      var ms = j.data.months, sel = document.getElementById('insMonthSel');
      sel.innerHTML = ms.map(function (m, i) { return '<option value="' + i + '">' + m.ym + '</option>'; }).join('');
      var show = function () { var m = ms[+sel.value], d = m.data;
        document.getElementById('insMonth').innerHTML = '<p>' + esc(m.narrative || '') + '</p><div class="ins-grid">' +
          '<div class="ins-tile"><h3>数据</h3><p>' + d.msgs + ' 条消息 · ' + d.authors + ' 人发言 · 新人 ' + d.new_authors + '</p><p>平均情绪 ' + d.sentiment.mean + '</p></div>' +
          '<div class="ins-tile"><h3>热帖</h3>' + (d.top_threads || []).map(function (t) { return '<p>' + esc(t.title) + ' <span class="faint">' + t.msgs + '</span></p>'; }).join('') + '</div>' +
          '<div class="ins-tile"><h3>流行词</h3><p>' + esc((d.rising_terms || []).map(function (r) { return r.term; }).join('、') || '—') + '</p></div>' +
          (d.best_msg ? '<div class="ins-tile"><h3>本月高赞</h3><p>“' + esc(d.best_msg.text) + '”</p><p class="faint">' + d.best_msg.reacts + ' 个反应 · 《' + esc(d.best_msg.thread) + '》</p></div>' : '') + '</div>'; };
      sel.addEventListener('change', show); show();
      document.getElementById('insOverview').innerHTML = md(j.data.overview);
    });
  }
  function spark(mo) { // monthly usage → inline SVG spread curve
    var ks = Object.keys(mo || {}).sort(); if (ks.length < 2) return '';
    var y0 = +ks[0].slice(0, 4), m0 = +ks[0].slice(5), y1 = +ks[ks.length - 1].slice(0, 4), m1 = +ks[ks.length - 1].slice(5), n = (y1 - y0) * 12 + m1 - m0 + 1, v = [], mx = 1;
    for (var i = 0; i < n; i++) { var y = y0 + Math.floor((m0 - 1 + i) / 12), m = (m0 - 1 + i) % 12 + 1, k = y + '-' + (m < 10 ? '0' : '') + m; v.push(mo[k] || 0); mx = Math.max(mx, mo[k] || 0); }
    var pts = v.map(function (x, i) { return (i * 260 / (n - 1)).toFixed(1) + ',' + (32 - 30 * x / mx).toFixed(1); }).join(' ');
    return '<svg class="ins-spark" viewBox="0 0 260 34" preserveAspectRatio="none"><polyline points="0,34 ' + pts + ' 260,34" fill="rgba(0,102,204,.12)" stroke="none"/><polyline points="' + pts + '" fill="none" stroke="#0066cc" stroke-width="1.5"/></svg>' +
      '<span class="faint" style="font-size:11px">' + ks[0] + ' → ' + ks[ks.length - 1] + '</span>';
  }
  function drawNet(d) { // Bubblemaps-style bubble map: d3-force on canvas, zoom/pan/pinch, drag, hover, click-to-focus, search
    var cv = document.getElementById('insNet'), g = d.graph; if (!g || !window.d3) return;
    var wrap = cv.parentNode, tip = document.getElementById('insNetTip'), side = document.getElementById('insNetSide');
    var ctx = cv.getContext('2d'), dpr = window.devicePixelRatio || 1, W = 0, H = 0, T = { k: 1, x: 0, y: 0 };
    var byId = {}, adj = {}, maxw = 1, CN = function (c) { return d.circles[c] ? d.circles[c].name : '—'; };
    var col = function (n) { return n.c >= 0 ? PAL[n.c % PAL.length] : '#8e8e93'; };
    g.nodes.forEach(function (n) { byId[n.id] = n; adj[n.id] = []; n.r = 4 + Math.sqrt(n.w) * 0.9; n.x = n.x * 600; n.y = n.y * 600; if (n.w > maxw) maxw = n.w; });
    var scale = 46 / Math.sqrt(maxw) / 0.9; g.nodes.forEach(function (n) { n.r = Math.max(3, Math.sqrt(n.w) * 0.9 * scale); });
    var links = [], lmax = 1;
    g.edges.forEach(function (e) { var a = byId[e[0]], b = byId[e[1]]; if (!a || !b) return; links.push({ source: a, target: b, w: e[2] });
      adj[a.id].push([b, e[2]]); adj[b.id].push([a, e[2]]); if (e[2] > lmax) lmax = e[2]; });
    Object.keys(adj).forEach(function (k) { adj[k].sort(function (x, y) { return y[1] - x[1]; }); });
    var sel = null, hov = null, nb = {};
    function size() { W = wrap.clientWidth; H = cv.clientHeight; cv.width = W * dpr; cv.height = H * dpr; draw(); }
    function draw() {
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, W, H);
      ctx.translate(W / 2 + T.x, H / 2 + T.y); ctx.scale(T.k, T.k);
      links.forEach(function (l) { var on = sel && (l.source === sel || l.target === sel);
        if (sel && !on) return; var a = Math.log1p(l.w) / Math.log1p(lmax);
        ctx.strokeStyle = on ? 'rgba(0,102,204,' + (0.25 + 0.6 * a) + ')' : 'rgba(110,110,115,' + (0.03 + 0.12 * a) + ')';
        ctx.lineWidth = (on ? 0.8 + 3 * a : 0.4 + a) / T.k; ctx.beginPath(); ctx.moveTo(l.source.x, l.source.y); ctx.lineTo(l.target.x, l.target.y); ctx.stroke(); });
      g.nodes.forEach(function (n) { var dim = sel && n !== sel && !nb[n.id], c = col(n);
        ctx.globalAlpha = dim ? 0.12 : 1; ctx.beginPath(); ctx.arc(n.x, n.y, n.r, 0, 6.2832);
        ctx.fillStyle = c + '38'; ctx.fill(); ctx.lineWidth = (n === sel || n === hov ? 3 : 1.4) / T.k; ctx.strokeStyle = n === sel ? '#1d1d1f' : c; ctx.stroke();
        if (n.r * T.k > 16 || n === sel || (sel && nb[n.id] && n.r * T.k > 7)) { var fs = Math.max(10, Math.min(14, n.r * T.k / 3.2)) / T.k;
          ctx.font = '600 ' + fs + 'px -apple-system,"PingFang SC",sans-serif'; ctx.fillStyle = '#1d1d1f'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
          var t = n.name.length > 8 ? n.name.slice(0, 7) + '…' : n.name; ctx.fillText(t, n.x, n.y); } });
      ctx.globalAlpha = 1;
    }
    var sim = d3.forceSimulation(g.nodes)
      .force('link', d3.forceLink(links).distance(function (l) { return l.source.r + l.target.r + 30; }).strength(function (l) { return 0.05 + 0.5 * Math.log1p(l.w) / Math.log1p(lmax); }))
      .force('charge', d3.forceManyBody().strength(function (n) { return -20 - n.r * 3; }).distanceMax(500))
      .force('collide', d3.forceCollide(function (n) { return n.r + 1.5; }).iterations(2))
      .force('x', d3.forceX(0).strength(0.04)).force('y', d3.forceY(0).strength(0.04))
      .alphaDecay(0.03).on('tick', draw);
    sim.on('end', function () { if (!fitted) fit(); });
    var fitted = false;
    function fit() { var x0 = 1e9, x1 = -1e9, y0 = 1e9, y1 = -1e9; g.nodes.forEach(function (n) { x0 = Math.min(x0, n.x - n.r); x1 = Math.max(x1, n.x + n.r); y0 = Math.min(y0, n.y - n.r); y1 = Math.max(y1, n.y + n.r); });
      T.k = Math.min(W / (x1 - x0), H / (y1 - y0)) * 0.94; T.x = -T.k * (x0 + x1) / 2; T.y = -T.k * (y0 + y1) / 2; fitted = true; draw(); }
    setTimeout(fit, 1600);
    var inv = function (px, py) { return [(px - W / 2 - T.x) / T.k, (py - H / 2 - T.y) / T.k]; };
    var hit = function (px, py) { var p = inv(px, py), best = null; g.nodes.forEach(function (n) { var dx = n.x - p[0], dy = n.y - p[1]; if (dx * dx + dy * dy <= n.r * n.r && (!best || n.r < best.r)) best = n; }); return best; };
    function select(n, zoom) {
      sel = n; nb = {}; if (!n) { side.hidden = true; draw(); return; }
      adj[n.id].forEach(function (x) { nb[x[0].id] = 1; });
      var tot = adj[n.id].reduce(function (s, x) { return s + x[1]; }, 0);
      side.hidden = false;
      side.innerHTML = '<button type="button" class="ins-x" aria-label="close">×</button><div class="ins-side-h"><span class="ins-dot" style="background:' + col(n) + '"></span><b>' + esc(n.name) + '</b></div>' +
        '<p class="faint">' + esc(CN(n.c)) + ' · 互动 ' + n.w + ' · 连接 ' + adj[n.id].length + ' 人</p><a class="ins-prof" href="/user/' + esc(n.id) + '">个人页 ›</a>' +
        '<h4>最常互动</h4><ol>' + adj[n.id].slice(0, 12).map(function (x) { return '<li data-id="' + esc(x[0].id) + '"><span class="ins-dot" style="background:' + col(x[0]) + '"></span><span class="truncate">' + esc(x[0].name) +
        '</span><span class="faint">' + x[1] + ' · ' + Math.round(100 * x[1] / (tot || 1)) + '%</span></li>'; }).join('') + '</ol>';
      side.querySelector('.ins-x').onclick = function () { select(null); };
      side.querySelectorAll('li[data-id]').forEach(function (li) { li.onclick = function () { select(byId[li.getAttribute('data-id')], true); }; });
      if (zoom) { T.k = Math.max(T.k, Math.min(3, 60 / n.r)); T.x = -n.x * T.k; T.y = -n.y * T.k; }
      draw();
    }
    // pointer: pan, drag node, pinch zoom, tap/click select, hover tooltip
    var pts = {}, drag = null, pan = null, pinch = null, moved = 0;
    cv.addEventListener('pointerdown', function (e) { cv.setPointerCapture(e.pointerId); var r = cv.getBoundingClientRect(), px = e.clientX - r.left, py = e.clientY - r.top;
      pts[e.pointerId] = [px, py]; moved = 0; var ids = Object.keys(pts);
      if (ids.length === 2) { var a = pts[ids[0]], b = pts[ids[1]]; pinch = { d: Math.hypot(a[0] - b[0], a[1] - b[1]), k: T.k, x: T.x, y: T.y, cx: (a[0] + b[0]) / 2, cy: (a[1] + b[1]) / 2 }; drag = pan = null; return; }
      var n = hit(px, py); if (n) { drag = n; n.fx = n.x; n.fy = n.y; } else pan = { px: px, py: py, x: T.x, y: T.y }; });
    cv.addEventListener('pointermove', function (e) { var r = cv.getBoundingClientRect(), px = e.clientX - r.left, py = e.clientY - r.top;
      if (pts[e.pointerId]) { var o = pts[e.pointerId]; moved += Math.abs(px - o[0]) + Math.abs(py - o[1]); pts[e.pointerId] = [px, py]; }
      var ids = Object.keys(pts);
      if (pinch && ids.length === 2) { var a = pts[ids[0]], b = pts[ids[1]], k = Math.max(0.05, Math.min(8, pinch.k * Math.hypot(a[0] - b[0], a[1] - b[1]) / pinch.d));
        var cx = pinch.cx - W / 2, cy = pinch.cy - H / 2; T.x = cx - (cx - pinch.x) * k / pinch.k; T.y = cy - (cy - pinch.y) * k / pinch.k; T.k = k; draw(); return; }
      if (drag) { var p = inv(px, py); drag.fx = p[0]; drag.fy = p[1]; if (moved > 4) sim.alphaTarget(0.2).restart(); return; }
      if (pan) { T.x = pan.x + px - pan.px; T.y = pan.y + py - pan.py; draw(); return; }
      if (e.pointerType === 'mouse') { var n = hit(px, py); if (n !== hov) { hov = n; draw(); }
        cv.style.cursor = n ? 'pointer' : 'grab';
        if (n) { tip.hidden = false; tip.style.left = Math.min(px + 14, W - 180) + 'px'; tip.style.top = (py + 14) + 'px';
          tip.innerHTML = '<b>' + esc(n.name) + '</b><br><span class="faint">' + esc(CN(n.c)) + ' · 互动 ' + n.w + ' · ' + adj[n.id].length + ' 人</span>'; } else tip.hidden = true; } });
    function up(e) { var r = cv.getBoundingClientRect(), px = e.clientX - r.left, py = e.clientY - r.top; delete pts[e.pointerId];
      if (pinch) { if (Object.keys(pts).length < 2) pinch = null; return; }
      if (drag) { drag.fx = drag.fy = null; sim.alphaTarget(0); }
      if (moved < 6) select(hit(px, py) || null);
      drag = pan = null; }
    cv.addEventListener('pointerup', up); cv.addEventListener('pointercancel', function (e) { delete pts[e.pointerId]; drag = pan = pinch = null; });
    cv.addEventListener('pointerleave', function () { tip.hidden = true; if (hov) { hov = null; draw(); } });
    cv.addEventListener('wheel', function (e) { e.preventDefault(); var r = cv.getBoundingClientRect(), cx = e.clientX - r.left - W / 2, cy = e.clientY - r.top - H / 2;
      var k = Math.max(0.05, Math.min(8, T.k * Math.exp(-e.deltaY * 0.0015))); T.x = cx - (cx - T.x) * k / T.k; T.y = cy - (cy - T.y) * k / T.k; T.k = k; draw(); }, { passive: false });
    document.getElementById('insNetFit').onclick = function () { select(null); fit(); };
    // search
    var q = document.getElementById('insNetQ'), res = document.getElementById('insNetRes');
    q.addEventListener('input', function () { var v = q.value.trim().toLowerCase(); if (!v) { res.hidden = true; return; }
      var m = g.nodes.filter(function (n) { return n.name.toLowerCase().indexOf(v) >= 0; }).sort(function (a, b) { return b.w - a.w; }).slice(0, 8);
      res.hidden = !m.length; res.innerHTML = m.map(function (n) { return '<li data-id="' + esc(n.id) + '"><span class="ins-dot" style="background:' + col(n) + '"></span>' + esc(n.name) + ' <span class="faint">' + esc(CN(n.c)) + '</span></li>'; }).join('');
      res.querySelectorAll('li').forEach(function (li) { li.onmousedown = li.ontouchstart = function (ev) { ev.preventDefault(); select(byId[li.getAttribute('data-id')], true); res.hidden = true; q.value = byId[li.getAttribute('data-id')].name; q.blur(); }; }); });
    q.addEventListener('keydown', function (e) { if (e.key === 'Enter') { var li = res.querySelector('li'); if (li) li.onmousedown(e); } });
    q.addEventListener('blur', function () { setTimeout(function () { res.hidden = true; }, 150); });
    var me = wrap.getAttribute('data-me'); if (me && byId[me]) { document.getElementById('insNetMe').hidden = false; document.getElementById('insNetMe').onclick = function () { select(byId[me], true); }; }
    window.addEventListener('resize', size); size();
    document.getElementById('insNetInfo').textContent = g.nodes.length + ' 人 · ' + links.length + ' 条互动 · ' + d.circles.length + ' 个圈子';
    document.getElementById('insCircles').innerHTML = d.circles.map(function (c, i) {
      return '<div class="ins-tile"><h3><span class="ins-dot" style="background:' + PAL[i % PAL.length] + '"></span>' + esc(c.name) + ' <span class="faint">' + c.size + ' 人</span></h3><p>' + esc(c.desc) + '</p>' +
        '<p>核心：' + esc(c.core.slice(0, 6).map(function (x) { return x.name; }).join('、')) + '</p><p class="faint">' + esc((c.words || []).slice(0, 8).join('、')) + '</p></div>';
    }).join('');
  }
  load('topics');
})();
