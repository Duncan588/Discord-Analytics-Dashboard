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
        return '<div class="ins-meme"><b>' + esc(m.term) + '</b> <span class="faint">' + (m.kind === 'emoji' ? '表情 · ' : m.kind === 'phrase' ? '语录 · ' : '') + m.total + ' 次 · ' + m.authors + ' 人 · 高峰 ' + esc(m.peak_ym) + '</span>' +
          '<p>' + esc(m.meaning) + '</p><p class="faint">起源：' + esc(m.origin) + '</p><p class="faint">首次：' + esc(m.first_day) + ' · ' + esc(m.first_author) + ' · 《' + esc(m.first_thread) + '》</p><p class="faint">用法：' + esc(m.usage) + '</p></div>';
      }).join('') || '<p class="faint">—</p>'; }); };
      q.addEventListener('input', function () { clearTimeout(t); t = setTimeout(run, 250); }); run(); }
    if (tab === 'months') get('/api/v1/insights/months').then(function (j) {
      var ms = j.data.months, sel = document.getElementById('insMonthSel');
      sel.innerHTML = ms.map(function (m, i) { return '<option value="' + i + '">' + m.ym + '</option>'; }).join('');
      var show = function () { var m = ms[+sel.value], d = m.data;
        document.getElementById('insMonth').innerHTML = '<p>' + esc(m.narrative || '') + '</p><div class="ins-grid">' +
          '<div class="ins-tile"><h3>数据</h3><p>' + d.msgs + ' 条消息 · ' + d.authors + ' 人发言 · 新人 ' + d.new_authors + '</p><p>平均情绪 ' + d.sentiment.mean + '</p></div>' +
          '<div class="ins-tile"><h3>热帖</h3>' + d.top_threads.map(function (t) { return '<p>' + esc(t.title) + ' <span class="faint">' + t.msgs + '</span></p>'; }).join('') + '</div>' +
          '<div class="ins-tile"><h3>流行词</h3><p>' + esc(d.rising_terms.map(function (r) { return r.term; }).join('、') || '—') + '</p></div>' +
          '<div class="ins-tile"><h3>本月高赞</h3><p>“' + esc(d.best_msg.text) + '”</p><p class="faint">' + d.best_msg.reacts + ' 个反应 · 《' + esc(d.best_msg.thread) + '》</p></div></div>'; };
      sel.addEventListener('change', show); show();
      document.getElementById('insOverview').innerHTML = md(j.data.overview);
    });
  }
  function drawNet(d) {
    var cv = document.getElementById('insNet'), g = d.graph; if (!g) return;
    var dpr = window.devicePixelRatio || 1, W = cv.clientWidth, H = cv.clientHeight; cv.width = W * dpr; cv.height = H * dpr;
    var ctx = cv.getContext('2d'); ctx.scale(dpr, dpr);
    var xs = g.nodes.map(function (n) { return n.x; }), ys = g.nodes.map(function (n) { return n.y; });
    var mnx = Math.min.apply(0, xs), mxx = Math.max.apply(0, xs), mny = Math.min.apply(0, ys), mxy = Math.max.apply(0, ys);
    var P = function (n) { return [20 + (n.x - mnx) / (mxx - mnx || 1) * (W - 40), 20 + (n.y - mny) / (mxy - mny || 1) * (H - 40)]; };
    var byId = {}; g.nodes.forEach(function (n) { n.p = P(n); n.r = 2 + Math.sqrt(n.w) / 12; byId[n.id] = n; });
    var maxw = g.edges.length ? g.edges[0][2] : 1;
    g.edges.forEach(function (e) { var a = byId[e[0]], b = byId[e[1]]; if (!a || !b) return;
      ctx.strokeStyle = 'rgba(160,160,170,' + (0.05 + 0.4 * e[2] / maxw) + ')'; ctx.lineWidth = 0.5 + 2 * e[2] / maxw; ctx.beginPath(); ctx.moveTo(a.p[0], a.p[1]); ctx.lineTo(b.p[0], b.p[1]); ctx.stroke(); });
    g.nodes.forEach(function (n) { ctx.fillStyle = n.c >= 0 ? PAL[n.c % PAL.length] : '#888'; ctx.beginPath(); ctx.arc(n.p[0], n.p[1], n.r, 0, 6.3); ctx.fill(); });
    ctx.fillStyle = getComputedStyle(document.body).color; ctx.font = '11px sans-serif';
    g.nodes.slice().sort(function (a, b) { return b.w - a.w; }).slice(0, 40).forEach(function (n) { ctx.fillText(n.name, n.p[0] + n.r + 2, n.p[1] + 3); });
    document.getElementById('insNetInfo').textContent = g.nodes.length + ' / ' + d.circles.length;
    cv.addEventListener('mousemove', function (ev) { var r = cv.getBoundingClientRect(), x = ev.clientX - r.left, y = ev.clientY - r.top, best = null, bd = 144;
      g.nodes.forEach(function (n) { var dd = (n.p[0] - x) * (n.p[0] - x) + (n.p[1] - y) * (n.p[1] - y); if (dd < bd) { bd = dd; best = n; } });
      var c = best && d.circles[best.c]; document.getElementById('insNetHover').textContent = best ? best.name + (c ? ' · ' + c.name : '') : ''; });
    document.getElementById('insCircles').innerHTML = d.circles.map(function (c, i) {
      return '<div class="ins-tile"><h3><span class="ins-dot" style="background:' + PAL[i % PAL.length] + '"></span>' + esc(c.name) + ' <span class="faint">' + c.size + ' 人</span></h3><p>' + esc(c.desc) + '</p>' +
        '<p>核心：' + esc(c.core.slice(0, 6).map(function (x) { return x.name; }).join('、')) + '</p><p class="faint">' + esc((c.words || []).slice(0, 8).join('、')) + '</p></div>';
    }).join('');
  }
  load('topics');
})();
