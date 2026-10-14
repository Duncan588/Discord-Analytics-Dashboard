/** Sentiment card: stacked pos/neu/neg share (bars) + mean score line. Aggregates only, no member lists.
 *  Markup: <div data-sentiment-card data-url="/api/v1/sentiment[?user=ID]"> with .seg [data-srange] buttons,
 *  a <canvas data-sentiment-chart> and a [data-sentiment-empty] element. */
(function () {
  'use strict';
  var T = function (k, zh) { return window.dixoT ? dixoT(k, zh) : zh; };
  function whenChart(fn) {
    if (window.Chart) { fn(); return; }
    var n = 0, t = setInterval(function () { if (window.Chart || ++n > 120) { clearInterval(t); if (window.Chart) fn(); } }, 50);
  }
  function init(card) {
    var empty = card.querySelector('[data-sentiment-empty]'), wrap = card.querySelector('[data-sentiment-wrap]');
    var canvas = card.querySelector('canvas[data-sentiment-chart]');
    fetch(card.getAttribute('data-url'), { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        var daily = (j && j.data && j.data.daily) || [];
        if (!j || !j.ok || !j.data.ready || !daily.length) {
          if (empty) empty.textContent = T('sent.empty', '情绪数据尚未生成（历史消息正在打分中）');
          return;
        }
        if (empty) empty.style.display = 'none';
        if (wrap) wrap.style.display = '';
        whenChart(function () { draw(card, canvas, daily); });
      })
      .catch(function (e) { if (empty) empty.textContent = String(e.message || e); });
  }
  function draw(card, canvas, daily) {
    var dark = document.documentElement.getAttribute('data-theme') === 'dark';
    var grid = dark ? 'rgba(255,255,255,.08)' : 'rgba(0,0,0,.06)';
    var ch = new Chart(canvas, {
      data: { labels: [], datasets: [
        { type: 'bar', label: T('sent.pos', '积极'), data: [], backgroundColor: 'rgba(48,209,88,.75)', stack: 's', yAxisID: 'y', barPercentage: 1, categoryPercentage: 1 },
        { type: 'bar', label: T('sent.neu', '中性'), data: [], backgroundColor: dark ? 'rgba(142,142,147,.45)' : 'rgba(142,142,147,.35)', stack: 's', yAxisID: 'y', barPercentage: 1, categoryPercentage: 1 },
        { type: 'bar', label: T('sent.neg', '消极'), data: [], backgroundColor: 'rgba(255,69,58,.75)', stack: 's', yAxisID: 'y', barPercentage: 1, categoryPercentage: 1 },
        { type: 'line', label: T('sent.mean', '平均情绪（7 日）'), data: [], borderColor: dark ? '#ffd60a' : '#b38600', borderWidth: 2, pointRadius: 0, tension: 0.3, yAxisID: 'y2' }
      ] },
      options: { responsive: true, maintainAspectRatio: false, animation: { duration: 250 }, interaction: { mode: 'index', intersect: false },
        plugins: { legend: { display: true, labels: { boxWidth: 10, boxHeight: 10 } },
          tooltip: { backgroundColor: 'rgba(29,29,31,.92)', cornerRadius: 8, callbacks: { label: function (c) {
            return c.dataset.label + ': ' + (c.dataset.yAxisID === 'y2' ? Number(c.raw).toFixed(3) : c.raw + '%'); } } } },
        scales: {
          x: { stacked: true, grid: { display: false }, border: { display: false }, ticks: { maxTicksLimit: 6, maxRotation: 0 } },
          y: { stacked: true, min: 0, max: 100, grid: { color: grid, drawTicks: false }, border: { display: false }, ticks: { maxTicksLimit: 5, callback: function (v) { return v + '%'; } } },
          y2: { position: 'right', min: -1, max: 1, grid: { display: false }, border: { display: false }, ticks: { maxTicksLimit: 5 } } } }
    });
    // 7-day message-weighted mean so sparse days don't whip the line around.
    var mean = daily.map(function (_, i) {
      var s = 0, n = 0;
      for (var k = Math.max(0, i - 6); k <= i; k++) { s += daily[k].mean * daily[k].n; n += daily[k].n; }
      return n ? +(s / n).toFixed(4) : null;
    });
    function range(n) {
      var s = n ? Math.max(0, daily.length - n) : 0, d = daily.slice(s);
      ch.data.labels = d.map(function (x) { return x.day; });
      ch.data.datasets[0].data = d.map(function (x) { return x.pos_pct; });
      ch.data.datasets[1].data = d.map(function (x) { return x.neu_pct; });
      ch.data.datasets[2].data = d.map(function (x) { return x.neg_pct; });
      ch.data.datasets[3].data = mean.slice(s);
      ch.update();
      card.querySelectorAll('[data-srange]').forEach(function (b) { b.classList.toggle('on', b.getAttribute('data-srange') === String(n)); });
    }
    card.querySelectorAll('[data-srange]').forEach(function (b) {
      b.addEventListener('click', function () { range(+b.getAttribute('data-srange')); });
    });
    range(90);
  }
  function boot() { document.querySelectorAll('[data-sentiment-card]').forEach(init); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();
