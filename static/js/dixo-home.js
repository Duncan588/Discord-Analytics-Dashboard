/** Homepage charts + analytics panel (Alpine-independent boot). */
(function () {
  'use strict';

  function whenChart(fn) {
    if (window.Chart) { fn(); return; }
    var n = 0, t = setInterval(function () {
      if (window.Chart || ++n > 120) {
        clearInterval(t);
        if (window.Chart) fn();
        else console.error('[dixo] Chart.js failed to load');
      }
    }, 50);
  }

  function readJson(id) {
    var el = document.getElementById(id);
    if (!el) return null;
    try { return JSON.parse(el.textContent || 'null'); } catch (e) { console.error('[dixo] bad JSON', id, e); return null; }
  }

  function ma(arr, n) {
    var out = [], s = 0;
    arr.forEach(function (v, i) {
      s += v;
      if (i >= n) s -= arr[i - n];
      out.push(i >= n - 1 ? Math.round(s / n) : null);
    });
    return out;
  }

  function T(k, zh) { return window.dixoT ? dixoT(k, zh) : zh; }

  function baseOpts(dark) {
    var grid = { color: dark ? 'rgba(255,255,255,.08)' : 'rgba(0,0,0,.06)', drawTicks: false };
    var tick = dark ? '#a1a1a6' : '#86868b';
    var mobile = window.matchMedia && window.matchMedia('(max-width: 720px)').matches;
    return {
      responsive: true, maintainAspectRatio: false, animation: { duration: 250 },
      layout: { padding: { bottom: mobile ? 8 : 4, left: 2, right: 4, top: 4 } },
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: { display: true, align: 'end', labels: { boxWidth: 10, boxHeight: 10, usePointStyle: true } },
        tooltip: { backgroundColor: dark ? 'rgba(28,28,30,.95)' : 'rgba(29,29,31,.92)', padding: 10, cornerRadius: 8 }
      },
      scales: {
        y: { beginAtZero: true, grid: grid, border: { display: false }, ticks: { padding: 8, maxTicksLimit: 4, color: tick, callback: function (v) { return window.dixoCompact ? dixoCompact(v) : v; } } },
        x: { grid: { display: false }, border: { display: false },
          ticks: { maxTicksLimit: mobile ? 4 : 8, maxRotation: mobile ? 0 : 0, autoSkip: true, color: tick,
            callback: function (val, i, ticks) {
              var label = this.getLabelForValue(val);
              if (!mobile || !label) return label;
              // shorten ISO dates on narrow screens
              if (/^\d{4}-\d{2}-\d{2}$/.test(label)) return label.slice(5);
              if (/^\d{4}-\d{2}$/.test(label)) return label.slice(2);
              return label;
            } } }
      }
    };
  }

  function initTrendCharts() {
    var data = readJson('dixo-home-data');
    if (!data) return;
    var daily = data.chart_daily || [], hourly = data.chart_hourly || [];
    whenChart(function () {
      try {
        var dark = document.documentElement.getAttribute('data-theme') === 'dark';
        Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
        Chart.defaults.color = dark ? '#a1a1a6' : '#86868b';
        var base = baseOpts(dark);
        var counts = daily.map(function (d) { return d.c; }), avg = ma(counts, 7);
        var dcEl = document.getElementById('chartDaily');
        var hcEl = document.getElementById('chartHourly');
        if (!dcEl || !hcEl) return;
        var dc = new Chart(dcEl, {
          type: 'line',
          data: {
            labels: daily.map(function (d) { return d.day; }),
            datasets: [
              { label: T('home.trend.messages', '消息'), data: counts, borderColor: 'rgba(41,151,255,.55)', backgroundColor: 'rgba(41,151,255,.08)', fill: true, borderWidth: 1.25, pointRadius: 0, tension: 0.25 },
              { label: T('home.trend.ma7', '7 日均线'), data: avg, borderColor: '#0066cc', borderWidth: 2, pointRadius: 0, tension: 0.35 }
            ]
          },
          options: base
        });
        var hc = new Chart(hcEl, {
          type: 'bar',
          data: {
            labels: hourly.map(function (d) { return d.hour; }),
            datasets: [{ label: T('home.trend.messages', '消息'), data: hourly.map(function (d) { return d.c; }), backgroundColor: '#2997ff', borderRadius: 4, maxBarThickness: 18 }]
          },
          options: Object.assign({}, base, { plugins: Object.assign({}, base.plugins, { legend: { display: false } }) })
        });
        function range(n) {
          var s = n ? Math.max(0, daily.length - n) : 0;
          dc.data.labels = daily.slice(s).map(function (d) { return d.day; });
          dc.data.datasets[0].data = counts.slice(s);
          dc.data.datasets[1].data = avg.slice(s);
          dc.update();
        }
        range(90);
        window.dixoCharts = { range: range, dc: dc, hc: hc };
        document.addEventListener('dixo:lang', function () {
          dc.data.datasets[0].label = T('home.trend.messages', '消息');
          dc.data.datasets[1].label = T('home.trend.ma7', '7 日均线');
          hc.data.datasets[0].label = dc.data.datasets[0].label;
          dc.update(); hc.update();
        });
        document.addEventListener('dixo:theme', function () {
          /* recreate on theme change via page paint is enough; skip full rebuild */
        });
        var status = document.getElementById('dixo-trend-status');
        if (status) status.textContent = '';
      } catch (err) {
        console.error('[dixo] trend charts', err);
        var st = document.getElementById('dixo-trend-status');
        if (st) st.textContent = String(err.message || err);
      }
    });
  }

  function analyticsController() {
    return {
      grain: 'day', preset: '90', since: '', until: '', data: null, charts: {}, status: '', tagFilter: 'always_active', tagPage: 1, tagPageSize: 40,
      boot: function () {
        this.reload();
        document.addEventListener('dixo:theme', this.paint.bind(this));
        document.addEventListener('dixo:lang', this.paint.bind(this));
        var self = this;
        document.querySelectorAll('[data-tag-filter]').forEach(function (btn) {
          btn.addEventListener('click', function () {
            self.tagFilter = btn.getAttribute('data-tag-filter');
            self.tagPage = 1;
            self.renderActivityTags();
          });
        });
        var listHost = document.getElementById('anTagSamples');
        if (listHost && !listHost._dixoPagerBound) {
          listHost._dixoPagerBound = true;
          listHost.addEventListener('click', function (ev) {
            var b = ev.target && ev.target.closest ? ev.target.closest('[data-tag-page]') : null;
            if (!b) return;
            ev.preventDefault();
            if (b.getAttribute('data-tag-page') === 'prev') self.tagPage = Math.max(1, self.tagPage - 1);
            else self.tagPage = self.tagPage + 1;
            self.renderActivityTags();
          });
        }
      },
      setGrain: function (g) { this.grain = g; this.paint(); },
      setPreset: function (p) { this.preset = String(p); if (p !== 'custom') this.reload(); },
      rangeArgs: function () {
        if (this.preset === 'custom') return { since: this.since || null, until: this.until || null };
        if (this.preset === 'all') return {};
        var n = parseInt(this.preset, 10); var d = new Date(); d.setDate(d.getDate() - n);
        return { since: d.toISOString().slice(0, 10) };
      },
      reload: function () {
        var q = new URLSearchParams(); var r = this.rangeArgs();
        if (r.since) q.set('since', r.since); if (r.until) q.set('until', r.until);
        this.status = '…';
        var self = this;
        fetch('/api/v1/analytics?' + q.toString(), { credentials: 'same-origin' })
          .then(function (res) { if (!res.ok) throw new Error('HTTP ' + res.status); return res.json(); })
          .then(function (j) {
            if (!j || !j.data) throw new Error('bad payload');
            self.data = j.data; self.status = ''; self.paint();
          })
          .catch(function (e) { self.status = String(e.message || e); console.error('[dixo] analytics', e); });
      },
      series: function (kind) {
        if (!this.data) return [];
        if (kind === 'messages') return this.data['messages_' + this.grain] || this.data.messages_day || [];
        if (kind === 'users') return this.data['active_users_' + this.grain] || this.data.active_users_day || [];
        if (kind === 'threads') return this.data.thread_growth_month || [];
        if (kind === 'reply') return this.data.reply_rate_week || [];
        return [];
      },
      renderActivityTags: function () {
        var tags = (this.data && this.data.activity_tags) || {};
        var counts = tags.counts || {};
        var labels = tags.labels || {};
        var order = ['always_active', 'returned', 'faded', 'rare', 'new'];
        var pill = { always_active: 'tag-always', returned: 'tag-returned', faded: 'tag-faded', rare: 'tag-rare', new: 'tag-new' };
        var countsEl = document.getElementById('anTagCounts');
        if (countsEl) {
          countsEl.innerHTML = order.map(function (k) {
            var lab = (labels[k] && (document.documentElement.lang === 'en' ? labels[k].en : labels[k].zh)) || k;
            return '<div class="activity-tag-card"><div class="lab">' + lab + '</div><div class="n num" title="' + (counts[k] || 0) + '">' + (window.dixoCompact ? dixoCompact(counts[k] || 0) : (counts[k] || 0)) + '</div></div>';
          }).join('');
        }
        var filter = this.tagFilter || 'always_active';
        document.querySelectorAll('[data-tag-filter]').forEach(function (btn) {
          btn.classList.toggle('on', btn.getAttribute('data-tag-filter') === filter);
        });
        var all = ((tags.by_tag || tags.samples || {})[filter]) || [];
        var pageSize = this.tagPageSize || 40;
        var totalPages = Math.max(1, Math.ceil(all.length / pageSize));
        if (this.tagPage > totalPages) this.tagPage = totalPages;
        if (this.tagPage < 1) this.tagPage = 1;
        var start = (this.tagPage - 1) * pageSize;
        var pageRows = all.slice(start, start + pageSize);
        var list = document.getElementById('anTagSamples');
        if (list) {
          var rowsHtml = pageRows.map(function (r) {
            var name = r.nickname || r.username || r.user_id;
            var av = r.avatar_url || 'https://cdn.discordapp.com/embed/avatars/0.png';
            return '<a href="/user/' + r.user_id + '"><img class="avatar av-24" src="' + av + '" alt=""><span class="truncate" style="flex:1">' + name + '</span><span class="tag-pill ' + (pill[filter] || '') + '">' + (r.active_months || 0) + ' mo</span></a>';
          }).join('') || '<div class="empty">—</div>';
          var pager = '';
          if (all.length > pageSize) {
            pager = '<div class="pager" style="margin-top:10px">' +
              '<button type="button" class="btn btn-ghost" data-tag-page="prev" ' + (this.tagPage <= 1 ? 'disabled' : '') + '>‹</button>' +
              '<span class="faint mono" style="padding:0 10px">' + this.tagPage + ' / ' + totalPages + ' · ' + all.length + '</span>' +
              '<button type="button" class="btn btn-ghost" data-tag-page="next" ' + (this.tagPage >= totalPages ? 'disabled' : '') + '>›</button></div>';
          }
          list.innerHTML = rowsHtml + pager;
        }
        var series = (this.data && this.data.activity_tags_monthly) || [];
        var self = this;
        whenChart(function () {
          var el = document.getElementById('anTagsMonth');
          if (!el || !window.Chart) return;
          if (self.charts.anTagsMonth) self.charts.anTagsMonth.destroy();
          var dark = document.documentElement.getAttribute('data-theme') === 'dark';
          var base = baseOpts(dark);
          // denser ticks when many months
          base.scales.x.ticks.maxTicksLimit = Math.min(24, Math.max(6, Math.floor(series.length / 2) || 6));
          self.charts.anTagsMonth = new Chart(el, {
            type: 'bar',
            data: {
              labels: series.map(function (d) { return d.t; }),
              datasets: [
                { label: 'always', data: series.map(function (d) { return d.always_active; }), backgroundColor: '#30d158', stack: 't' },
                { label: 'returned', data: series.map(function (d) { return d.returned; }), backgroundColor: '#0a84ff', stack: 't' },
                { label: 'faded', data: series.map(function (d) { return d.faded; }), backgroundColor: '#ff453a', stack: 't' },
                { label: 'rare', data: series.map(function (d) { return d.rare; }), backgroundColor: '#8e8e93', stack: 't' },
                { label: 'new', data: series.map(function (d) { return d.new; }), backgroundColor: '#ff9f0a', stack: 't' }
              ]
            },
            options: Object.assign({}, base, {
              plugins: Object.assign({}, base.plugins, { legend: { display: true, align: 'end', labels: { boxWidth: 10, usePointStyle: true } } }),
              scales: Object.assign({}, base.scales, { x: Object.assign({}, base.scales.x, { stacked: true }), y: Object.assign({}, base.scales.y, { stacked: true }) })
            })
          });
        });
      },
      paint: function () {
        if (!this.data) return;
        var self = this;
        whenChart(function () {
          try {
            var dark = document.documentElement.getAttribute('data-theme') === 'dark';
            var base = baseOpts(dark);
            base.plugins.legend.display = false;
            Chart.defaults.color = dark ? '#a1a1a6' : '#86868b';
            function mk(id, cfg) {
              if (self.charts[id]) self.charts[id].destroy();
              var el = document.getElementById(id); if (!el) return;
              self.charts[id] = new Chart(el, cfg);
            }
            var msgs = self.series('messages'), users = self.series('users'), thr = self.series('threads'), rep = self.series('reply');
            mk('anMsgs', { type: 'line', data: { labels: msgs.map(function (d) { return d.t; }), datasets: [{ data: msgs.map(function (d) { return d.c; }), borderColor: '#0a84ff', backgroundColor: 'rgba(10,132,255,.12)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }] }, options: base });
            mk('anUsers', { type: 'bar', data: { labels: users.map(function (d) { return d.t; }), datasets: [{ data: users.map(function (d) { return d.c; }), backgroundColor: '#30d158', borderRadius: 3, maxBarThickness: 14 }] }, options: base });
            mk('anThreads', { type: 'line', data: { labels: thr.map(function (d) { return d.t; }), datasets: [
              { label: 'new', data: thr.map(function (d) { return d.c; }), borderColor: '#ff9f0a', tension: 0.3, pointRadius: 0, borderWidth: 2 },
              { label: 'cum', data: thr.map(function (d) { return d.cum; }), borderColor: '#64d2ff', borderDash: [4, 4], tension: 0.3, pointRadius: 0, borderWidth: 1.5, yAxisID: 'y1' }
            ] }, options: Object.assign({}, base, {
              plugins: Object.assign({}, base.plugins, { legend: { display: true, align: 'end', labels: { boxWidth: 10, usePointStyle: true } } }),
              scales: Object.assign({}, base.scales, { y1: { position: 'right', grid: { drawOnChartArea: false }, border: { display: false }, ticks: { maxTicksLimit: 4 } } })
            }) });
            var mobile = window.matchMedia && window.matchMedia('(max-width: 720px)').matches;
            if (!mobile) {
              mk('anReply', { type: 'line', data: { labels: rep.map(function (d) { return d.t; }), datasets: [{ data: rep.map(function (d) { return Math.round((d.reply_rate || 0) * 1000) / 10; }), borderColor: '#bf5af2', tension: 0.3, pointRadius: 0, borderWidth: 2 }] },
                options: Object.assign({}, base, { scales: Object.assign({}, base.scales, { y: Object.assign({}, base.scales.y, { ticks: { callback: function (v) { return v + '%'; } } }) }) }) });
            }
            var box = document.getElementById('anEmoji');
            if (box) {
              box.innerHTML = (self.data.top_emoji || []).slice(0, 16).map(function (e) {
                var img = e.emoji_url ? '<img src="' + e.emoji_url + '" alt="" style="width:18px;height:18px;object-fit:contain">' : '<span>' + e.emoji_name + '</span>';
                return '<span class="chip">' + img + '<span class="num">' + e.c + '</span></span>';
              }).join('');
            }
            self.renderActivityTags();
          } catch (err) {
            self.status = String(err.message || err);
            console.error('[dixo] paint', err);
          }
        });
      }
    };
  }

  function dashboardController() {
    return {
      mode: 'daily', range: 90, showVisitors: false, showBoard: false, board: [], page: 0, loading: false, done: false,
      openBoard: function () { this.showBoard = true; if (!this.page) this.more(); },
      more: function () {
        if (this.loading || this.done) return;
        this.loading = true;
        var self = this;
        fetch('/api/v1/leaderboard?page=' + (this.page + 1), { credentials: 'same-origin' })
          .then(function (r) { return r.json(); })
          .then(function (j) {
            var rows = (j && j.data) || [];
            self.board = self.board.concat(rows);
            self.page++;
            self.done = rows.length < 50;
            self.loading = false;
          })
          .catch(function () { self.loading = false; });
      },
      onScroll: function (el) { if (el.scrollTop + el.clientHeight >= el.scrollHeight - 80) this.more(); },
      setRange: function (n) { this.range = n; if (window.dixoCharts) window.dixoCharts.range(n); }
    };
  }

  // Register with Alpine if present; also expose globals for classic x-data="fn()"
  window.dashboard = dashboardController;
  window.analyticsPanel = analyticsController;
  window.dixoWhenChart = whenChart;

  document.addEventListener('alpine:init', function () {
    if (!window.Alpine) return;
    Alpine.data('dashboard', dashboardController);
    Alpine.data('analyticsPanel', analyticsController);
  });

  function bootStandaloneAnalytics() {
    // If Alpine never starts nested x-init, still fetch+paint once.
    var root = document.getElementById('analytics');
    if (!root || root.getAttribute('data-dixo-booted')) return;
    // Wait briefly for Alpine; if status never appears and no charts, force boot.
    setTimeout(function () {
      if (root.getAttribute('data-dixo-booted')) return;
      if (root._x_dataStack) return; // Alpine owns it
      root.setAttribute('data-dixo-booted', '1');
      var ctl = analyticsController();
      // bind minimal controls via query
      root.querySelectorAll('[data-grain]').forEach(function (btn) {
        btn.addEventListener('click', function () { ctl.setGrain(btn.getAttribute('data-grain')); });
      });
      root.querySelectorAll('[data-preset]').forEach(function (btn) {
        btn.addEventListener('click', function () { ctl.setPreset(btn.getAttribute('data-preset')); });
      });
      ctl.boot();
      window.__dixoAnalytics = ctl;
    }, 800);
  }

  function onReady() {
    initTrendCharts();
    bootStandaloneAnalytics();
    document.addEventListener('dixo:lang', function (e) {
      document.querySelectorAll('[data-wd-zh]').forEach(function (el) {
        el.textContent = el.dataset[e.detail === 'en' ? 'wdEn' : 'wdZh'];
      });
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', onReady, { once: true });
  else onReady();
})();
