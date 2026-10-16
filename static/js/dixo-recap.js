/* 服务器总结 / 个人总结：周期选择 + 故事卡片。数据来自 /api/v1/recap/(server|me)，后台生成时轮询。 */
(function () {
  var root = document.getElementById("recap"); if (!root) return;
  var scope = root.dataset.scope, target = root.dataset.user, body = document.getElementById("rc-body");
  var years = (root.dataset.years || "").split(",").filter(Boolean);
  var $ = function (id) { return document.getElementById(id); };
  var st = { k: "year", y: years[0] || String(new Date().getFullYear()), m: null, w: null }, seq = 0, timer = null;
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function n(x) { return (x || 0).toLocaleString("zh-CN"); }
  function delta(p) { if (p == null) return ""; return '<span class="rc-delta ' + (p >= 0 ? "up" : "down") + '">' + (p >= 0 ? "▲ " : "▼ ") + Math.abs(p) + "%</span>"; }
  function av(u, s) { s = s || 32; return '<img class="avatar" width="' + s + '" height="' + s + '" style="width:' + s + 'px;height:' + s + 'px;border-radius:50%" src="' + esc((u && u.avatar_url) || "https://cdn.discordapp.com/embed/avatars/0.png") + '" alt="" loading="lazy" onerror="this.src=\'https://cdn.discordapp.com/embed/avatars/0.png\'">'; }
  function ulink(u) { return '<a class="t" href="/user/' + esc(u.user_id) + '">' + esc(u.name) + "</a>"; }
  function emo(e) { return '<span class="rc-emo">' + (e.emoji_url ? '<img src="' + esc(e.emoji_url) + '" alt="">' : esc(e.emoji_name)) + " " + n(e.count) + "</span>"; }
  function card(cls, title, html, i) { return '<section class="rc-story ' + (cls || "") + '" style="animation-delay:' + (i || 0) * 60 + 'ms"><h3>' + title + "</h3>" + html + "</section>"; }
  function hours(h, pk) { var mx = Math.max.apply(null, h.concat([1])); return '<div class="rc-hours">' + h.map(function (v, i) { return '<i class="' + (i === pk ? "pk" : "") + '" title="' + i + ' 点 · ' + n(v) + ' 条" style="height:' + Math.max(2, v * 100 / mx) + '%"></i>'; }).join("") + '</div><div class="row between faint" style="font-size:11px;margin-top:4px"><span>0</span><span>6</span><span>12</span><span>18</span><span>23</span></div>'; }
  function words(ws) { if (!ws || !ws.length) return '<div class="faint">暂无</div>'; var mx = ws[0].count || 1; return '<div class="rc-words">' + ws.map(function (w) { return '<span style="font-size:' + (13 + 15 * w.count / mx).toFixed(0) + 'px;opacity:' + (0.55 + 0.45 * w.count / mx).toFixed(2) + '">' + esc(w.word || w.term) + "</span>"; }).join("") + "</div>"; }
  function quote(m, who) {
    return '<a class="rc-quote" style="display:block;color:inherit;text-decoration:none" href="https://discord.com/channels/' + esc(root.dataset.gid || "@me") + "/" + esc(m.thread_id) + "/" + esc(m.message_id) + '" target="_blank" rel="noopener"><div class="c">' + esc(m.content || "（图片 / 附件）") + '</div><div class="m">' + (who || "") + (m.emojis || []).map(emo).join("") + '<span>❤️ ' + n(m.reactions) + "</span></div></a>";
  }
  function users(list, val) { if (!list || !list.length) return '<div class="faint">暂无</div>'; return '<ul class="rc-list">' + list.map(function (u, i) { return '<li><span class="n">' + (i + 1) + "</span>" + av(u) + '<div class="grow">' + ulink(u) + '<span class="s">' + val(u) + "</span></div></li>"; }).join("") + "</ul>"; }
  function threads(list) { if (!list || !list.length) return '<div class="faint">暂无</div>'; return '<ul class="rc-list">' + list.map(function (t, i) { return '<li><span class="n">' + (i + 1) + '</span><div class="grow"><span class="t">' + esc(t.name) + '</span><span class="s">' + n(t.messages) + " 条" + (t.people ? " · " + n(t.people) + " 人" : "") + (t.span_days ? " · 持续 " + t.span_days + " 天" : "") + "</span></div></li>"; }).join("") + "</ul>"; }

  function renderServer(d) {
    var t = d.totals || {}, i = 0, h = [];
    h.push(card("w12", "总览", '<div class="rc-kpis">' +
      '<div><div class="v">' + n(t.messages) + delta(t.messages_change) + '</div><div class="l">条消息</div></div>' +
      '<div><div class="v">' + n(t.active_members) + delta(t.active_change) + '</div><div class="l">位活跃成员</div></div>' +
      '<div><div class="v">' + n(t.threads_new) + delta(t.threads_change) + '</div><div class="l">个新帖子</div></div>' +
      '<div><div class="v">' + n(t.reactions) + '</div><div class="l">个表情反应</div></div>' +
      '<div><div class="v">' + n(t.newcomers) + '</div><div class="l">位新面孔</div></div></div>', i++));
    h.push(card("w4", "最热的一天", d.peak_day ? '<div class="rc-big">' + esc(d.peak_day.day.slice(5).replace("-", "/")) + '</div><p class="muted">' + esc(d.peak_day.day.slice(0, 4)) + " · " + n(d.peak_day.messages) + " 条消息</p>" : '<div class="faint">暂无</div>', i++));
    h.push(card("w8", "一天里最热闹的时刻", (d.peak_hour ? '<div class="rc-big">' + d.peak_hour.hour + ':00</div>' : "") + hours(d.hours || [], d.peak_hour && d.peak_hour.hour), i++));
    h.push(card("", "🔥 热帖", threads(d.hot_threads), i++));
    h.push(card("", "💬 热评", (d.hot_messages || []).slice(0, 4).map(function (m) { return quote(m, av(m.author, 18) + "<b>" + esc(m.author.name) + "</b>"); }).join("") || '<div class="faint">暂无</div>', i++));
    h.push(card("w4", "🏆 热门成员", users(d.top_posters, function (u) { return n(u.messages) + " 条 · " + u.share + "%"; }), i++));
    h.push(card("w4", "🚀 上升最快", users(d.rising, function (u) { return "+" + n(u.delta) + " 条（共 " + n(u.messages) + "）"; }), i++));
    h.push(card("w4", "👋 新面孔", users(d.newcomers, function (u) { return "首次发言 " + esc(u.first_day) + " · " + n(u.messages) + " 条"; }), i++));
    if ((d.superlatives || []).length) h.push(card("w12", "✨ 本期之最", '<div class="rc-sup">' + d.superlatives.map(function (s) { return '<div class="rc-x"><span class="eyebrow">' + esc(s.title) + "</span><b>" + (s.user ? ulink(s.user) : esc(s.thread.name)) + '</b><span class="muted" style="font-size:13px">' + n(s.value) + " " + esc(s.unit) + "</span></div>"; }).join("") + "</div>", i++));
    h.push(card("", "😀 最常用的表情", (d.top_emojis || []).map(emo).join("") || '<div class="faint">暂无</div>', i++));
    h.push(card("", "🧵 最会开帖的人", users(d.starters, function (u) { return "开了 " + n(u.threads) + " 个帖子"; }), i++));
    h.push(card("", "📜 最长的对话", threads(d.longest_threads), i++));
    var ms = d.member_changes;
    h.push(card("", "👥 成员变化", ms ? '<div class="rc-kpis"><div><div class="v" style="color:var(--green)">+' + n(ms.joined) + '</div><div class="l">加入</div></div><div><div class="v" style="color:var(--orange)">' + n(ms.left) + '</div><div class="l">退群</div></div><div><div class="v" style="color:var(--muted)">' + n(ms.deleted) + '</div><div class="l">已销号（当前）</div></div></div><p class="faint" style="font-size:12px;margin-top:12px">按记录到的加入 / 退群时间统计</p>' : '<div class="faint">暂无成员状态数据</div>', i++));
    h.push(card("", "🎭 群梗热度", (d.memes || []).length ? '<ul class="rc-list">' + d.memes.slice(0, 6).map(function (m, k) { return '<li><span class="n">' + (k + 1) + '</span><div class="grow"><span class="t">' + esc(m.term) + '</span><span class="s">' + esc(m.meaning || "") + '</span></div><span class="chip num">' + n(m.count) + "</span></li>"; }).join("") + "</ul>" : '<div class="faint">洞察数据未生成</div>', i++));
    h.push(card("", "🔤 高频词", words(d.words), i++));
    body.innerHTML = h.join("");
    $("rc-sub").textContent = d.period.label + " · " + n(t.messages) + " 条消息 · " + n(t.active_members) + " 位成员";
  }

  function renderMe(d) {
    var i = 0, h = [];
    $("rc-title").textContent = (d.user && d.user.name) || "我的总结";
    h.push(card("w12", "这段时间的你", '<div class="rc-kpis">' +
      '<div><div class="v">' + n(d.messages) + delta(d.messages_change) + '</div><div class="l">条消息</div></div>' +
      '<div><div class="v">' + (d.rank ? "#" + d.rank : "—") + '</div><div class="l">排名（共 ' + n(d.ranked_of) + ' 人）</div></div>' +
      '<div><div class="v">' + n(d.active_days) + '</div><div class="l">个活跃日' + (d.active_days_prev != null ? "（上期 " + n(d.active_days_prev) + "）" : "") + '</div></div>' +
      '<div><div class="v">' + n(d.longest_streak) + '</div><div class="l">天最长连续发言</div></div>' +
      '<div><div class="v">' + d.share + '%</div><div class="l">全服消息占比</div></div></div>', i++));
    h.push(card("w8", "你最活跃的时刻", (d.peak_hour != null ? '<div class="rc-big">' + d.peak_hour + ':00</div>' : "") + hours(d.hours || [], d.peak_hour), i++));
    var wdn = ["一", "二", "三", "四", "五", "六", "日"], wd = d.weekdays || [], wm = wd.indexOf(Math.max.apply(null, wd.concat([0])));
    h.push(card("w4", "最爱的日子", wd.some(Boolean) ? '<div class="rc-big">周' + wdn[wm] + '</div><p class="muted">' + n(wd[wm]) + " 条消息</p>" : '<div class="faint">暂无</div>', i++));
    h.push(card("", "🧵 你最常去的帖子", threads(d.top_threads), i++));
    h.push(card("", "❤️ 你最火的发言", (d.hot_messages || []).slice(0, 3).map(function (m) { return quote(m, m.thread_name ? "<span># " + esc(m.thread_name) + "</span>" : ""); }).join("") || '<div class="faint">暂无</div>', i++));
    h.push(card("", "收到的表情 · " + n(d.reactions_received), (d.top_received || []).map(emo).join("") || '<div class="faint">暂无</div>', i++));
    h.push(card("", "送出的表情 · " + n(d.reactions_given), (d.top_given || []).map(emo).join("") || '<div class="faint">暂无</div>', i++));
    h.push(card("", "🤝 互动最多的人", users(d.partners, function (u) { return "互动指数 " + n(u.score); }), i++));
    h.push(card("", "🎭 你用过的群梗", (d.memes || []).length ? (d.memes || []).map(function (m) { return '<span class="rc-emo">' + esc(m.term) + " · " + n(m.count) + "</span>"; }).join("") : '<div class="faint">暂无</div>', i++));
    h.push(card("w12", "🔤 你的高频词", words(d.words), i++));
    body.innerHTML = h.join("");
    $("rc-sub").textContent = d.period.label + (d.first_day ? " · " + d.first_day + " 至 " + d.last_day : " · 这段时间还没有发言");
  }

  function params() {
    var v = st.k === "year" ? st.y : st.k === "month" ? st.m : st.k === "week" ? st.w : "all";
    var q = "p=" + st.k + (v ? "&v=" + encodeURIComponent(v) : "");
    return target ? q + "&user=" + encodeURIComponent(target) : q;
  }
  function load() {
    var my = ++seq; clearTimeout(timer);
    $("rc-status").textContent = "加载中…";
    history.replaceState(null, "", location.pathname + "?" + params());
    fetch("/api/v1/recap/" + (scope === "server" ? "server" : "me") + "?" + params(), { credentials: "same-origin" })
      .then(function (r) { return r.json(); }).then(function (j) {
        if (my !== seq) return;
        if (j.state === "building") { $("rc-status").textContent = "正在生成，第一次会慢一点…"; timer = setTimeout(load, 2500); return; }
        if (!j.ok) { $("rc-status").textContent = "生成失败：" + (j.error || ""); return; }
        (scope === "server" ? renderServer : renderMe)(j.data);
        $("rc-status").textContent = "生成于 " + String(j.data.generated_at || "").replace("T", " ").slice(0, 16);
      }).catch(function (e) { if (my === seq) $("rc-status").textContent = "加载失败：" + e; });
  }
  // pickers
  var ysel = $("rc-year"), msel = $("rc-month"), wk = $("rc-week");
  ysel.innerHTML = years.map(function (y) { return '<option value="' + y + '">' + y + " 年</option>"; }).join("");
  function fillMonths() { var y = +st.y, now = new Date(), opts = []; for (var m = 12; m >= 1; m--) { if (y === now.getFullYear() && m > now.getMonth() + 1) continue; var v = y + "-" + (m < 10 ? "0" : "") + m; opts.push('<option value="' + v + '">' + m + " 月</option>"); } msel.innerHTML = opts.join(""); st.m = msel.value; }
  function isoWeek(d) { var t = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())); var day = t.getUTCDay() || 7; t.setUTCDate(t.getUTCDate() + 4 - day); var y0 = new Date(Date.UTC(t.getUTCFullYear(), 0, 1)); var w = Math.ceil(((t - y0) / 86400000 + 1) / 7); return t.getUTCFullYear() + "-W" + (w < 10 ? "0" : "") + w; }
  var qs = new URLSearchParams(location.search);
  if (qs.get("p")) { st.k = qs.get("p"); var v = qs.get("v") || ""; if (st.k === "year" && v) st.y = v; if (st.k === "month" && v) { st.y = v.slice(0, 4); st.m = v; } if (st.k === "week" && v) st.w = v; }
  if (years.indexOf(st.y) < 0 && years.length) st.y = years[0];
  ysel.value = st.y; fillMonths(); if (qs.get("p") === "month" && qs.get("v")) msel.value = st.m = qs.get("v");
  wk.value = st.w = st.w || isoWeek(new Date());
  function sync() {
    document.querySelectorAll("#rc-kind button").forEach(function (b) { b.classList.toggle("on", b.dataset.k === st.k); });
    ysel.hidden = !(st.k === "year" || st.k === "month"); msel.hidden = st.k !== "month"; wk.hidden = st.k !== "week";
  }
  document.getElementById("rc-kind").addEventListener("click", function (e) { var b = e.target.closest("button"); if (!b) return; st.k = b.dataset.k; sync(); load(); });
  ysel.addEventListener("change", function () { st.y = ysel.value; fillMonths(); load(); });
  msel.addEventListener("change", function () { st.m = msel.value; load(); });
  wk.addEventListener("change", function () { if (wk.value) { st.w = wk.value; load(); } });
  sync(); load();
})();
