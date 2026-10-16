/* Admin panel: guild/forum picker + live download-task cards. Uses window.dixoT / DIXO_I18N. */
(function () {
  "use strict";
  function T(k, zh) { return (window.dixoT ? window.dixoT(k, zh) : zh); }
  function esc(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  var CSRF = window.DIXO_ADMIN_CSRF || "";
  var resources = { guilds: [], forums: {} };

  function csrfField() {
    return '<input type="hidden" name="csrf_token" value="' + esc(CSRF) + '">';
  }

  function renderTaskActions(el, t) {
    var html = "";
    if (["pending", "running"].indexOf(t.status) >= 0) {
      html += '<form method="post" action="/admin/download-task/' + encodeURIComponent(t.id) + '/pause">' + csrfField() +
        '<button class="btn btn-warning" type="submit">' + esc(T("adminjs.pause", "暂停下载")) + "</button></form>";
      html += '<form method="post" action="/admin/download-task/' + encodeURIComponent(t.id) + '/cancel">' + csrfField() +
        '<button class="btn btn-ghost" type="submit">' + esc(T("adminjs.cancel", "取消任务")) + "</button></form>";
    } else if (t.status === "paused") {
      html += '<form method="post" action="/admin/download-task/' + encodeURIComponent(t.id) + '/resume">' + csrfField() +
        '<button class="btn btn-success" type="submit">' + esc(T("adminjs.resume", "继续下载")) + "</button></form>";
    }
    html += '<form method="post" action="/admin/download-task/' + encodeURIComponent(t.id) + '/delete" data-confirm-key="adminjs.delete_confirm" data-confirm-zh="' + esc(T("adminjs.delete_confirm", "删除后会彻底清空该下载任务的临时文件；如果该任务是当前服务器分析数据来源，还会删除对应分析数据库和用户访问记录。确定继续？")) + '">' +
      csrfField() + '<button class="btn btn-danger" type="submit">' + esc(T("adminjs.delete", "删除下载")) + "</button></form>";
    el.innerHTML = html;
  }

  function phaseLabel(t) {
    var map = {
      queued: T("adminjs.queued", "排队中"),
      scanning: T("adminjs.scanning", "扫描 Discord"),
      downloading: T("adminjs.downloading", "下载中"),
      importing: T("adminjs.importing", "实时写入数据库"),
      paused: T("adminjs.paused", "已暂停"),
      cancelled: T("adminjs.cancelled", "已取消"),
      completed: T("adminjs.completed", "已完成"),
      failed: T("adminjs.failed", "失败")
    };
    return map[t.phase] || map[t.status] || t.phase || t.status;
  }

  function formatDuration(sec) {
    sec = Math.max(0, Number(sec || 0));
    var h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = Math.floor(sec % 60);
    var H = T("adminjs.h", "小时"), M = T("adminjs.m", "分"), S = T("adminjs.s", "秒");
    if (h) return h + " " + H + " " + m + " " + M;
    if (m) return m + " " + M + " " + s + " " + S;
    return s + " " + S;
  }

  function formatEta(sec) {
    sec = Math.max(0, Number(sec || 0));
    if (!sec) return T("adminjs.calc", "计算中");
    var H = T("adminjs.h", "小时"), M = T("adminjs.m", "分"), S = T("adminjs.s", "秒");
    if (sec < 60) return Math.round(sec) + " " + S;
    var m = Math.floor(sec / 60), s = Math.round(sec % 60);
    if (m < 60) return m + " " + M + " " + s + " " + S;
    return Math.floor(m / 60) + " " + H + " " + (m % 60) + " " + M;
  }

  function updateTaskCard(t) {
    var e = document.querySelector('[data-task-id="' + t.id + '"]');
    if (!e) return;
    var percent = 0;
    if (t.phase === "scanning") {
      var denom = Math.max(Number(t.scan_total_estimate || 0), Number(t.scan_discovered || 0), Number(t.scan_processed || 0));
      percent = denom ? Math.min(95, Number(t.scan_processed || 0) / denom * 100) : 3;
    } else if (t.total) {
      percent = Math.min(100, Number(t.completed || 0) / Number(t.total) * 100);
    }
    var bar = e.querySelector(".task-progress");
    if (bar) bar.style.width = percent + "%";
    e.querySelector(".task-phase").textContent = phaseLabel(t);
    if (t.phase === "scanning") {
      e.querySelector(".task-count").textContent = (t.scan_processed || 0) + " " + T("adminjs.scanned", "已扫描");
    } else {
      e.querySelector(".task-count").textContent = (t.completed || 0) + "/" + (t.total || 0);
    }
    e.querySelector(".task-elapsed").textContent = formatDuration(t.elapsed_seconds);
    e.querySelector(".task-eta").textContent = t.phase === "scanning" ? T("adminjs.scan_now", "扫描中") : formatEta(t.estimated_seconds);
    e.querySelector(".task-scan").textContent =
      T("adminjs.scan", "扫描") + "：" + (t.scan_processed || 0) + " · " +
      T("adminjs.found", "已发现") + "：" + (t.scan_discovered || 0) + " · " +
      T("adminjs.active_bots", "活跃机器人") + "：" + (t.active_bots || 0);
    e.querySelector(".task-message").textContent = t.message || t.status;
    var err = e.querySelector(".task-error");
    if (err) err.textContent = t.error || "";
    var bots = e.querySelector(".task-bots");
    bots.innerHTML = (t.bot_progress || []).map(function (b) {
      return '<div><span class="truncate">' + esc(b.name) + '</span><span class="muted">' + esc(b.count) + "</span></div>";
    }).join("");
    var addBox = e.querySelector(".task-add-bot");
    var addSelect = e.querySelector(".task-add-bot-select");
    var addButton = e.querySelector(".task-add-bot-button");
    var available = t.available_bots || [];
    addSelect.innerHTML = '<option value="">' + esc(T("adminjs.pick_bot", "选择要加入的机器人")) + "</option>" +
      available.map(function (b) { return '<option value="' + esc(b.id) + '">' + esc(b.name) + "</option>"; }).join("");
    addBox.classList.toggle("hidden", ["pending", "running"].indexOf(t.status) < 0 || !available.length);
    addButton.disabled = !available.length;
    addButton.onclick = function () { addBotToTask(t.id, addSelect, addButton, addBox.querySelector(".task-add-bot-message")); };
    renderTaskActions(e.querySelector(".task-actions"), t);
  }

  async function addBotToTask(taskId, select, button, message) {
    var botId = select.value;
    if (!botId) { message.textContent = T("adminjs.pick_bot_first", "请先选择机器人"); return; }
    button.disabled = true;
    message.textContent = T("adminjs.verifying", "正在验证权限…");
    try {
      var body = new URLSearchParams({ csrf_token: CSRF, bot_id: botId });
      var r = await fetch("/admin/download-task/" + encodeURIComponent(taskId) + "/bot/add", {
        method: "POST",
        headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
        body: body
      });
      var data = await r.json();
      if (!r.ok || !data.ok) throw new Error(data.message || data.error || T("adminjs.add_failed", "加入机器人失败"));
      message.textContent = data.message || T("adminjs.added", "已加入，下载器将自动启动");
      setTimeout(refreshTasks, 500);
    } catch (err) {
      message.textContent = err.message || T("adminjs.add_failed", "加入机器人失败");
      button.disabled = false;
    }
  }

  async function refreshTasks() {
    try {
      var r = await fetch("/admin/download-tasks/status", { cache: "no-store" });
      var ts = await r.json();
      var active = false;
      ts.forEach(function (t) {
        updateTaskCard(t);
        active = active || ["pending", "running"].indexOf(t.status) >= 0 ||
          ["scanning", "downloading", "importing", "queued"].indexOf(t.phase) >= 0;
      });
      if (active) setTimeout(refreshTasks, 1000);
    } catch (e) {
      setTimeout(refreshTasks, 5000);
    }
  }

  function loadForums() {
    var guildId = document.querySelector("#guild").value;
    var guild = (resources.guilds || []).find(function (x) { return String(x.id) === String(guildId); });
    var icon = document.querySelector("#guildIcon");
    if (icon) {
      icon.src = (guild && guild.icon_url) || "https://cdn.discordapp.com/embed/avatars/0.png";
      icon.onerror = function () { icon.src = "https://cdn.discordapp.com/embed/avatars/0.png"; };
    }
    var fs = (resources.forums || {})[guildId] || [];
    var forum = document.querySelector("#forum");
    forum.innerHTML = fs.length
      ? fs.map(function (x) { return '<option value="' + esc(x.id) + '">' + esc(x.name) + "</option>"; }).join("")
      : '<option value="">' + esc(T("adminjs.no_forum", "没有可访问论坛频道")) + "</option>";
  }

  async function loadResources() {
    var g = document.querySelector("#guild");
    if (!g) return;
    try {
      var r = await fetch("/api/managed-discord-resources", { cache: "no-store" });
      resources = await r.json();
      g.innerHTML = '<option value="">' + esc(T("adminjs.pick_server", "选择服务器")) + "</option>" +
        (resources.guilds || []).map(function (x) {
          return '<option value="' + esc(x.id) + '">' + esc(x.name) + "</option>";
        }).join("");
      g.onchange = loadForums;
      if (resources.guilds && resources.guilds.length) {
        g.value = resources.guilds[0].id;
        loadForums();
      }
    } catch (e) {
      g.innerHTML = "<option>" + esc(T("adminjs.relogin", "请重新登录 Discord")) + "</option>";
    }
  }

  function bindConfirms() {
    document.addEventListener("submit", function (ev) {
      var form = ev.target;
      if (!(form instanceof HTMLFormElement)) return;
      var key = form.getAttribute("data-confirm-key");
      var zh = form.getAttribute("data-confirm-zh");
      if (!key && !zh) return;
      var msg = key ? T(key, zh || "") : zh;
      if (msg && !window.confirm(msg)) ev.preventDefault();
    });
  }

  function relabelStatic() {
    // Re-render dynamic task bits when language changes
    document.querySelectorAll("[data-task-id]").forEach(function (e) {
      renderTaskActions(e.querySelector(".task-actions"), { id: e.dataset.taskId, status: e.dataset.status });
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    bindConfirms();
    document.querySelectorAll("[data-task-id]").forEach(function (e) {
      renderTaskActions(e.querySelector(".task-actions"), { id: e.dataset.taskId, status: e.dataset.status });
    });
    loadResources();
    if (document.querySelector("[data-task-id]")) refreshTasks();
    document.addEventListener("dixo:lang", relabelStatic);
  });
})();

/* Cloudflare R2 backup admin panel (level-1). Appended into dixo-admin.js boot. */
(function () {
  "use strict";
  var box = document.getElementById("r2-status");
  if (!box) return;
  var CSRF = window.DIXO_ADMIN_CSRF || "";

  function esc(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function fmtUtc(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      if (isNaN(d.getTime())) return esc(iso);
      // Asia/Shanghai display
      return d.toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) + " CST";
    } catch (e) {
      return esc(iso);
    }
  }

  function badge(health) {
    var map = {
      ok: ["badge", "正常"],
      running: ["badge warn", "备份中"],
      failed: ["badge warn", "最近失败"],
      unconfigured: ["badge warn", "未配置"],
      unknown: ["badge", "未知"]
    };
    var x = map[health] || map.unknown;
    return '<span class="' + x[0] + '">' + esc(x[1]) + "</span>";
  }

  function kv(label, value) {
    return '<div class="admin-item"><div class="min0"><span class="faint">' + esc(label) +
      '</span><div style="margin-top:2px">' + value + "</div></div></div>";
  }

  function render(data) {
    if (!data) {
      box.innerHTML = '<div class="empty">无数据</div>';
      return;
    }
    var c = data.credentials || {};
    var log = data.log || {};
    var sch = data.schedule || {};
    var st = data.storage || {};
    var snaps = (data.snapshots && data.snapshots.snapshots) || [];
    var snapErr = data.snapshots && !data.snapshots.ok ? data.snapshots.error : null;

    var html = "";
    html += '<div class="row between" style="gap:8px;flex-wrap:wrap"><div class="row" style="gap:8px;align-items:center">' +
      badge(data.health) +
      '<span class="faint" style="font-size:12px">更新于 ' + esc(fmtUtc(data.generated_at)) +
      (data.cached ? " · 缓存" : "") + "</span></div></div>";

    html += '<div class="sub-card"><div class="card-sub" style="margin-bottom:8px">备份状态</div>';
    html += kv("上次成功", esc(fmtUtc(log.last_ok)) + (log.last_ok_duration_s != null ? ' <span class="faint">（' + esc(log.last_ok_duration_s) + "s）</span>" : ""));
    html += kv("上次开始", esc(fmtUtc(log.last_start)));
    html += kv("上次失败", log.last_fail ? (esc(fmtUtc(log.last_fail)) + (log.last_fail_detail ? ' <span class="faint">' + esc(log.last_fail_detail) + "</span>" : "")) : "—");
    html += kv("下次计划", esc(sch.next_run || sch.calendar || "—"));
    html += kv("systemd", 'timer=' + esc(sch.timer_active || "?") + ' · service=' + esc(sch.service_active || "?") +
      (sch.running ? ' <span class="badge warn">运行中</span>' : ""));
    html += kv("保留策略", esc(sch.prune_policy || ""));
    html += "</div>";

    html += '<div class="sub-card"><div class="card-sub" style="margin-bottom:8px">凭证（掩码）</div>';
    html += kv("Access Key ID", '<span class="mono">' + esc(c.access_key_id_masked || "未配置") + "</span>");
    html += kv("Secret Access Key", c.secret_configured ? '<span class="badge">已配置</span>' : '<span class="badge warn">未配置</span>');
    html += kv("restic 密码", c.restic_password_configured ? '<span class="badge">已配置</span>' : '<span class="badge warn">未配置</span>');
    html += kv("仓库", '<span class="mono" style="font-size:12px;word-break:break-all">' + esc(c.repository || "—") + "</span>");
    html += kv("桶 / Account", esc(c.bucket || "—") + ' · <span class="mono">' + esc(c.account_id || "—") + "</span>");
    html += kv("CF API Token", c.cf_api_token_configured ? '<span class="badge">已配置</span>' : '<span class="faint">未配置（用量走 S3/restic）</span>');
    html += "</div>";

    html += '<div class="sub-card"><div class="card-sub" style="margin-bottom:8px">Cloudflare 空间</div>';
    html += kv("已用", esc(st.used_human || "—") + (st.object_count != null ? ' <span class="faint">· ' + esc(st.object_count) + " 对象</span>" : ""));
    html += kv("剩余（相对免费额度）", esc(st.remaining_human || "—") + (st.used_pct != null ? ' <span class="faint">· 已用 ' + esc(st.used_pct) + "%</span>" : ""));
    html += kv("额度说明", esc(st.quota_label || "") + (st.note ? '<div class="hint">' + esc(st.note) + "</div>" : ""));
    if (st.used_pct != null) {
      var pct = Math.min(100, Math.max(0, Number(st.used_pct)));
      html += '<div style="height:8px;background:var(--hairline);border-radius:99px;overflow:hidden;margin-top:8px"><div style="height:100%;width:' +
        pct + '%;background:var(--color-primary)"></div></div>';
    }
    html += "</div>";

    html += '<div class="sub-card"><div class="card-sub" style="margin-bottom:8px">最近快照（' +
      esc((data.snapshots && data.snapshots.count) || snaps.length) + "）</div>";
    if (snapErr) {
      html += '<div class="empty">' + esc(snapErr) + "</div>";
    } else if (!snaps.length) {
      html += '<div class="empty">暂无快照</div>';
    } else {
      snaps.forEach(function (s) {
        html += '<div class="admin-item"><div class="min0"><b class="mono">' + esc(s.id) + "</b> · " +
          esc(fmtUtc(s.time)) + ' <span class="faint">' + esc((s.tags || []).join(", ")) +
          "</span></div></div>";
      });
    }
    html += "</div>";

    if (log.recent_lines && log.recent_lines.length) {
      html += '<details><summary style="cursor:pointer;font-weight:600">最近日志</summary><pre class="mono" style="font-size:11px;white-space:pre-wrap;max-height:220px;overflow:auto;margin-top:8px">' +
        esc(log.recent_lines.join("\n")) + "</pre></details>";
    }
    box.innerHTML = html;
  }

  async function load(force) {
    box.innerHTML = '<div class="empty">正在加载备份状态…</div>';
    try {
      var r = await fetch("/admin/r2-backup/status" + (force ? "?force=1" : ""), {
        cache: "no-store",
        credentials: "same-origin"
      });
      if (!r.ok) throw new Error("HTTP " + r.status);
      render(await r.json());
    } catch (e) {
      box.innerHTML = '<div class="empty">加载失败：' + esc(e.message || e) + "</div>";
    }
  }

  async function post(url, body) {
    var opts = {
      method: "POST",
      credentials: "same-origin",
      headers: { "X-CSRF-Token": CSRF, "Content-Type": "application/json" }
    };
    if (body !== undefined) opts.body = JSON.stringify(body);
    var r = await fetch(url, opts);
    var data = null;
    try { data = await r.json(); } catch (e) { data = { ok: false, error: "HTTP " + r.status }; }
    return { status: r.status, data: data };
  }

  var btnRefresh = document.getElementById("r2-refresh");
  var btnTest = document.getElementById("r2-test");
  var btnRun = document.getElementById("r2-run");
  var form = document.getElementById("r2-creds-form");

  if (btnRefresh) btnRefresh.addEventListener("click", function () { load(true); });
  if (btnTest) btnTest.addEventListener("click", async function () {
    btnTest.disabled = true;
    try {
      var res = await post("/admin/r2-backup/test", {});
      if (res.data && res.data.ok) {
        var t = res.data.latest ? (res.data.latest.id + " @ " + (res.data.latest.time || "")) : "无快照";
        alert("凭证可用。最新快照：" + t);
      } else {
        alert("测试失败：" + ((res.data && res.data.error) || res.status));
      }
    } finally {
      btnTest.disabled = false;
      load(true);
    }
  });
  if (btnRun) btnRun.addEventListener("click", async function () {
    if (!confirm("确定立刻开始一次数据库备份？可能需要数分钟。")) return;
    btnRun.disabled = true;
    try {
      var res = await post("/admin/r2-backup/run", {});
      if (res.data && res.data.ok) alert(res.data.message || "已触发");
      else alert("触发失败：" + ((res.data && res.data.error) || res.status));
    } finally {
      btnRun.disabled = false;
      load(true);
    }
  });
  if (form) form.addEventListener("submit", async function (ev) {
    ev.preventDefault();
    if (!confirm("确定写入新的 R2 凭证？旧密钥会被覆盖。")) return;
    var fd = new FormData(form);
    var body = {
      access_key_id: fd.get("access_key_id"),
      secret_access_key: fd.get("secret_access_key"),
      restic_password: fd.get("restic_password") || "",
      cf_api_token: fd.get("cf_api_token") || ""
    };
    var res = await post("/admin/r2-backup/creds", body);
    if (res.data && res.data.ok) {
      alert("已保存：" + (res.data.access_key_id_masked || ""));
      form.reset();
      load(true);
    } else {
      alert("保存失败：" + ((res.data && res.data.error) || res.status));
    }
  });

  load(false);
})();

/* ---- 服务器同步卡片 ---- */
(function () {
  var box = document.getElementById("guild-sync-list");
  if (!box) return;
  var CSRF = window.DIXO_ADMIN_CSRF || "";
  var LABEL = { backfilling: ["回填历史", "#3b82f6"], live: ["实时同步", "#16a34a"], retrying: ["重试中", "#f59e0b"], paused: ["已暂停", "#6b7280"], error: ["错误", "#dc2626"] };
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function fmt(t) { if (!t) return "—"; var d = new Date(t); if (isNaN(d)) return esc(t); return d.toLocaleString("zh-CN", { hour12: false, timeZone: "Asia/Shanghai" }); }
  function ago(t) { if (!t) return "—"; var s = (Date.now() - new Date(t).getTime()) / 1000; if (isNaN(s)) return "—"; if (s < 60) return Math.round(s) + " 秒前"; if (s < 3600) return Math.round(s / 60) + " 分钟前"; return Math.round(s / 3600) + " 小时前"; }
  function n(x) { return (x || 0).toLocaleString("zh-CN"); }
  function card(g) {
    var st = LABEL[g.state] || [g.state || "?", "#6b7280"];
    var running = g.run_status === "running";
    var phase = g.run_kind === "backfill" ? "历史回填" : (g.run_kind === "live" ? "增量扫描" : "等待下一轮");
    if (g.run_phase) phase += " · " + esc(g.run_phase);
    var oldest = g.backfill_done ? "已到最早（" + fmt(g.backfill_done_at) + " 完成）" : (g.oldest_reached || g.run_cursor ? fmt(g.run_cursor || g.oldest_reached) : "—");
    var rows = [
      ["当前阶段", phase],
      ["本轮进度", "已扫描 " + n(g.scan_processed) + " · 已下载 " + n(g.run_completed) + "/" + n(g.run_total)],
      ["已回填到", oldest],
      ["累计导入", n(g.threads_done) + " 帖 · " + n(g.messages_ingested) + " 条消息"],
      ["速率", (g.recent_speed ? (+g.recent_speed).toFixed(1) : "0") + " 帖/分钟 · 机器人 " + (g.active_bots || 0)],
      ["最后心跳", running ? ago(g.run_heartbeat) + "（" + fmt(g.run_heartbeat) + "）" : fmt(g.updated_at)],
      ["重试", (g.retry_count || 0) + " 次" + (g.next_retry_at ? " · 下次 " + fmt(g.next_retry_at) : "")],
      ["下次增量", g.backfill_done ? fmt(g.next_live_at) : "回填完成后开始"]
    ];
    var h = '<div class="admin-task" style="border-left:4px solid ' + st[1] + '">';
    h += '<div class="row between" style="flex-wrap:wrap;gap:8px"><div><b>' + esc(g.server_display_name) + '</b> <span class="muted">' + esc(g.forum_display_name) + '</span> ' +
      '<span style="background:' + st[1] + ';color:#fff;border-radius:999px;padding:2px 10px;font-size:12px;margin-left:6px">' + st[0] + '</span></div>';
    h += '<div class="row" style="gap:6px">' +
      (g.paused ? '<button class="btn btn-primary" data-act="resume" data-id="' + g.id + '">继续</button>' : '<button class="btn btn-ghost" data-act="pause" data-id="' + g.id + '">暂停</button>') +
      '<button class="btn btn-ghost" data-act="restart" data-id="' + g.id + '">从断点重启</button></div></div>';
    h += '<div class="stats4" style="grid-template-columns:repeat(auto-fit,minmax(200px,1fr));margin-top:10px">';
    rows.forEach(function (r) { h += '<div><div class="muted">' + r[0] + '</div><b style="font-weight:600">' + r[1] + '</b></div>'; });
    h += "</div>";
    if (g.run_message) h += '<div class="faint" style="font-size:12px;margin-top:8px;word-break:break-word">' + esc(g.run_message) + "</div>";
    if (g.last_error) h += '<div class="task-error" style="margin-top:6px">最近错误：' + esc(g.last_error) + "</div>";
    return h + "</div>";
  }
  async function load() {
    try {
      var r = await fetch("/admin/guild-sync/status", { cache: "no-store", credentials: "same-origin" });
      if (!r.ok) throw new Error("HTTP " + r.status);
      var d = await r.json();
      box.innerHTML = (d.syncs || []).length ? d.syncs.map(card).join("") : '<div class="empty">还没有启用的服务器配置。添加下载配置后会自动开始回填 + 实时同步。</div>';
    } catch (e) { box.innerHTML = '<div class="empty">加载失败：' + esc(e.message || e) + "</div>"; }
  }
  box.addEventListener("click", async function (ev) {
    var b = ev.target.closest("button[data-act]"); if (!b) return;
    if (b.dataset.act === "restart" && !confirm("从断点重启这个服务器同步？")) return;
    b.disabled = true;
    await fetch("/admin/guild-sync/" + b.dataset.id + "/" + b.dataset.act, { method: "POST", credentials: "same-origin", headers: { "X-CSRF-Token": CSRF } });
    load();
  });
  load(); setInterval(load, 5000);
})();
