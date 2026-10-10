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
