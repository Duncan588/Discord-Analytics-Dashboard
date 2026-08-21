/**
 * Discord Activities 登录：只在 Discord iframe 中运行。
 *
 * 这里不能对 Discord 的 429 进行立即重试：authorize() 返回的 code 是一次性的，
 * 而连续 authorize 会继续消耗配额。遇到 retry_after 时记录冷却时间并显示倒计时，
 * 冷却结束后由用户重新发起一次授权。
 */
(function () {
  "use strict";

  var AUTHED_KEY = "discordActivityAuthed";
  var IN_FLIGHT_KEY = "discordActivityAuthInFlight";
  var RATE_LIMIT_KEY = "discordActivityRateLimitUntil";
  var IN_FLIGHT_MAX_AGE = 10 * 60 * 1000;
  // 卡在"正在登陆中"的自愈等待时间：如果上一次尝试是因为移动端 WebView 被挂起
  // （切后台/锁屏）而中断，从未真正完成，就不应该让用户干等到 10 分钟锁过期。
  var IN_FLIGHT_RETRY_MS = 8000;
  var clientRequestId = activityState();
  var sdkPromise;
  var errorBox;
  var countdownTimer;
  var AUTHENTICATE_TIMEOUT_MS = 2500;
  // sdk.ready() / authorize() 之前完全没有超时：Discord 客户端与小活动 iframe 的
  // RPC 桥接一旦某次不回包（弱网/移动端常见），Promise 永远不 resolve，页面就会
  // 卡死在加载动画上且没有任何报错。这里给它们各自设置超时并显式失败，
  // 让用户能看到错误提示并重试，而不是无限转圈。
  var SDK_READY_TIMEOUT_MS = 6000;
  var AUTHORIZE_TIMEOUT_MS = 8000;

  function rejectAfterTimeout(promise, timeoutMs, message) {
    return new Promise(function (resolve, reject) {
      var settled = false;
      var timer = window.setTimeout(function () {
        if (settled) return;
        settled = true;
        var err = new Error(message);
        err.timedOut = true;
        reject(err);
      }, timeoutMs);
      promise.then(function (value) {
        if (settled) return;
        settled = true;
        window.clearTimeout(timer);
        resolve(value);
      }, function (err) {
        if (settled) return;
        settled = true;
        window.clearTimeout(timer);
        reject(err);
      });
    });
  }

  function isInsideDiscordActivity() {
    try {
      var params = new URLSearchParams(window.location.search);
      if (params.has("frame_id") || params.has("instance_id")) return true;
    } catch (e) { /* ignore */ }
    try {
      return window.self !== window.top;
    } catch (e) {
      return true;
    }
  }

  function showActivityLoginState() {
    var action = document.getElementById("web-login-action");
    var status = document.getElementById("activity-login-status");
    var container = action && action.parentNode;
    if (container) container.removeChild(action);
    if (!status) {
      status = document.createElement("div");
      status.id = "activity-login-status";
      status.className = "text-white";
      status.setAttribute("aria-live", "polite");
      status.innerHTML = '<div class="flex items-center justify-center gap-3 text-xl font-bold"><span class="inline-block w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin"></span>正在登陆中</div><p class="text-gray-400 text-sm mt-3">Discord 小活动正在自动完成身份验证，请稍候。</p>';
      if (container) container.appendChild(status);
    }
    if (status) status.classList.remove("hidden");
  }

  // ---------------------------------------------------------------------
  // 外部链接（如 https://discord.com/channels/...）在 Activity 沙盒 iframe 里
  // 用普通 <a target="_blank"> 打不开，必须走 SDK 的 OPEN_EXTERNAL_LINK 命令
  // 交给 Discord 客户端在 iframe 之外打开。这里用事件委托而不是给每个链接
  // 绑定监听器，这样列表页动态插入的卡片也能被拦截到；SDK 只在真正点到
  // 外部链接时才懒加载，不占用首屏加载时间。
  // ---------------------------------------------------------------------
  var linkSdkPromise = null;

  function getLinkSdk() {
    if (!linkSdkPromise) {
      var clientIdMeta = document.querySelector('meta[name="discord-client-id"]');
      var clientId = clientIdMeta ? clientIdMeta.getAttribute("content") : "";
      linkSdkPromise = import("/static/vendor/discord-embedded-app-sdk/discord-activity-sdk.mjs?v=20260821-2")
        .then(function (mod) {
          var sdk = new mod.DiscordSDK(clientId);
          return rejectAfterTimeout(sdk.ready(), SDK_READY_TIMEOUT_MS, "Discord 客户端没有响应（ready 超时）").then(function () {
            return sdk;
          });
        });
      linkSdkPromise.catch(function () {
        // 让下一次点击可以重新尝试加载/握手，而不是被这次失败永久卡住。
        linkSdkPromise = null;
      });
    }
    return linkSdkPromise;
  }

  function isExternalHref(href) {
    try {
      var url = new URL(href, window.location.href);
      return url.origin !== window.location.origin;
    } catch (e) {
      return false;
    }
  }

  function handleExternalLinkClick(event) {
    if (event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    var link = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!link) return;
    var rawHref = link.getAttribute("href") || "";
    if (!rawHref || rawHref.charAt(0) === "#") return;
    if (rawHref.indexOf("javascript:") === 0 || rawHref.indexOf("mailto:") === 0 || rawHref.indexOf("tel:") === 0) return;
    if (!isExternalHref(link.href)) return;
    event.preventDefault();
    var absoluteUrl = link.href;
    getLinkSdk().then(function (sdk) {
      return sdk.commands.openExternalLink({ url: absoluteUrl });
    }).catch(function (err) {
      sendLog("link.open_external.failure", { error: safeError(err) });
      console.warn("[discord-activity] 打开外部链接失败，回退到默认行为：", err);
      window.open(absoluteUrl, "_blank", "noopener,noreferrer");
    });
  }

  function setupExternalLinkInterception() {
    if (!isInsideDiscordActivity()) return;
    document.addEventListener("click", handleExternalLinkClick, true);
  }

  function keepReportInsideActivity() {
    if (!isInsideDiscordActivity()) return;
    document.querySelectorAll('a[data-report-link], a[href^="/report"]').forEach(function (link) {
      // A new top-level tab does not share the Discord Activity's partitioned
      // session cookie. Keep the report in the authenticated Activity iframe.
      link.removeAttribute("target");
      link.removeAttribute("rel");
    });
  }

  function openActivityLoginFirst() {
    if (document.body.getAttribute("data-activity-landing") !== "welcome") return false;
    // Keep Discord's frame parameters so login.html can initialize the SDK.
    window.location.replace("/login" + window.location.search + window.location.hash);
    return true;
  }

  function hasSdkQueryParams() {
    try {
      var params = new URLSearchParams(window.location.search);
      return Boolean(params.get("frame_id") && params.get("instance_id") && params.get("platform"));
    } catch (e) {
      return false;
    }
  }

  function activityState() {
    try {
      if (window.crypto && typeof window.crypto.randomUUID === "function") {
        return window.crypto.randomUUID();
      }
    } catch (e) { /* ignore */ }
    return "discord-activity-" + Date.now() + "-" + Math.random().toString(36).slice(2);
  }

  function safeError(error) {
    if (!error) return { message: "unknown" };
    var result = {};
    if (typeof error === "string") {
      result.message = error.slice(0, 300);
    } else {
      if (error.message) result.message = String(error.message).slice(0, 300);
      if (error.name) result.name = String(error.name).slice(0, 80);
      if (error.status) result.status = Number(error.status) || String(error.status).slice(0, 30);
      if (error.retry_after != null) result.retry_after = Number(error.retry_after) || 0;
      if (error.global != null) result.global = Boolean(error.global);
      if (error.rate_limited != null) result.rate_limited = Boolean(error.rate_limited);
      if (error.stage) result.stage = String(error.stage).slice(0, 80);
    }
    return result;
  }

  function sendLog(event, details) {
    try {
      var payload = JSON.stringify({ event: event, request_id: clientRequestId, details: details || {} });
      var blob = new Blob([payload], { type: "application/json" });
      if (navigator.sendBeacon && navigator.sendBeacon("/api/activity/log", blob)) return;
      fetch("/api/activity/log", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Request-ID": clientRequestId },
        body: payload,
        keepalive: true,
      }).catch(function () { /* 诊断日志不能影响登录流程 */ });
    } catch (e) { /* 诊断日志不能影响登录流程 */ }
  }

  function getRetryAfter(value) {
    var seconds = Number(value);
    return isFinite(seconds) && seconds > 0 ? Math.ceil(Math.min(seconds, 86400)) : 0;
  }

  function currentCooldownSeconds() {
    var until = Number(sessionStorage.getItem(RATE_LIMIT_KEY) || 0);
    try {
      until = Math.max(until, Number(localStorage.getItem(RATE_LIMIT_KEY) || 0));
    } catch (e) { /* 某些 iframe 环境会禁用 localStorage */ }
    return until > Date.now() ? Math.ceil((until - Date.now()) / 1000) : 0;
  }

  function setRateLimitCooldown(seconds) {
    seconds = getRetryAfter(seconds);
    if (seconds > 0) {
      var until = String(Date.now() + seconds * 1000);
      sessionStorage.setItem(RATE_LIMIT_KEY, until);
      try {
        // localStorage 让 Discord 客户端重建 iframe 后仍能遵守同一冷却时间。
        localStorage.setItem(RATE_LIMIT_KEY, until);
      } catch (e) { /* 某些 iframe 环境会禁用 localStorage */ }
    }
    return seconds;
  }

  function formatWait(seconds) {
    if (seconds >= 60) {
      var minutes = Math.floor(seconds / 60);
      var remaining = seconds % 60;
      return minutes + " 分钟" + (remaining ? " " + remaining + " 秒" : "");
    }
    return seconds + " 秒";
  }

  function removeErrorBox() {
    if (countdownTimer) clearInterval(countdownTimer);
    countdownTimer = null;
    if (errorBox && errorBox.parentNode) errorBox.parentNode.removeChild(errorBox);
    errorBox = null;
  }

  function showActivityError(message, retryAfter) {
    removeErrorBox();
    var seconds = getRetryAfter(retryAfter) || currentCooldownSeconds();
    errorBox = document.createElement("div");
    errorBox.style.cssText = "position:fixed;left:16px;right:16px;bottom:16px;z-index:99999;padding:14px 16px;border-radius:12px;background:#202225;color:#fff;border:1px solid #ed4245;font:14px/1.5 Arial,'Microsoft YaHei',sans-serif;box-shadow:0 10px 30px rgba(0,0,0,.35)";
    var text = document.createElement("div");
    errorBox.appendChild(text);
    document.body.appendChild(errorBox);

    function update() {
      var remaining = currentCooldownSeconds();
      if (remaining > 0) {
        text.textContent = message + " 已暂停授权请求，请等待约 " + formatWait(remaining) + "。";
      } else {
        sessionStorage.removeItem(RATE_LIMIT_KEY);
        try { localStorage.removeItem(RATE_LIMIT_KEY); } catch (e) { /* ignore */ }
        text.textContent = message + " 可以重新尝试。";
      }
    }
    update();
    if (seconds > 0) countdownTimer = setInterval(update, 1000);
  }

  function showRateLimit(source, error) {
    var info = safeError(error);
    // Discord 偶尔只返回限流消息而不带 retry_after；此时也要留出冷却时间，
    // 防止用户反复点击继续触发同一限流。
    var seconds = setRateLimitCooldown(info.retry_after || 60);
    sendLog("rate_limit", { source: source, error: info, retry_after: seconds });
    console.warn("[discord-activity] Discord API 受到速率限制：", info);
    showActivityError("Discord 活动登录请求过于频繁。", seconds);
  }

  function parseTokenResponse(response) {
    return response.text().then(function (text) {
      var result;
      try {
        result = text ? JSON.parse(text) : {};
      } catch (e) {
        result = { ok: false, error: "服务器返回了无效响应", status: response.status };
      }
      result.status = response.status;
      return result;
    });
  }

  function hasRecentAuthInFlight() {
    var value = sessionStorage.getItem(IN_FLIGHT_KEY);
    if (!value) return false;
    // 兼容旧版本写入的固定值 "1"：它无法证明请求仍在进行，直接清理。
    var startedAt = Number(value);
    if (!startedAt || Date.now() - startedAt > IN_FLIGHT_MAX_AGE) {
      sessionStorage.removeItem(IN_FLIGHT_KEY);
      return false;
    }
    return true;
  }

  function checkActivitySession() {
    return fetch("/api/activity/status", {
      method: "GET",
      headers: { "X-Request-ID": clientRequestId },
      credentials: "same-origin",
    }).then(parseTokenResponse);
  }

  function resolveAfterTimeout(promise, timeoutMs) {
    return Promise.race([
      promise,
      new Promise(function (resolve) {
        window.setTimeout(function () { resolve({ timedOut: true }); }, timeoutMs);
      }),
    ]);
  }

  function startAuthentication() {
    if (sessionStorage.getItem(AUTHED_KEY) === "1") return;
    var cooldown = currentCooldownSeconds();
    if (cooldown > 0) {
      showActivityError("Discord 活动登录暂时受到限流。", cooldown);
      return;
    }
    if (hasRecentAuthInFlight()) {
      // 不要静默卡住：多半是上一次尝试被移动端挂起打断、从未真正完成。
      // 短暂等待后自动清掉旧锁并重试一次，而不是让用户对着转圈等 10 分钟。
      sendLog("authorize.stale_lock_wait", { retry_in_ms: IN_FLIGHT_RETRY_MS });
      window.setTimeout(function () {
        if (sessionStorage.getItem(AUTHED_KEY) === "1") return;
        sessionStorage.removeItem(IN_FLIGHT_KEY);
        startAuthentication();
      }, IN_FLIGHT_RETRY_MS);
      return;
    }
    sessionStorage.setItem(IN_FLIGHT_KEY, String(Date.now()));
    sendLog("authorize.start", { client_id_present: true });

    sdkPromise.then(function (discordSdk) {
      return rejectAfterTimeout(discordSdk.ready(), SDK_READY_TIMEOUT_MS, "Discord 客户端没有响应（ready 超时）").then(function () {
        sendLog("sdk.ready", {});
        return rejectAfterTimeout(
          discordSdk.commands.authorize({
            client_id: clientId,
            response_type: "code",
            state: activityState(),
            prompt: "none",
            scope: ["identify", "guilds"],
          }),
          AUTHORIZE_TIMEOUT_MS,
          "Discord 授权请求超时"
        );
      });
    }).then(function (authorizeResult) {
      if (!authorizeResult || !authorizeResult.code) throw new Error("Discord 没有返回授权 code");
      sendLog("authorize.success", { code_received: true });
      return fetch("/api/activity/token", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Request-ID": clientRequestId },
        credentials: "same-origin",
        body: JSON.stringify({ code: authorizeResult.code, request_id: clientRequestId }),
      }).then(parseTokenResponse);
    }).then(function (result) {
      if (!result.ok) {
        sessionStorage.removeItem(IN_FLIGHT_KEY);
        if (result.status === 429 || result.rate_limited) {
          showRateLimit("activity.token", result);
          return;
        }
        throw new Error(result.error || "未知错误");
      }
      sendLog("login.success", { user_id_present: Boolean(result.user && result.user.id) });
      // SDK authenticate() 只影响 Discord 客户端侧能力（比如参与者列表），
      // 不影响本站 Flask Session，也没有任何后续代码依赖它执行完成。
      // 不再 await 它：让它在后台自己跑完，登录流程直接去做会话确认，
      // 这样能把它最多 AUTHENTICATE_TIMEOUT_MS（2.5 秒）的等待从关键路径里去掉，
      // 是目前登录变慢最主要的一块。
      sdkPromise.then(function (discordSdk) {
        return resolveAfterTimeout(
          discordSdk.commands.authenticate({ access_token: result.access_token }),
          AUTHENTICATE_TIMEOUT_MS
        );
      }).then(function (outcome) {
        if (outcome && outcome.timedOut) {
          sendLog("sdk.authenticate.timeout", { timeout_ms: AUTHENTICATE_TIMEOUT_MS });
        }
      }).catch(function (err) {
        sendLog("sdk.authenticate.failure", { error: safeError(err) });
        console.warn("[discord-activity] SDK authenticate 失败（不影响网站登录）：", err);
      });
      // 先确认 Discord 代理确实保存了 Flask session；否则重载后会回到未登录页，
      // 用户再次点击会重新消耗 authorize 配额并触发 429。
      return checkActivitySession().then(function (status) {
        sendLog("session.check", { authenticated: Boolean(status.authenticated), status: status.status });
        if (!status.ok || !status.authenticated) {
          throw new Error("服务器登录会话没有保存，请检查 HTTPS、Cookie 和 Discord URL Mapping");
        }
        sessionStorage.setItem(AUTHED_KEY, "1");
        sessionStorage.removeItem(IN_FLIGHT_KEY);
        window.location.reload();
      });
    }).catch(function (err) {
      sessionStorage.removeItem(IN_FLIGHT_KEY);
      var info = safeError(err);
      sendLog("authorize.failure", { error: info });
      console.error("[discord-activity] Activity 登录失败：", err);
      if (info.retry_after || info.rate_limited || /rate.?limit|限流|速率/i.test(info.message || "")) {
        showRateLimit("sdk.authorize", err);
      } else {
        showActivityError("Discord 活动登录失败：" + (info.message || "未知错误"), 0);
      }
    });
  }

  if (!isInsideDiscordActivity()) return;
  // 预留 Discord 手机端小活动顶部框架占用的空间，避免内容被遮住。
  // 这一步要对 Activity 里的每一个页面都生效（不只是登录/欢迎页），
  // 之前只在登录流程分支里加这个 class，导致登录完成、跳到首页/排行榜/
  // 个人主页等页面之后就再也不会加上，顶部内容就被 Discord 的框架盖住了。
  document.documentElement.classList.add("discord-activity");

  // 这一步同样要对 Activity 里的每一个页面都生效，不只是登录/欢迎页——
  // 首页/排行榜/个人主页/年度报告上都有指向 discord.com 的外部链接卡片。
  // 只在用户真的点了外部链接时才去加载 SDK 并握手，不影响首屏加载。
  setupExternalLinkInterception();

  // 已经进入登录后的 Activity 页面时，不要每次切换页面都再次请求
  // /api/activity/status。只有欢迎页/登录页才需要运行 Activity 登录逻辑。
  var activityLoginUi = document.getElementById("web-login-action") || document.getElementById("activity-login-status");
  if (!activityLoginUi) return;
  if (openActivityLoginFirst()) return;
  showActivityLoginState();
  keepReportInsideActivity();

  var clientIdMeta = document.querySelector('meta[name="discord-client-id"]');
  var clientId = clientIdMeta ? clientIdMeta.getAttribute("content") : "";
  if (!clientId) {
    sendLog("configuration.failure", { client_id_present: false });
    console.warn("[discord-activity] 未配置 DISCORD_CLIENT_ID，跳过活动模式登录。");
    return;
  }

  sendLog("activity.detected", { client_id_present: true });

  function initializeSdkAndAuthenticate() {
    if (!hasSdkQueryParams()) {
      sendLog("configuration.failure", { frame_id_present: false, instance_id_present: false, platform_present: false });
      showActivityError("Discord 活动参数不完整，请关闭后从 Discord 重新打开小活动。", 0);
      return;
    }
    // 必须使用本站托管的固定版本。Discord Activity 的网络代理不保证可以访问
    // jsDelivr；远程模块加载失败时 authorize() 根本不会执行。
    sdkPromise = import("/static/vendor/discord-embedded-app-sdk/discord-activity-sdk.mjs?v=20260821-2")
      .then(function (mod) {
        sendLog("sdk.loaded", {});
        return new mod.DiscordSDK(clientId);
      });
    sessionStorage.removeItem(AUTHED_KEY);
    sessionStorage.removeItem(IN_FLIGHT_KEY);
    startAuthentication();
  }

  // 先确认服务器会话，再构造 Discord SDK。已有有效登录时不需要 SDK，
  // 也不会因为 URL 缺少 frame_id 把已经登录的用户误判为登录失败。
  checkActivitySession().then(function (status) {
    sendLog("session.initial", { authenticated: Boolean(status.authenticated), status: status.status });
    if (status.ok && status.authenticated) {
      sessionStorage.setItem(AUTHED_KEY, "1");
      sessionStorage.removeItem(IN_FLIGHT_KEY);
      return;
    }
    initializeSdkAndAuthenticate();
  }).catch(function (err) {
    sessionStorage.removeItem(AUTHED_KEY);
    sessionStorage.removeItem(IN_FLIGHT_KEY);
    sendLog("session.initial.failure", { error: safeError(err) });
    showActivityError("无法确认网站登录状态，请稍后重试。", 0);
  });
})();
