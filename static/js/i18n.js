/* Shared UI language switcher. Chinese is the source language and default. */
(function () {
  "use strict";
  var STORAGE_KEY = "portal-language";
  var sourceNodes = new WeakMap();
  var translations = {
    "切换服务器":"Switch server", "历史访客":"Recent visitors", "历史访客记录":"Visitor history", "我的界面":"My profile", "个人主页":"My profile", "管理员":"Admin", "退出登录":"Log out", "档案馆":"Archive", "Discord 档案馆":"Discord Archive", "抽象派档案馆":"Abstract Archive", "🌊 抽象派档案馆":"🌊 Abstract Archive",
    "搜索用户名、昵称或 User ID...":"Search username, nickname, or User ID...", "搜索结果":"Search results", "收录帖子":"Threads", "收录消息":"Messages", "活跃成员 (点击查看)":"Active members (click to view)", "活跃榜单":"Active members", "🏆 活跃榜单":"🏆 Active members", "完整活跃榜单":"Full active leaderboard", "加载更多...":"Loading more...",
    "活跃度趋势":"Activity trend", "日期":"Daily", "📅 日期":"📅 Daily", "服务器热门词汇 (纯汉字)":"Popular server keywords", "热门词汇 Top 15":"Popular keywords Top 15", "热门回复":"Popular replies", "热门回复 Top 10":"Popular replies Top 10", "⭐ 热门回复":"⭐ Popular replies", "热门讨论区":"Popular discussions", "热门讨论区 Top 10":"Popular discussions Top 10", "🔥 热门讨论区":"🔥 Popular discussions", "返回首页":"Back to home", "选择服务器":"Select a server", "你的 Discord 账号拥有多个服务器的数据访问权限":"Your Discord account can access data from multiple servers",
    "完整排行榜":"Full leaderboard", "活跃成员排行榜 (Top 200)":"Active members leaderboard (Top 200)", "用户":"User", "消息数":"Messages", "常用表情":"Common emojis", "返回介绍页面":"Back to introduction", "登录":"Log in", "登录后即可查看你有权限访问的服务器数据":"Log in to view server data you can access", "使用 Discord 登录":"Log in with Discord", "登录后查看我的数据":"Log in to view my data", "未登录也可以浏览介绍页面。":"You can browse the introduction without logging in.",
    "多服务器 Discord 数据分析平台":"Multi-server Discord analytics platform", "Discord 身份验证":"Discord authentication", "自动匹配服务器":"Automatic server matching", "开始数据分析":"Start analyzing data", "暂时没有与你关联的服务器数据。":"There is currently no server data associated with you.", "账号已登录":"Account logged in", "等待数据":"Waiting for data", "当前账号没有服务器数据":"No server data for this account", "JSON 导入":"JSON import", "导入并创建服务器":"Import and create server",
    "个人词云 (3D)":"Personal word cloud (3D)", "常用词排行 Top 10":"Common words Top 10", "活跃度统计":"Activity statistics", "经常使用":"Frequently used", "热门获赞":"Popular received reactions", "发言":"Messages", "获赞":"Reactions", "浏览":"Views", "帖子":"Threads", "消息":"Messages", "热度":"Hot", "时间":"Time", "📢 发帖记录":"📢 Thread history", "📜 发言记录":"📜 Message history", "🔥 热度":"🔥 Hot", "🕒 时间":"🕒 Time", "❤️ 在意你的 (他->你)":"❤️ Cares about you (they->you)", "💙 你在意的 (你->他)":"💙 You care about (you->them)", "该用户暂无发布帖子记录。":"This user has no published threads yet.", "已无更多数据":"No more data", "最近访客":"Recent visitors", "暂无访客记录":"No visitors yet",
    "年度总结":"Annual summary", "年度总结 -":"Annual summary -", "登录 - Discord 档案馆":"Log in - Discord Archive", "初次相遇":"First encounter", "击败全服":"Beat", "下滑查看更多":"Scroll down for more", "数据高光时刻":"Data highlights", "发言最多的一天":"Most active day", "条消息":" messages", "熬夜最晚":"Latest late-night message", "引发热议的帖子":"Most discussed thread", "最受欢迎的消息":"Most popular message", "你的年度关键词":"Your annual keywords", "你最常互动的":"Your most frequent interaction", "最常与你互动的":"Most frequent interaction with you", "互动":"Interactions", "这就是你的":"This is your", "感谢在抽象派留下的每一个足迹":"Thank you for every footprint you left in Abstract", "✨ 生成我的年度报告":"✨ Generate my annual report", "认领此账号":"Claim this account",
    "管理后台":"Admin panel", "下载、数据导入和账号合并管理":"Downloads, imports, and account merge management", "下载、数据导入与账号合并管理":"Downloads, imports, and account merge management", "白名单用户":"Whitelist users", "我的下载机器人":"My download bots", "Discord 数据下载配置":"Discord data download settings", "服务器成员名单":"Server members", "下载任务":"Download tasks", "待审核的合并申请":"Pending merge requests", "已生效的合并关系":"Active merge relationships", "知道了":"Got it", "保存配置":"Save settings", "添加白名单":"Add to whitelist", "验证并添加":"Verify and add", "保存配额":"Save quota", "暂无白名单用户":"No whitelist users", "暂无下载机器人。":"No download bots.", "暂无下载配置。":"No download settings.", "暂无下载任务。":"No download tasks.", "暂无待审核申请。":"No pending requests.", "暂无合并关系。":"No merge relationships.", "操作完成":"Operation complete", "机器人权限检查失败":"Bot permission check failed", "服务器":"Server", "登录":"Log in", "使用 Discord 登录":"Log in with Discord", "返回介绍页面":"Back to introduction", "正在登陆中":"Signing in", "Discord 小活动正在自动完成身份验证，请稍候。":"The Discord Activity is signing you in. Please wait.", "账号已登录":"Account logged in", "多服务器 Discord 数据分析平台":"Multi-server Discord analytics platform", "认领此账号":"Claim this account"
  };
  function lang() { try { return localStorage.getItem(STORAGE_KEY) === "en" ? "en" : "zh"; } catch (e) { return "zh"; } }
  function walk(root, fn) { var w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT), n; while ((n = w.nextNode())) fn(n); }
  function ignored(n) { return !n.parentElement || n.parentElement.closest("script,style,textarea,input,[data-i18n-ignore]"); }
  function translateText(source) {
    var key = source.trim();
    if (translations[key]) return source.replace(key, translations[key]);
    var match;
    if ((match = key.match(/^年度总结\s*-\s*(.+)$/))) return source.replace(key, "Annual summary - " + match[1]);
    if ((match = key.match(/^(.+)\s+的个人档案$/))) return source.replace(key, match[1] + "'s profile");
    if ((match = key.match(/^互动\s+(.+?)\s*次$/))) return source.replace(key, "Interactions " + match[1] + " times");
    if ((match = key.match(/^(\d+)\s*次$/))) return source.replace(key, match[1] + " times");
    if ((match = key.match(/^(\d+)\s*条消息$/))) return source.replace(key, match[1] + " messages");
    if ((match = key.match(/^🌙\s*熬夜最晚\s*\((.*)\)$/))) return source.replace(key, "🌙 Latest late-night message (" + match[1] + ")");
    if (key === "最近访问:") return source.replace(key, "Last visited:");
    return null;
  }

  function apply(value) {
    document.documentElement.lang = value === "en" ? "en" : "zh";
    walk(document.documentElement, function (n) {
      if (ignored(n)) return;
      var source = sourceNodes.get(n);
      if (source === undefined) {
        source = n.nodeValue;
        sourceNodes.set(n, source);
      }
      if (value === "zh") { n.nodeValue = source; return; }
      var translated = translateText(source);
      if (translated !== null) n.nodeValue = value === "en" ? translated : source;
    });
    document.querySelectorAll("[placeholder],[title]").forEach(function (el) {
      ["placeholder", "title"].forEach(function (attr) {
        if (!el.hasAttribute(attr)) return;
        var key = attr + "I18nSource";
        if (!el.dataset[key]) el.dataset[key] = el.getAttribute(attr);
        var source = el.dataset[key];
        el.setAttribute(attr, value === "en" ? (translations[source] || source) : source);
      });
    });
    document.querySelectorAll("[data-language-toggle]").forEach(function (button) {
      button.textContent = value === "en" ? "中文" : "English";
    });
  }
  function set(value) { try { localStorage.setItem(STORAGE_KEY, value); } catch (e) {} apply(value); }
  function ensureToggle() {
    document.querySelectorAll("details").forEach(function (details) {
      if (details.querySelector("[data-language-toggle]")) return;
      var menu = details.querySelector(".absolute");
      if (!menu) return;
      var button = document.createElement("button");
      button.type = "button";
      button.dataset.languageToggle = "";
      button.className = "block w-full text-left p-2 hover:bg-[#36393f] rounded";
      button.textContent = "English";
      menu.insertBefore(button, menu.lastElementChild);
      button.addEventListener("click", function (event) { event.preventDefault(); event.stopPropagation(); set(lang() === "en" ? "zh" : "en"); });
    });
  }
  function addHomepageRanks() {
    document.querySelectorAll("h2").forEach(function (heading) {
      var label = heading.textContent;
      if (label.indexOf("热门回复") < 0 && label.indexOf("Popular replies") < 0 && label.indexOf("热门讨论区") < 0 && label.indexOf("Popular discussions") < 0) return;
      var panel = heading.parentElement && heading.parentElement.parentElement;
      if (!panel) return;
      panel.querySelectorAll(":scope > .space-y-4 > a").forEach(function (card, index) {
        var row = card.querySelector(".flex.gap-3");
        if (!row || row.querySelector(".portal-rank")) return;
        var rank = document.createElement("span");
        rank.className = "portal-rank text-[#faa61a] font-mono font-bold w-5 shrink-0";
        rank.textContent = String(index + 1);
        row.insertBefore(rank, row.firstChild);
      });
      if (!heading.querySelector(".portal-top-label") && !/Top 10/i.test(heading.textContent)) {
        var top = document.createElement("span");
        top.className = "portal-top-label ml-2 text-xs text-gray-500 font-normal";
        top.textContent = "Top 10";
        heading.appendChild(top);
      }
    });
  }
  document.addEventListener("DOMContentLoaded", function () {
    ensureToggle();
    document.querySelectorAll("[data-language-toggle]").forEach(function (button) {
      button.addEventListener("click", function (event) { event.preventDefault(); event.stopPropagation(); set(lang() === "en" ? "zh" : "en"); });
    });
    apply(lang());
    addHomepageRanks();
  });
})();
