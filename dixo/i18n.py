"""Key-based i18n (replaces text-node guessing in static/js/i18n.js for redesigned pages).

Templates: {{ t("home.kpi.messages") }} renders <span data-i18n="home.kpi.messages">中文</span>;
           {{ tr("key") }} returns the plain zh string (for attributes / JS).
Client:    static/js/dixo-i18n.js swaps text to English using window.DIXO_I18N (injected per page),
           same localStorage key ("portal-language") as the legacy switcher.
Missing keys raise in tests (tests/test_i18n.py scans templates for t("...") keys).
"""
from markupsafe import Markup, escape

MESSAGES = {
    "nav.switch_server": ("切换服务器", "Switch server"),
    "nav.visitors": ("访客", "Visitors"),
    "nav.my_profile": ("我的主页", "My profile"),
    "nav.admin": ("管理", "Admin"),
    "nav.language": ("English", "中文"),
    "nav.logout": ("退出登录", "Log out"),
    "nav.home": ("总览", "Overview"),
    "nav.archive": ("档案馆", "Archive"),
    "search.placeholder": ("搜索用户名、昵称或 User ID", "Search username, nickname or user ID"),
    "search.results": ("搜索结果", "Search results"),
    "search.none": ("没有找到匹配的成员。", "No matching members."),
    "state.rebuilding": ("统计数据正在后台更新", "Statistics are updating in the background"),
    "state.updated": ("数据更新至", "Data as of"),
    "home.kpi.messages": ("消息总数", "Messages"),
    "home.kpi.threads": ("帖子", "Threads"),
    "home.kpi.members": ("成员", "Members"),
    "home.kpi.reactions": ("表情反应", "Reactions"),
    "home.kpi.msgs7d": ("近 7 天消息", "Messages, last 7 days"),
    "home.kpi.active7d": ("近 7 天活跃成员", "Active members, last 7 days"),
    "home.kpi.vs_prev": ("较前 7 天", "vs previous 7 days"),
    "home.trend": ("活跃趋势", "Activity trend"),
    "home.trend.daily": ("按日", "Daily"),
    "home.trend.hourly": ("24 小时", "24 hours"),
    "home.trend.range30": ("30 天", "30 days"),
    "home.trend.range90": ("90 天", "90 days"),
    "home.trend.rangeall": ("全部", "All"),
    "home.trend.ma7": ("7 日均线", "7-day average"),
    "home.trend.messages": ("消息", "Messages"),
    "home.heatmap": ("活跃热力图", "Activity heatmap"),
    "home.heatmap.hint": ("按星期 × 小时（北京时间）", "Weekday × hour (UTC+8)"),
    "home.heatmap.less": ("少", "Less"),
    "home.heatmap.more": ("多", "More"),
    "home.words": ("热门词汇", "Top keywords"),
    "home.words.hint": ("已过滤链接、提及、表情、代码与常见停用词", "Links, mentions, emoji, code and stopwords filtered"),
    "home.words.rank": ("词频排行", "Keyword ranking"),
    "home.leaderboard": ("活跃排行", "Most active members"),
    "home.leaderboard.all": ("查看全部", "View all"),
    "home.leaderboard.full": ("完整活跃排行", "Full leaderboard"),
    "home.leaderboard.member": ("成员", "Member"),
    "home.leaderboard.messages": ("消息", "Messages"),
    "home.leaderboard.reactions": ("获赞", "Reactions"),
    "home.leaderboard.loading": ("加载中…", "Loading…"),
    "home.hot_threads": ("热门讨论", "Top discussions"),
    "home.hot_replies": ("高赞消息", "Most reacted messages"),
    "home.replies": ("回复", "replies"),
    "home.visitors.title": ("最近访客", "Recent visitors"),
    "home.visitors.last": ("最近访问", "Last visit"),
    "common.close": ("关闭", "Close"),
    "common.empty": ("暂无数据", "No data yet"),
    "common.prev": ("上一页", "Previous"),
    "common.next": ("下一页", "Next"),
    "common.times": ("次", "×"),
    "user.back": ("返回总览", "Back to overview"),
    "user.claim": ("认领此账号", "Claim this account"),
    "user.claim_confirm": ("确认申请认领此账号？需管理员审核。", "Request to claim this account? An admin must approve."),
    "user.report": ("生成我的年度报告", "Generate my yearly report"),
    "user.kpi.messages": ("发言", "Messages"),
    "user.kpi.reactions": ("获赞", "Reactions received"),
    "user.kpi.given": ("送出反应", "Reactions given"),
    "user.kpi.threads": ("发起帖子", "Threads started"),
    "user.kpi.days": ("活跃天数", "Active days"),
    "user.kpi.rank": ("排名", "Rank"),
    "user.kpi.views": ("主页浏览", "Profile views"),
    "user.since": ("首次发言", "First message"),
    "user.last": ("最近发言", "Last message"),
    "user.activity": ("活跃度", "Activity"),
    "user.emoji_given": ("常用表情", "Favourite emoji"),
    "user.emoji_received": ("最常收到", "Most received"),
    "user.incoming": ("最常给 TA 反应", "Reacts to them most"),
    "user.outgoing": ("TA 最常互动", "They interact with most"),
    "user.words": ("个人词云", "Personal keywords"),
    "user.words.hint": ("最近 2000 条消息", "Last 2,000 messages"),
    "user.computing": ("正在计算…", "Computing…"),
    "user.blocks": ("拉黑列表", "Blocked users"),
    "user.blocks.empty": ("暂无拉黑记录。使用 /block 或右键消息拉黑用户。", "No blocks. Use /block or right-click a message."),
    "user.favorites": ("收藏列表", "Saved threads"),
    "user.favorites.empty": ("暂无收藏。在 Forum 帖子上右键 → Apps → 📌 收藏帖子。", "Nothing saved. Right-click a forum post → Apps → 📌 Save."),
    "user.tab.threads": ("发帖记录", "Threads"),
    "user.tab.messages": ("发言记录", "Messages"),
    "user.sort.hot": ("最热", "Top"),
    "user.sort.time": ("最新", "Latest"),
    "user.threads.empty": ("该成员暂无发帖记录。", "No threads started yet."),
    "user.messages.empty": ("暂无发言记录。", "No messages."),
    "user.viewers": ("最近访客", "Recent viewers"),
    "user.viewers.empty": ("暂无访客记录", "No viewers yet"),
    "user.intercepted": ("拦截", "blocked"),
}


def tr(key, lang="zh"):
    zh, en = MESSAGES[key]
    return en if lang == "en" else zh


def t(key):
    return Markup('<span data-i18n="%s">%s</span>') % (key, escape(MESSAGES[key][0]))


def client_table():
    return {k: v[1] for k, v in MESSAGES.items()}


def init_app(app):
    app.jinja_env.globals.update(t=t, tr=tr, i18n_en=client_table)
