/** Instant feedback for server-rendered tab / sort / pager links (profile, report, admin).
 *  Without it a slow page load (esp. inside Discord Activity WebView, which shows no
 *  progress bar) looks like the tap did nothing. */
(function () {
  "use strict";
  var SEL = ".tabs a[href], .seg a[href], .pager a[href]";
  document.addEventListener("click", function (ev) {
    var a = ev.target && ev.target.closest ? ev.target.closest(SEL) : null;
    if (!a || ev.defaultPrevented || ev.metaKey || ev.ctrlKey || ev.shiftKey || a.target === "_blank") return;
    var group = a.parentElement;
    if (group) group.querySelectorAll("a").forEach(function (x) {
      if (x.classList.contains("on")) x.setAttribute("data-was-on", "1");
      x.classList.remove("on");
    });
    a.classList.add("on", "is-pending");
    var rec = a.closest("#records") || document.getElementById("records");
    if (rec && a.closest("#records")) rec.classList.add("is-loading");
  }, false);
  window.addEventListener("pageshow", function () {
    document.querySelectorAll(".is-pending").forEach(function (x) { x.classList.remove("is-pending", "on"); });
    document.querySelectorAll("[data-was-on]").forEach(function (x) { x.classList.add("on"); x.removeAttribute("data-was-on"); });
    document.querySelectorAll("#records.is-loading").forEach(function (x) { x.classList.remove("is-loading"); });
  });
})();
