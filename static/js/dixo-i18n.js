/* Key-based language switch for redesigned pages (see dixo/i18n.py). */
(function () {
  "use strict";
  var KEY = "portal-language";
  var zhCache = new WeakMap();
  function lang() { try { return localStorage.getItem(KEY) === "en" ? "en" : "zh"; } catch (e) { return "zh"; } }
  function apply(l) {
    var en = window.DIXO_I18N || {};
    document.documentElement.lang = l === "en" ? "en" : "zh-CN";
    document.querySelectorAll("[data-i18n]").forEach(function (el) {
      if (!zhCache.has(el)) zhCache.set(el, el.textContent);
      var k = el.getAttribute("data-i18n");
      el.textContent = l === "en" && en[k] ? en[k] : zhCache.get(el);
    });
    document.querySelectorAll("[data-i18n-attr]").forEach(function (el) {
      el.getAttribute("data-i18n-attr").split(";").forEach(function (pair) {
        var p = pair.split(":"), attr = p[0], k = p[1];
        var store = "data-zh-" + attr;
        if (!el.hasAttribute(store)) el.setAttribute(store, el.getAttribute(attr) || "");
        el.setAttribute(attr, l === "en" && en[k] ? en[k] : el.getAttribute(store));
      });
    });
    document.dispatchEvent(new CustomEvent("dixo:lang", { detail: l }));
  }
  window.dixoLang = lang;
  window.dixoT = function (k, zh) { var en = window.DIXO_I18N || {}; return lang() === "en" && en[k] ? en[k] : zh; };
  document.addEventListener("DOMContentLoaded", function () {
    apply(lang());
    document.querySelectorAll("[data-language-toggle]").forEach(function (b) {
      b.addEventListener("click", function (e) {
        e.preventDefault();
        var next = lang() === "en" ? "zh" : "en";
        try { localStorage.setItem(KEY, next); } catch (err) {}
        apply(next);
      });
    });
  });
})();
