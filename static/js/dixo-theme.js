/* Light / dark theme toggle. Preference in localStorage ("dixo-theme"). */
(function () {
  "use strict";
  var KEY = "dixo-theme";
  function preferred() {
    try { var v = localStorage.getItem(KEY); if (v === "dark" || v === "light") return v; } catch (e) {}
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  function apply(t) {
    document.documentElement.setAttribute("data-theme", t);
    document.querySelectorAll("[data-theme-toggle]").forEach(function (b) {
      b.setAttribute("aria-pressed", t === "dark" ? "true" : "false");
      b.title = t === "dark" ? "Light" : "Dark";
      b.textContent = t === "dark" ? "☀️" : "🌙";
    });
    document.dispatchEvent(new CustomEvent("dixo:theme", { detail: t }));
  }
  function set(t) { try { localStorage.setItem(KEY, t); } catch (e) {} apply(t); }
  // Apply ASAP to avoid flash (also duplicated inline in base.html)
  apply(preferred());
  document.addEventListener("DOMContentLoaded", function () {
    apply(preferred());
    document.querySelectorAll("[data-theme-toggle]").forEach(function (b) {
      b.addEventListener("click", function (e) {
        e.preventDefault();
        set((document.documentElement.getAttribute("data-theme") === "dark") ? "light" : "dark");
      });
    });
  });
  window.dixoTheme = { get: preferred, set: set };
})();
