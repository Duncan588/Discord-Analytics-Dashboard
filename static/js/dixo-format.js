/** Shared compact number formatter (100, 1.2K, 5M, 1B). */
(function (root) {
  function compact(value) {
    var n = Number(value);
    if (!isFinite(n)) return value == null ? "0" : String(value);
    if (n < 0) return "-" + compact(-n);
    if (n < 1000) return (Math.round(n) === n) ? String(Math.round(n)) : String(Number(n.toFixed(1)));
    var units = [[1e9, "B"], [1e6, "M"], [1e3, "K"]];
    for (var i = 0; i < units.length; i++) {
      var div = units[i][0], suf = units[i][1];
      if (n >= div) {
        var v = n / div;
        if (v >= 100) return Math.round(v) + suf;
        var s = v.toFixed(1).replace(/\.0$/, "");
        return s + suf;
      }
    }
    return String(Math.round(n));
  }
  root.dixoCompact = compact;
  // Chart.js tick helper
  root.dixoCompactTick = function (v) { return compact(v); };
})(typeof window !== "undefined" ? window : globalThis);
