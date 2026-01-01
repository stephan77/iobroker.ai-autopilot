"use strict";

/**
 * Discovery-Modul für automatische Kandidatenerkennung.
 * Sammelt geeignete Datenpunkte, ohne sie zu aktivieren.
 */
module.exports = (adapter) => {
  /**
   * Scannt bekannte Adapter und erzeugt Kandidaten mit einfacher Bewertung.
   */
  async function runDiscovery() {
    const candidates = [];

    try {
      const view = await adapter.getObjectViewAsync("system", "state", {
        startkey: "",
        endkey: "\u9999",
      });

      for (const row of view.rows || []) {
        const obj = row.value;
        if (!obj || obj.type !== "state" || !obj.common) {
          continue;
        }
        const common = obj.common;
        if (!common.type || !["number", "boolean"].includes(common.type)) {
          continue;
        }

        const score = scoreCandidate(obj);
        if (score <= 0) {
          continue;
        }

        candidates.push({
          objectId: obj._id,
          name: common.name || obj._id,
          unit: common.unit || "",
          role: common.role || "",
          type: common.type,
          score,
        });
      }
    } catch (error) {
      adapter.log.warn(`Discovery konnte Objekte nicht laden: ${error.message}`);
    }

    candidates.sort((a, b) => b.score - a.score);
    return candidates.slice(0, 200);
  }

  /**
   * Einfache Bewertung anhand von Rolle, Einheit und Namen.
   */
  function scoreCandidate(obj) {
    const common = obj.common || {};
    const name = String(common.name || "").toLowerCase();
    const role = String(common.role || "").toLowerCase();
    const unit = String(common.unit || "").toLowerCase();

    let score = 0;
    if (role.includes("energy") || role.includes("power")) {
      score += 3;
    }
    if (unit.includes("w") || unit.includes("wh")) {
      score += 2;
    }
    if (name.includes("power") || name.includes("leistung")) {
      score += 2;
    }
    if (name.includes("temperature") || name.includes("temperatur")) {
      score += 1;
    }

    return score;
  }

  return {
    runDiscovery,
  };
};
