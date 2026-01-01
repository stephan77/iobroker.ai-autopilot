"use strict";

/**
 * Statistiken berechnen und als JSON zurückgeben.
 */
module.exports = () => {
  /**
   * Berechnet Kennzahlen auf Basis von Live- und Historiendaten.
   */
  function compute(config, liveContext, historyData) {
    const stats = {
      timestamp: new Date().toISOString(),
      energy: {},
      temperature: {},
      water: {},
      deviations: [],
    };

    stats.energy.houseConsumption = extractValue(liveContext.energy["energy.houseConsumption"]);
    stats.energy.pvPower = extractValue(liveContext.energy["energy.pvPower"]);
    stats.energy.gridPower = extractValue(liveContext.energy["energy.gridPower"]);
    stats.energy.batterySoc = extractValue(liveContext.energy["energy.batterySoc"]);
    stats.energy.wallboxPower = extractValue(liveContext.energy["energy.wallbox"]);

    stats.temperature.outside = extractValue(liveContext.temperature["temperature.outside"]);

    stats.water.total = extractValue(liveContext.water["water.total"]);

    for (const [objectId, series] of Object.entries(historyData || {})) {
      stats.deviations.push({
        objectId,
        avg: average(series),
        min: minimum(series),
        max: maximum(series),
        last: lastValue(series),
      });
    }

    return stats;
  }

  function extractValue(entry) {
    if (!entry) {
      return null;
    }
    return entry.value;
  }

  function average(series) {
    if (!series || !series.length) {
      return null;
    }
    const values = series.map((item) => Number(item.val)).filter((val) => Number.isFinite(val));
    if (!values.length) {
      return null;
    }
    return values.reduce((sum, val) => sum + val, 0) / values.length;
  }

  function minimum(series) {
    if (!series || !series.length) {
      return null;
    }
    const values = series.map((item) => Number(item.val)).filter((val) => Number.isFinite(val));
    return values.length ? Math.min(...values) : null;
  }

  function maximum(series) {
    if (!series || !series.length) {
      return null;
    }
    const values = series.map((item) => Number(item.val)).filter((val) => Number.isFinite(val));
    return values.length ? Math.max(...values) : null;
  }

  function lastValue(series) {
    if (!series || !series.length) {
      return null;
    }
    const last = series[series.length - 1];
    return last ? last.val : null;
  }

  return {
    compute,
  };
};
