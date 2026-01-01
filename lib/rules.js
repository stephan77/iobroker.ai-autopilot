"use strict";

/**
 * Regeln und Abweichungen erkennen.
 */
module.exports = (adapter) => {
  /**
   * Detektiert einfache Abweichungen gegenüber Historie.
   */
  function detectDeviations(config, liveContext, historyData, stats) {
    const deviations = [];

    for (const entry of config.dataPoints) {
      if (!entry.enabled) {
        continue;
      }
      const series = historyData[entry.objectId] || [];
      if (!series.length) {
        continue;
      }
      const avg = seriesAverage(series);
      const current = liveContext.raw[entry.objectId];
      if (avg !== null && current !== null && Number.isFinite(avg)) {
        const diff = Number(current) - avg;
        if (Math.abs(diff) > 0 && Math.abs(diff) > Math.abs(avg) * 0.3) {
          deviations.push({
            objectId: entry.objectId,
            current,
            average: avg,
            delta: diff,
          });
        }
      }
    }

    adapter.log.debug(`Abweichungen gefunden: ${deviations.length}`);
    return deviations;
  }

  function seriesAverage(series) {
    const values = series.map((item) => Number(item.val)).filter((val) => Number.isFinite(val));
    if (!values.length) {
      return null;
    }
    return values.reduce((sum, val) => sum + val, 0) / values.length;
  }

  return {
    detectDeviations,
  };
};
