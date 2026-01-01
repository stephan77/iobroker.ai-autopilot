"use strict";

/**
 * Report-Erstellung und Persistierung in States.
 */
module.exports = (adapter) => {
  /**
   * Baut das finale Report-Objekt.
   */
  function build(config, liveContext, historyData, stats, actions) {
    return {
      timestamp: new Date().toISOString(),
      live: liveContext,
      history: historyData,
      stats,
      actions,
    };
  }

  /**
   * Persistiert den Report in den vorgesehenen States.
   */
  async function persist(report) {
    await adapter.setStateAsync("report.last", JSON.stringify(report), true);
    await adapter.setStateAsync("report.stats", JSON.stringify(report.stats || {}), true);
    await adapter.setStateAsync("report.actions", JSON.stringify(report.actions || []), true);
  }

  /**
   * Persistiert einen leeren Report, falls keine Datenpunkte vorhanden sind.
   */
  async function persistEmpty() {
    const emptyReport = {
      timestamp: new Date().toISOString(),
      info: "Keine Datenpunkte konfiguriert",
    };
    await adapter.setStateAsync("report.last", JSON.stringify(emptyReport), true);
    await adapter.setStateAsync("report.stats", JSON.stringify({}), true);
    await adapter.setStateAsync("report.actions", JSON.stringify([]), true);
  }

  return {
    build,
    persist,
    persistEmpty,
  };
};
