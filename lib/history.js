"use strict";

/**
 * History-Modul: Ermittelt verfügbare History-Adapter und lädt Daten.
 */
module.exports = (adapter) => {
  /**
   * Sammelt historische Daten für konfigurierte Punkte.
   */
  async function collect(config, liveContext) {
    const historyInstance = await detectHistoryInstance(config.history);
    const result = {};

    if (!historyInstance) {
      adapter.log.info("Kein History-Adapter verfügbar. Überspringe Historie.");
      return result;
    }

    for (const entry of config.dataPoints) {
      if (!entry.enabled) {
        continue;
      }
      result[entry.objectId] = await fetchHistory(historyInstance, entry.objectId);
    }

    return result;
  }

  /**
   * Ermittelt die History-Instanz entsprechend der Konfiguration.
   */
  async function detectHistoryInstance(historyConfig) {
    if (historyConfig.mode === "instance" && historyConfig.instance) {
      return historyConfig.instance;
    }

    try {
      const view = await adapter.getObjectViewAsync("system", "instance", {
        startkey: "system.adapter.",
        endkey: "system.adapter.\u9999",
      });
      const historyAdapters = (view.rows || [])
        .map((row) => row.value)
        .filter((obj) => obj && obj.common && obj.common.type === "history")
        .map((obj) => obj._id.replace("system.adapter.", ""));

      return historyAdapters[0] || "";
    } catch (error) {
      adapter.log.warn(`History-Adapter konnte nicht ermittelt werden: ${error.message}`);
      return "";
    }
  }

  /**
   * Lädt historische Daten für ein Objekt.
   */
  async function fetchHistory(instance, objectId) {
    const options = {
      id: objectId,
      options: {
        start: Date.now() - 24 * 60 * 60 * 1000,
        end: Date.now(),
        aggregate: "onchange",
      },
    };

    try {
      const response = await adapter.sendToAsync(instance, "getHistory", options);
      return normalizeHistory(response);
    } catch (error) {
      adapter.log.info(`Keine Historie für ${objectId}: ${error.message}`);
      return [];
    }
  }

  /**
   * Normalisiert die History-Antwort.
   */
  function normalizeHistory(response) {
    if (!response) {
      return [];
    }
    if (Array.isArray(response)) {
      return response;
    }
    if (Array.isArray(response.result)) {
      return response.result;
    }
    return [];
  }

  return {
    collect,
  };
};
