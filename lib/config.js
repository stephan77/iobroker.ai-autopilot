"use strict";

/**
 * Konfiguration normalisieren.
 * Verhindert undefinierte Felder und sorgt für saubere Defaults.
 */
module.exports = (adapter) => {
  /**
   * Liefert eine normalisierte Konfiguration aus native.
   */
  function normalize() {
    const native = adapter.config || {};

    const dataPoints = Array.isArray(native.dataPoints) ? native.dataPoints : [];
    const history = native.history || {};
    const telegram = native.telegram || {};
    const gpt = native.gpt || {};
    const scheduler = native.scheduler || {};

    return {
      dataPoints: dataPoints
        .filter((entry) => entry && entry.objectId)
        .map((entry) => ({
          objectId: entry.objectId,
          dailyObjectId: entry.dailyObjectId || "",
          category: entry.category || "unknown",
          description: entry.description || "",
          unit: entry.unit || "",
          enabled: entry.enabled !== false,
          includeInBalance: entry.includeInBalance === true,
          isTotalMeter: entry.isTotalMeter === true,
          isSource: entry.isSource === true,
          isConsumer: entry.isConsumer === true,
          orientation: entry.orientation || "",
        })),
      history: {
        mode: history.mode || "auto",
        instance: history.instance || "",
      },
      telegram: {
        enabled: telegram.enabled === true,
        token: telegram.token || "",
        chatId: telegram.chatId || "",
      },
      gpt: {
        enabled: gpt.enabled === true,
        openaiApiKey: gpt.openaiApiKey || "",
        model: gpt.model || "",
      },
      scheduler: {
        enabled: scheduler.enabled === true,
        time: scheduler.time || "08:00",
        days: scheduler.days || "mon,tue,wed,thu,fri,sat,sun",
        timezone: scheduler.timezone || "UTC",
      },
    };
  }

  return {
    normalize,
  };
};
