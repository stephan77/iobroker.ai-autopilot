"use strict";

/**
 * State-Handling für den Adapter.
 * Erstellt Objekte, ohne vorhandene Werte zu überschreiben.
 */
module.exports = (adapter) => {
  const stateDefinitions = [
    { id: "control.run", type: "boolean", role: "button", read: true, write: true, def: false },
    { id: "info.connection", type: "boolean", role: "indicator.connected", read: true, write: false, def: false },
    { id: "info.lastError", type: "string", role: "text", read: true, write: false, def: "" },
    { id: "meta.running", type: "boolean", role: "indicator", read: true, write: false, def: false },
    { id: "meta.lastRun", type: "string", role: "date", read: true, write: false, def: "" },
    { id: "meta.lastDailyReportTs", type: "string", role: "date", read: true, write: false, def: "" },
    { id: "report.last", type: "string", role: "json", read: true, write: false, def: "{}" },
    { id: "report.stats", type: "string", role: "json", read: true, write: false, def: "{}" },
    { id: "report.actions", type: "string", role: "json", read: true, write: false, def: "[]" },
    { id: "report.actionHistory", type: "string", role: "json", read: true, write: false, def: "[]" },
    { id: "report.dailyLastSent", type: "string", role: "json", read: true, write: false, def: "{}" },
    { id: "memory.feedback", type: "string", role: "json", read: true, write: false, def: "{}" },
    { id: "memory.learning", type: "string", role: "json", read: true, write: false, def: "{}" },
    { id: "memory.history", type: "string", role: "json", read: true, write: false, def: "{}" },
    { id: "memory.policy", type: "string", role: "json", read: true, write: false, def: "{}" },
  ];

  /**
   * Erstellt nur fehlende Objekte.
   */
  async function ensureStates() {
    for (const state of stateDefinitions) {
      await adapter.setObjectNotExistsAsync(state.id, {
        type: "state",
        common: {
          name: state.id,
          type: state.type,
          role: state.role,
          read: state.read,
          write: state.write,
          def: state.def,
        },
        native: {},
      });
    }
  }

  /**
   * Hilfsfunktion für Info-States.
   */
  async function setInfo(key, value) {
    await adapter.setStateAsync(`info.${key}`, value, true);
  }

  /**
   * Hilfsfunktion für Meta-States.
   */
  async function setMeta(key, value) {
    await adapter.setStateAsync(`meta.${key}`, value, true);
  }

  return {
    ensureStates,
    setInfo,
    setMeta,
  };
};
