"use strict";

/**
 * GPT-Integration (optional) zur Textanreicherung.
 */
module.exports = (adapter) => {
  /**
   * Ergänzt Aktionen optional durch GPT.
   */
  async function enrichActions(config, actions, stats) {
    if (!config.gpt.enabled || !config.gpt.openaiApiKey) {
      return actions;
    }

    adapter.log.info("GPT ist aktiviert, es erfolgt jedoch keine Änderung der Logik.");
    return actions;
  }

  return {
    enrichActions,
  };
};
