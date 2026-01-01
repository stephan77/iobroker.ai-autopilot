"use strict";

/**
 * Telegram-Integration (optional).
 * Wird nur aktiv, wenn enabled und token gesetzt sind.
 */
module.exports = (adapter) => {
  let enabled = false;

  /**
   * Initialisiert Telegram, falls gewünscht.
   */
  async function setup(config) {
    enabled = config.telegram.enabled && !!config.telegram.token;
    if (!enabled) {
      adapter.log.info("Telegram ist deaktiviert.");
    } else {
      adapter.log.info("Telegram ist aktiviert.");
    }
  }

  /**
   * Behandelt Aktionen aus der Telegram-UI.
   */
  async function handleAction(msg) {
    if (!enabled) {
      return;
    }
    adapter.log.info(`Telegram-Aktion empfangen: ${JSON.stringify(msg.message)}`);
  }

  /**
   * Stoppt Telegram-bezogene Prozesse.
   */
  async function stop() {
    enabled = false;
  }

  return {
    setup,
    handleAction,
    stop,
  };
};
