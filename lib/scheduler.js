"use strict";

/**
 * Scheduler für tägliche Reports.
 */
module.exports = (adapter) => {
  let timer = null;

  /**
   * Startet den Scheduler basierend auf Konfiguration.
   */
  async function start(config, onRun) {
    if (!config.scheduler.enabled) {
      adapter.log.info("Scheduler deaktiviert.");
      return;
    }

    const [hours, minutes] = parseTime(config.scheduler.time);
    const intervalMs = 60 * 1000;

    timer = setInterval(async () => {
      const now = new Date();
      if (now.getUTCHours() === hours && now.getUTCMinutes() === minutes) {
        await onRun();
      }
    }, intervalMs);

    adapter.log.info("Scheduler aktiv.");
  }

  /**
   * Stoppt den Scheduler.
   */
  async function stop() {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  }

  /**
   * Parst Uhrzeit im Format HH:MM.
   */
  function parseTime(time) {
    const parts = String(time).split(":");
    const hours = Number(parts[0] || 0);
    const minutes = Number(parts[1] || 0);
    return [hours, minutes];
  }

  return {
    start,
    stop,
  };
};
