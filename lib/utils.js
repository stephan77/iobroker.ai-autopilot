"use strict";

/**
 * Hilfsfunktionen für das Projekt.
 */
module.exports = () => {
  /**
   * Formatiert eine Zahl sicher als String.
   */
  function safeNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  return {
    safeNumber,
  };
};
