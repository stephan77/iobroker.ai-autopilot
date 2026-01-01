"use strict";

/**
 * Vorschläge (Aktionen) generieren.
 */
module.exports = () => {
  /**
   * Erstellt einfache Vorschläge aus Abweichungen.
   */
  function build(config, stats, deviations) {
    const now = Date.now();
    const actions = [];

    for (const deviation of deviations) {
      actions.push({
        id: `${deviation.objectId}-${now}`,
        category: "deviation",
        type: "suggestion",
        priority: "medium",
        title: "Abweichung erkannt",
        description: `Der Wert von ${deviation.objectId} weicht deutlich vom Durchschnitt ab.`,
        reason: `Aktuell ${deviation.current}, Durchschnitt ${deviation.average}`,
        requiresApproval: true,
        learningKey: deviation.objectId,
        timestamp: new Date().toISOString(),
        status: "proposed",
      });
    }

    return actions;
  }

  return {
    build,
  };
};
