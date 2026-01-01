"use strict";

/**
 * Erzeugt den Live-Kontext anhand der konfigurierten Datenpunkte.
 */
module.exports = (adapter) => {
  /**
   * Liest aktuelle Zustände und baut ein Kontext-Objekt.
   */
  async function collect(config) {
    const context = {
      energy: {},
      temperature: {},
      water: {},
      leaks: [],
      rooms: [],
      raw: {},
    };

    for (const entry of config.dataPoints) {
      if (!entry.enabled) {
        continue;
      }

      const stateValue = await readState(entry.objectId);
      const dailyValue = entry.dailyObjectId ? await readState(entry.dailyObjectId) : null;

      context.raw[entry.objectId] = stateValue;

      const payload = {
        objectId: entry.objectId,
        category: entry.category,
        value: stateValue,
        dailyValue,
        unit: entry.unit,
        description: entry.description,
      };

      if (entry.category.startsWith("energy")) {
        context.energy[entry.category] = payload;
      } else if (entry.category.startsWith("temperature")) {
        context.temperature[entry.category] = payload;
      } else if (entry.category.startsWith("water")) {
        context.water[entry.category] = payload;
      } else if (entry.category === "leak") {
        context.leaks.push(payload);
      } else if (entry.category === "room") {
        context.rooms.push(payload);
      }
    }

    return context;
  }

  /**
   * Liest einen Zustand, unabhängig ob lokal oder fremd.
   */
  async function readState(objectId) {
    if (!objectId) {
      return null;
    }
    try {
      if (objectId.startsWith(`${adapter.namespace}.`)) {
        const state = await adapter.getStateAsync(objectId);
        return state ? state.val : null;
      }
      const foreignState = await adapter.getForeignStateAsync(objectId);
      return foreignState ? foreignState.val : null;
    } catch (error) {
      adapter.log.warn(`Zustand ${objectId} konnte nicht gelesen werden: ${error.message}`);
      return null;
    }
  }

  return {
    collect,
  };
};
