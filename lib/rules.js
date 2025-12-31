'use strict';

module.exports = () => ({
  buildDeviationActions(context) {
    const deviations = Array.isArray(context.history?.deviations) ? context.history.deviations : [];
    const baseId = Date.now();
    let index = 1;
    const actions = [];
    const actionContext = this.buildActionContext(context);

    if (this.config.debug) {
      this.log.info(`[DEBUG] Deviations for action mapping: ${JSON.stringify(deviations, null, 2)}`);
    }

    for (const deviation of deviations) {
      const mapping = this.mapDeviationToAction(deviation, context);
      if (!mapping) {
        continue;
      }
      const deviationRef =
        deviation.id || deviation.description || `${deviation.category || 'unknown'}:${deviation.type || 'unknown'}`;
      const action = {
        id: `${baseId}-${index++}`,
        category: mapping.category,
        type: mapping.type,
        target: mapping.target,
        value: mapping.value,
        unit: mapping.unit,
        priority: mapping.priority || this.mapDeviationPriority(deviation.severity),
        source: 'deviation',
        reason: mapping.reason,
        context: actionContext,
        requiresApproval: mapping.requiresApproval ?? true,
        status: 'proposed',
        decision: null,
        timestamps: this.buildActionTimestamps(),
        learningKey: mapping.learningKey || 'deviation_generic',
        title: mapping.title,
        description: mapping.description,
        deviationRef
      };

      if (this.isDuplicateAction(action, actions)) {
        this.logDebug('Duplicate action skipped', action);
        continue;
      }

      actions.push(action);
    }

    this.logDebug('Deviation actions derived', actions);
    return actions;
  },

  buildLiveRuleActions(context) {
    const actions = [];
    const baseId = Date.now();
    let index = 1;
    const batterySoc =
      Number.isFinite(context?.summary?.batterySoc) ? context.summary.batterySoc : this.getLiveRoleValue(context?.live?.energy || [], 'batterySoc');
    const outsideTemp = this.getOutsideTemperature(context?.live?.temperature || []);
    const gridPower =
      Number.isFinite(context?.summary?.gridPower) ? context.summary.gridPower : this.sumLiveRole(context?.live?.energy || [], 'gridPower');
    const pvPower =
      Number.isFinite(context?.summary?.pvPower) ? context.summary.pvPower : this.sumLiveRole(context?.live?.energy || [], 'pvPower');
    const configuredThreshold = Number(this.config.energy?.gridPowerThreshold);
    const gridThreshold = Number.isFinite(configuredThreshold)
      ? configuredThreshold
      : this.constants.DEFAULT_GRID_POWER_THRESHOLD;
    const actionContext = this.buildActionContext(context);

    if (Number.isFinite(batterySoc) && batterySoc < 20) {
      actions.push({
        id: `${baseId}-${index++}`,
        category: 'energy',
        type: 'protect_battery',
        priority: 'high',
        title: 'Batterie schützen',
        description: 'Batterie-SOC unter 20 %. Entladung reduzieren oder Reserve schützen.',
        reason: `Live-Regel: Batterie-SOC ${batterySoc}% < 20%.`,
        context: actionContext,
        requiresApproval: false,
        status: 'proposed',
        decision: null,
        timestamps: this.buildActionTimestamps(),
        source: 'live-rule',
        learningKey: 'battery_low',
        deviationRef: 'live:protect_battery'
      });
    }

    if (Number.isFinite(outsideTemp) && outsideTemp < 0) {
      actions.push({
        id: `${baseId}-${index++}`,
        category: 'heating',
        type: 'check_frost_protection',
        priority: 'high',
        title: 'Frostschutz prüfen',
        description: 'Außentemperatur unter 0 °C. Frostschutz und Heizkreise prüfen.',
        reason: `Live-Regel: Außentemperatur ${outsideTemp} °C < 0 °C.`,
        context: actionContext,
        requiresApproval: false,
        status: 'proposed',
        decision: null,
        timestamps: this.buildActionTimestamps(),
        source: 'live-rule',
        learningKey: 'frost_risk',
        deviationRef: 'live:check_frost_protection'
      });
    }

    if (
      Number.isFinite(gridPower) &&
      Number.isFinite(pvPower) &&
      gridPower > gridThreshold &&
      pvPower === 0
    ) {
      actions.push({
        id: `${baseId}-${index++}`,
        category: 'energy',
        type: 'reduce_load',
        priority: 'medium',
        title: 'Last reduzieren',
        description: 'Hoher Netzbezug ohne PV-Erzeugung. Verbraucher prüfen und reduzieren.',
        reason: `Live-Regel: Netzbezug ${gridPower} W > ${gridThreshold} W bei PV 0.`,
        context: actionContext,
        requiresApproval: true,
        status: 'proposed',
        decision: null,
        timestamps: this.buildActionTimestamps(),
        source: 'live-rule',
        learningKey: 'grid_peak',
        deviationRef: 'live:reduce_load'
      });
    }

    this.logDebug('Live rule actions derived', actions);
    return actions;
  },

  mapDeviationToAction(deviation, context) {
    const type = deviation?.type;
    const category = deviation?.category;
    const deviationDescription = deviation?.description || 'Abweichung erkannt.';
    const batterySoc = context?.summary?.batterySoc;
    const outsideTemp = this.getOutsideTemperature(context?.live?.temperature || []);
    const insideTemp = this.getAverageRoomTemperature(context?.live?.temperature || []);

    if (category === 'energy' && type === 'night') {
      const action = {
        category: 'energy',
        type: 'reduce_standby',
        priority: 'medium',
        title: 'Standby-Verbrauch reduzieren',
        description: 'Erhöhter Nachtverbrauch erkannt. Standby-Verbraucher prüfen und reduzieren.',
        reason: deviationDescription,
        requiresApproval: true,
        learningKey: 'energy_night'
      };
      this.logDebug('Deviation mapping rule fired: energy-night', { deviation, action });
      return action;
    }

    if (category === 'energy' && type === 'peak') {
      const action = {
        category: 'energy',
        type: 'avoid_peak',
        priority: 'high',
        title: 'Lastspitzen vermeiden',
        description: 'Hohe Lastspitze erkannt. Flexible Verbraucher zeitlich verschieben.',
        reason: deviationDescription,
        requiresApproval: true,
        learningKey: 'energy_peak'
      };
      this.logDebug('Deviation mapping rule fired: energy-peak', { deviation, action });
      return action;
    }

    if (type === 'anomaly' && Number.isFinite(batterySoc) && batterySoc < 20) {
      const action = {
        category: 'energy',
        type: 'protect_battery',
        priority: 'high',
        title: 'Batterie schützen',
        description: 'Batterie-SOC unter 20 %. Entladung reduzieren oder Reserve schützen.',
        reason: deviationDescription,
        requiresApproval: true,
        learningKey: 'battery_low'
      };
      this.logDebug('Deviation mapping rule fired: battery-low-soc', { deviation, action });
      return action;
    }

    if (category === 'water' && type === 'night') {
      const action = {
        category: 'water',
        type: 'check_leak',
        priority: 'high',
        title: 'Mögliche Wasserleckage prüfen',
        description: 'Nächtlicher Wasserverbrauch über Baseline. Leitungen und Geräte prüfen.',
        reason: deviationDescription,
        requiresApproval: true,
        learningKey: 'water_night'
      };
      this.logDebug('Deviation mapping rule fired: water-night', { deviation, action });
      return action;
    }

    if (
      category === 'heating' &&
      type === 'anomaly' &&
      Number.isFinite(outsideTemp) &&
      Number.isFinite(insideTemp) &&
      outsideTemp > insideTemp
    ) {
      const action = {
        category: 'heating',
        type: 'check_heating_control',
        priority: 'medium',
        title: 'Heizungsregelung prüfen',
        description: 'Außentemperatur höher als Innentemperatur. Heizungsregelung prüfen.',
        reason: deviationDescription,
        requiresApproval: true,
        learningKey: 'heating_inefficiency'
      };
      this.logDebug('Deviation mapping rule fired: heating-inefficiency', { deviation, action });
      return action;
    }

    const fallbackCategory = this.normalizeActionCategory(category);
    const action = {
      category: fallbackCategory,
      type: 'inspect_deviation',
      priority: this.mapDeviationPriority(deviation?.severity),
      title: 'Abweichung prüfen',
      description: 'Eine Abweichung wurde erkannt. Bitte Ursache prüfen.',
      reason: deviationDescription,
      requiresApproval: true,
      learningKey: 'deviation_generic'
    };
    this.logDebug('Deviation mapping rule fired: fallback', { deviation, action });
    return action;
  },

  mapDeviationPriority(severity) {
    switch (severity) {
      case 'critical':
        return 'high';
      case 'warn':
        return 'medium';
      case 'info':
      default:
        return 'low';
    }
  },

  normalizeActionCategory(category) {
    const normalized = String(category || '').toLowerCase();
    const allowed = new Set(['energy', 'heating', 'water', 'pv', 'safety']);
    if (allowed.has(normalized)) {
      return normalized;
    }
    if (normalized === 'battery') {
      return 'energy';
    }
    return 'energy';
  },

  isDuplicateAction(action, existing) {
    return existing.some(
      (entry) =>
        entry.id === action.id ||
        (entry.category === action.category &&
          entry.type === action.type &&
          entry.learningKey === action.learningKey &&
          entry.deviationRef === action.deviationRef)
    );
  }
});
