'use strict';

module.exports = () => ({
  async collectLiveData() {
    const data = {
      pvSources: await this.readTableNumbers(this.config.pvSources),
      pvDailySources: await this.readTableNumbers(this.config.pvDailySources),
      consumers: await this.readConsumerTable(),
      rooms: await this.readRoomTable(),
      heaters: await this.readTableNumbers(this.config.heaters),
      temperature: {
        outside: await this.readNumber(this.config.temperature.outside),
        weather: await this.readState(this.config.temperature.weather),
        frostRisk: await this.readState(this.config.temperature.frostRisk)
      },
      windows: await this.readTableStates(this.config.windowContacts),
      water: {
        total: await this.readNumber(this.config.water.total),
        daily: await this.readNumber(this.config.water.daily),
        hotWater: await this.readNumber(this.config.water.hotWater),
        coldWater: await this.readNumber(this.config.water.coldWater),
        boilerTemp: await this.readNumber(this.config.water.boilerTemp),
        circulation: await this.readState(this.config.water.circulation),
        additionalSources: await this.readTableNumbers(this.config.water.additionalSources),
        flowSources: await this.readTableNumbers(this.config.water.flowSources)
      },
      leaks: await this.readTableStates(this.config.leakSensors)
    };

    this.logDebug('Live data collected', data);
    return data;
  },

  async buildContext(liveData, aggregates) {
    await this.loadFeedbackHistory();
    await this.loadLearningHistory();
    await this.loadLearningHistoryEntries();
    this.learningStats = this.aggregateLearningStats(this.learningHistoryEntries);
    const context = {
      timestamp: new Date().toISOString(),
      mode: this.config.mode,
      dryRun: this.config.dryRun,
      summary: this.buildEmptySummary(),
      live: {
        energy: [],
        pv: [],
        water: [],
        temperature: [],
        heaters: []
      },
      history: {
        energy: {},
        water: {},
        temperature: {},
        baselines: {
          houseConsumptionAvg: null,
          houseConsumptionNightAvg: null,
          waterDailyAvg: null,
          temperatureOutsideAvg: null
        },
        deviations: []
      },
      historyDecision: null,
      decisionBasis: null,
      learning: {
        feedback: this.feedbackHistory,
        stats: this.learningStats,
        entries: this.learningHistory
      },
      semantics: {}
    };

    const readSingleValue = async (id) => {
      if (!id) {
        return null;
      }
      try {
        const state = await this.getForeignStateAsync(id);
        if (!state) {
          return null;
        }
        const value = Number(state.val);
        return Number.isFinite(value) ? value : null;
      } catch (error) {
        this.handleError(`Konnte State nicht lesen: ${id}`, error, true);
        return null;
      }
    };

    const singleEnergyMappings = [
      {
        id: this.config.energy?.houseConsumption,
        role: 'houseConsumption',
        description: 'House consumption',
        unit: 'W'
      },
      {
        id: this.config.energy?.gridPower,
        role: 'gridPower',
        description: 'Grid power',
        unit: 'W'
      },
      {
        id: this.config.energy?.batterySoc,
        role: 'batterySoc',
        description: 'Battery SOC',
        unit: '%'
      },
      {
        id: this.config.energy?.batteryPower,
        role: 'batteryPower',
        description: 'Battery power',
        unit: 'W'
      },
      {
        id: this.config.energy?.wallbox,
        role: 'wallbox',
        description: 'Wallbox',
        unit: 'W'
      }
    ];

    for (const entry of singleEnergyMappings) {
      if (!entry.id) {
        continue;
      }
      const value = await readSingleValue(entry.id);
      context.live.energy.push({
        value: value ?? null,
        unit: entry.unit,
        role: entry.role,
        description: entry.description,
        source: 'single'
      });
    }

    for (const src of this.config.energySources || []) {
      if (!src || !src.enabled) {
        continue;
      }
      const value = await this.getForeignStateValue(src.id);
      context.live.energy.push({
        value: value ?? null,
        unit: src.unit || '',
        role: src.role || '',
        description: src.description || ''
      });
    }

    for (const src of this.config.pvSources || []) {
      if (!src || !src.objectId) {
        continue;
      }
      const value = await this.getForeignStateValue(src.objectId);
      context.live.pv.push({
        value: value ?? null,
        unit: src.unit || '',
        role: 'currentPower',
        description: this.buildPvDescription(src)
      });
      if (src.dailyObjectId) {
        const dailyValue = await this.getForeignStateValue(src.dailyObjectId);
        context.live.pv.push({
          value: dailyValue ?? null,
          unit: src.unit || '',
          role: 'dailyEnergy',
          description: this.buildPvDescription(src)
        });
      }
    }

    for (const src of this.config.pvDailySources || []) {
      if (!src || !src.objectId) {
        continue;
      }
      const value = await this.getForeignStateValue(src.objectId);
      context.live.pv.push({
        value: value ?? null,
        unit: src.unit || '',
        role: 'dailyEnergy',
        description: src.description || src.name || ''
      });
    }

    for (const src of this.config.waterSources || []) {
      if (!src || !src.enabled) {
        continue;
      }
      const value = await this.getForeignStateValue(src.id);
      context.live.water.push({
        value: value ?? null,
        unit: src.unit || '',
        role: src.kind || '',
        description: src.description || ''
      });
    }

    const waterMappings = [
      {
        id: this.config.water?.total,
        role: 'waterTotal'
      },
      {
        id: this.config.water?.hot ?? this.config.water?.hotWater,
        role: 'waterHot'
      },
      {
        id: this.config.water?.cold ?? this.config.water?.coldWater,
        role: 'waterCold'
      },
      {
        id: this.config.water?.flow,
        role: 'waterFlow'
      }
    ];

    for (const entry of waterMappings) {
      if (!entry.id) {
        continue;
      }
      const value = await readSingleValue(entry.id);
      context.live.water.push({
        role: entry.role,
        value: value ?? null,
        unit: '',
        source: 'single'
      });
    }

    const roomConfigs = this.config.temperature?.rooms || this.config.rooms || [];
    for (const room of roomConfigs) {
      if (!room) {
        continue;
      }
      const roomEntry = {
        role: 'room',
        name: room.name || ''
      };
      if (room.temperature) {
        roomEntry.temperature = await readSingleValue(room.temperature);
      } else {
        roomEntry.temperature = null;
      }
      if (room.target) {
        roomEntry.target = await readSingleValue(room.target);
      }
      if (room.heatingPower) {
        roomEntry.heatingPower = await readSingleValue(room.heatingPower);
      }
      context.live.temperature.push(roomEntry);
    }

    if (this.config.temperature?.outside) {
      const value = await readSingleValue(this.config.temperature.outside);
      context.live.temperature.push({
        role: 'outside',
        temperature: value ?? null
      });
    }

    for (const heater of this.config.heaters || []) {
      if (!heater || !heater.objectId) {
        continue;
      }
      const value = await this.getForeignStateValue(heater.objectId);
      context.live.heaters.push({
        value: value ?? null,
        unit: heater.unit || '',
        role: heater.type || '',
        description: heater.type || ''
      });
    }

    const historySeries = [...(aggregates.influx || []), ...(aggregates.mysql || [])];
    const historyById = new Map();
    for (const series of historySeries) {
      if (!series || !series.id || !series.aggregate) {
        continue;
      }
      const category = this.getHistoryCategory(series.role);
      context.history[category][series.id] = {
        avg: series.aggregate.avg,
        min: series.aggregate.min,
        max: series.aggregate.max,
        last: series.aggregate.last,
        nightAvg: series.aggregate.nightAvg,
        dayAvg: series.aggregate.dayAvg
      };
      historyById.set(series.id, series.aggregate);
    }

    const houseConsumptionAggregate = historyById.get(this.config.energy?.houseConsumption) || null;
    const waterBaselineId =
      this.config.water?.daily || this.config.water?.total || this.config.water?.flow || null;
    const waterAggregate = waterBaselineId ? historyById.get(waterBaselineId) : null;
    const outsideTempAggregate = historyById.get(this.config.temperature?.outside) || null;

    context.history.baselines.houseConsumptionAvg = houseConsumptionAggregate?.avg ?? null;
    context.history.baselines.houseConsumptionNightAvg = houseConsumptionAggregate?.nightAvg ?? null;
    context.history.baselines.waterDailyAvg = waterAggregate?.avg ?? null;
    context.history.baselines.temperatureOutsideAvg = outsideTempAggregate?.avg ?? null;

    const deviations = [];
    const nowHour = new Date().getHours();
    const isNight = nowHour < this.constants.DAY_START_HOUR || nowHour >= this.constants.NIGHT_START_HOUR;
    const liveHouseConsumption = this.sumLiveRole(context.live.energy, 'houseConsumption');
    if (
      Number.isFinite(liveHouseConsumption) &&
      Number.isFinite(context.history.baselines.houseConsumptionNightAvg) &&
      liveHouseConsumption > 1.5 * context.history.baselines.houseConsumptionNightAvg
    ) {
      deviations.push({
        category: 'energy',
        type: 'peak',
        description: 'House consumption significantly above historical night baseline.',
        severity: 'warn'
      });
    }

    const liveWaterUsage = this.sumLiveRoles(context.live.water, [
      'waterFlow',
      'waterTotal',
      'waterConsumption'
    ]);
    if (
      isNight &&
      Number.isFinite(liveWaterUsage) &&
      waterAggregate?.nightAvg &&
      liveWaterUsage > waterAggregate.nightAvg
    ) {
      deviations.push({
        category: 'water',
        type: 'night',
        description: 'Night water usage above historical baseline.',
        severity: 'warn'
      });
    }

    const batterySocAggregate = historyById.get(this.config.energy?.batterySoc) || null;
    const liveBatterySoc = this.sumLiveRole(context.live.energy, 'batterySoc');
    if (
      Number.isFinite(liveBatterySoc) &&
      Number.isFinite(batterySocAggregate?.avg) &&
      liveBatterySoc < batterySocAggregate.avg - 10
    ) {
      deviations.push({
        category: 'energy',
        type: 'anomaly',
        description: 'Battery SOC below historical average.',
        severity: 'info'
      });
    }

    context.history.deviations = deviations;
    const totalHistoryPoints =
      (aggregates.influx || []).length + (aggregates.mysql || []).length;
    if (totalHistoryPoints === 0) {
      context.history.notice =
        'Keine historischen Daten verfügbar (InfluxDB/SQL). Bitte History-Adapter prüfen.';
    }

    if (this.config.debug) {
      this.log.info(`[DEBUG] LIVE ENERGY CONTEXT: ${JSON.stringify(context.live.energy, null, 2)}`);
      this.log.info(`[DEBUG] LIVE WATER CONTEXT: ${JSON.stringify(context.live.water, null, 2)}`);
      this.log.info(`[DEBUG] LIVE TEMPERATURE CONTEXT: ${JSON.stringify(context.live.temperature, null, 2)}`);
    }

    if (this.isHistoryEnabled()) {
      context.historyDecision = this.buildHistoryDecisionContext({
        houseConsumption: houseConsumptionAggregate,
        batterySoc: batterySocAggregate,
        waterDaily: waterAggregate,
        outsideTemperature: outsideTempAggregate
      });
    }
    context.decisionBasis = {
      history: context.historyDecision,
      explanationHint:
        'Use these historical decision foundations explicitly for reasoning, deviation analysis, and action justification.'
    };

    return context;
  },

  buildPvDescription(src) {
    const parts = [src.name, src.orientation, src.description].filter(Boolean);
    return parts.join(' | ');
  },

  async getForeignStateValue(id) {
    if (!id) {
      return null;
    }
    try {
      const state = await this.getForeignStateAsync(id);
      return state ? state.val : null;
    } catch (error) {
      this.handleError(`Konnte State nicht lesen: ${id}`, error, true);
      return null;
    }
  },

  async readNumber(id) {
    const state = await this.readState(id);
    if (state === null || state === undefined) {
      return null;
    }
    const value = Number(state);
    return Number.isFinite(value) ? value : null;
  },

  async readState(id) {
    if (!id) {
      return null;
    }
    try {
      const isLocal = id.startsWith(`${this.namespace}.`) || !id.includes('.');
      const state = isLocal ? await this.getStateAsync(id) : await this.getForeignStateAsync(id);
      if (this.config.debug) {
        this.log.info(`[DEBUG] readState ${isLocal ? 'local' : 'foreign'}: ${id}`);
      }
      return state ? state.val : null;
    } catch (error) {
      this.handleError(`Konnte State nicht lesen: ${id}`, error, true);
      return null;
    }
  },

  async readTableNumbers(table) {
    if (!Array.isArray(table)) {
      return [];
    }

    const results = [];
    for (const entry of table) {
      const value = await this.readNumber(entry.objectId || entry.id);
      results.push({
        ...entry,
        value
      });
    }

    return results;
  },

  async readConsumerTable() {
    if (!Array.isArray(this.config.consumers)) {
      return [];
    }

    const results = [];
    for (const entry of this.config.consumers) {
      const value = await this.readNumber(entry.objectId);
      const daily = await this.readNumber(entry.dailyObjectId);
      results.push({
        ...entry,
        value,
        daily
      });
    }

    return results;
  },

  async readRoomTable() {
    if (!Array.isArray(this.config.rooms)) {
      return [];
    }

    const results = [];
    for (const entry of this.config.rooms) {
      const temperature = await this.readNumber(entry.temperature);
      const target = await this.readNumber(entry.target);
      const heatingPower = await this.readNumber(entry.heatingPower);
      results.push({
        ...entry,
        temperature,
        target,
        heatingPower
      });
    }

    return results;
  },

  async readTableStates(table) {
    if (!Array.isArray(table)) {
      return [];
    }

    const results = [];
    for (const entry of table) {
      const value = await this.readState(entry.objectId);
      results.push({
        ...entry,
        value
      });
    }

    return results;
  },

  averageNumbers(values) {
    const valid = values.filter((value) => Number.isFinite(value));
    if (valid.length === 0) {
      return null;
    }
    const sum = valid.reduce((acc, value) => acc + value, 0);
    return sum / valid.length;
  },

  sumTableValues(table) {
    if (!Array.isArray(table)) {
      return 0;
    }
    return table.reduce((sum, entry) => sum + (Number(entry.value) || 0), 0);
  },

  buildEmptySummary() {
    return {
      houseConsumption: null,
      gridPower: null,
      batterySoc: null,
      batteryPower: null,
      pvPower: null,
      pvDailyEnergy: null,
      waterConsumption: null,
      wallboxPower: null
    };
  },

  buildEnergySummary(energyEntries) {
    const sumValues = (entries) => {
      const values = (entries || [])
        .map((entry) => Number(entry.value))
        .filter((value) => Number.isFinite(value));
      if (values.length === 0) {
        return null;
      }
      return values.reduce((sum, value) => sum + value, 0);
    };

    const sumRole = (role) =>
      sumValues((energyEntries || []).filter((entry) => entry && entry.role === role));

    const sumRoles = (roles) =>
      sumValues((energyEntries || []).filter((entry) => entry && roles.includes(entry.role)));

    const firstRole = (role) => {
      const entry = (energyEntries || []).find(
        (item) => item && item.role === role && Number.isFinite(Number(item.value))
      );
      return entry ? Number(entry.value) : null;
    };

    return {
      ...this.buildEmptySummary(),
      houseConsumption: sumRole('houseConsumption'),
      gridPower: sumRole('gridPower'),
      batterySoc: firstRole('batterySoc'),
      batteryPower: sumRole('batteryPower'),
      pvPower: sumRole('pvPower'),
      pvDailyEnergy: sumRole('pvDailyEnergy'),
      waterConsumption: sumRoles(['waterTotal', 'waterFlow', 'waterConsumption']),
      wallboxPower: sumRole('wallbox')
    };
  },

  getMissingEnergyValues(energyEntries, energySummary) {
    const roleToSummaryField = {
      houseConsumption: 'houseConsumption',
      gridPower: 'gridPower',
      batterySoc: 'batterySoc',
      batteryPower: 'batteryPower',
      pvPower: 'pvPower',
      pvDailyEnergy: 'pvDailyEnergy',
      waterTotal: 'waterConsumption',
      waterFlow: 'waterConsumption',
      waterConsumption: 'waterConsumption',
      wallbox: 'wallboxPower'
    };

    const roles = new Set(
      (energyEntries || [])
        .map((entry) => entry?.role)
        .filter((role) => role && Object.prototype.hasOwnProperty.call(roleToSummaryField, role))
    );

    const missing = [];
    for (const role of roles) {
      const field = roleToSummaryField[role];
      if (!Number.isFinite(Number(energySummary[field]))) {
        missing.push(role);
      }
    }

    return missing;
  },

  redactContext(context) {
    const clone = JSON.parse(JSON.stringify(context));
    const sections = ['energy', 'pv', 'water', 'temperature', 'heaters'];
    for (const section of sections) {
      for (const entry of clone.live[section] || []) {
        if (Object.prototype.hasOwnProperty.call(entry, 'value')) {
          entry.value = '[redacted]';
        }
      }
    }
    clone.history = {
      energy: '[redacted]',
      water: '[redacted]',
      temperature: '[redacted]',
      baselines: '[redacted]',
      deviations: '[redacted]'
    };
    return clone;
  },

  sumLiveRole(entries, role) {
    const values = (entries || [])
      .filter((entry) => entry && entry.role === role)
      .map((entry) => Number(entry.value))
      .filter((value) => Number.isFinite(value));
    if (values.length === 0) {
      return null;
    }
    return values.reduce((sum, value) => sum + value, 0);
  },

  getLiveRoleValue(entries, role) {
    const entry = (entries || []).find(
      (item) => item && item.role === role && Number.isFinite(Number(item.value))
    );
    return entry ? Number(entry.value) : null;
  },

  getOutsideTemperature(entries) {
    const outsideEntry = (entries || []).find((entry) => entry && entry.role === 'outside');
    const value = outsideEntry?.temperature;
    return Number.isFinite(value) ? value : null;
  },

  getAverageRoomTemperature(entries) {
    const temps = (entries || [])
      .filter((entry) => entry && entry.role === 'room' && Number.isFinite(entry.temperature))
      .map((entry) => entry.temperature);
    if (temps.length === 0) {
      return null;
    }
    const total = temps.reduce((sum, value) => sum + value, 0);
    return total / temps.length;
  },

  sumLiveRoles(entries, roles) {
    const values = (entries || [])
      .filter((entry) => entry && roles.includes(entry.role))
      .map((entry) => Number(entry.value))
      .filter((value) => Number.isFinite(value));
    if (values.length === 0) {
      return null;
    }
    return values.reduce((sum, value) => sum + value, 0);
  }
});
