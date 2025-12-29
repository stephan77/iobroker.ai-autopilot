'use strict';

const utils = require('@iobroker/adapter-core');
const OpenAI = require('openai');

const MINUTE_MS = 60_000;
const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;
const DAY_START_HOUR = 6;
const NIGHT_START_HOUR = 22;
const GPT_LOG_TRIM = 800;

class AiAutopilot extends utils.Adapter {
  constructor(options = {}) {
    super({ ...options, name: 'ai-autopilot' });
    this.on('ready', () => this.onReady());
    this.on('stateChange', (id, state) => this.onStateChange(id, state));
    this.on('unload', (callback) => this.onUnload(callback));

    this.runLock = false;
    this.intervalTimer = null;
    this.pendingActions = null;
    this.openaiClient = null;
  }

  async onReady() {
    await this.ensureStates();
    await this.setStateAsync('info.connection', false, true);
    await this.setStateAsync('info.lastError', '', true);

    if (this.config.debug) {
      this.log.info('[DEBUG] Debug logging enabled');
    }

    if (this.config.openaiApiKey) {
      this.openaiClient = new OpenAI({ apiKey: this.config.openaiApiKey });
    }

    this.subscribeStates('control.run');
    this.subscribeStates('memory.feedback');

    this.startScheduler();

    this.log.info('AI Autopilot v0.5.8 ready');
  }

  onUnload(callback) {
    try {
      if (this.intervalTimer) {
        clearInterval(this.intervalTimer);
        this.intervalTimer = null;
      }
      callback();
    } catch (error) {
      this.log.error(`Unload error: ${error.message}`);
      callback();
    }
  }

  async ensureStates() {
    await this.setObjectNotExistsAsync('control.run', {
      type: 'state',
      common: {
        type: 'boolean',
        role: 'button',
        read: true,
        write: true,
        def: false
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('info.connection', {
      type: 'state',
      common: {
        type: 'boolean',
        role: 'indicator.connected',
        read: true,
        write: false,
        def: false
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('info.lastError', {
      type: 'state',
      common: {
        type: 'string',
        role: 'text',
        read: true,
        write: false,
        def: ''
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('report.last', {
      type: 'state',
      common: {
        type: 'string',
        role: 'text',
        read: true,
        write: false,
        def: ''
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('report.actions', {
      type: 'state',
      common: {
        type: 'string',
        role: 'json',
        read: true,
        write: false,
        def: ''
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('memory.feedback', {
      type: 'state',
      common: {
        type: 'string',
        role: 'text',
        read: true,
        write: true,
        def: ''
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('memory.policy', {
      type: 'state',
      common: {
        type: 'string',
        role: 'text',
        read: true,
        write: true,
        def: ''
      },
      native: {}
    });
  }

  startScheduler() {
    if (this.intervalTimer) {
      clearInterval(this.intervalTimer);
      this.intervalTimer = null;
    }

    if (this.config.mode === 'manual') {
      this.log.info('Manual mode active. Waiting for control.run.');
      return;
    }

    const intervalMin = Number(this.config.intervalMin) || 60;
    const intervalMs = intervalMin * MINUTE_MS;

    this.intervalTimer = setInterval(() => {
      this.runAnalysis('interval').catch((error) => {
        this.handleError('Interval analysis failed', error);
      });
    }, intervalMs);

    this.runAnalysis('startup').catch((error) => {
      this.handleError('Startup analysis failed', error);
    });
  }

  async onStateChange(id, state) {
    if (!state || state.ack) {
      return;
    }

    if (id === `${this.namespace}.control.run` && state.val === true) {
      await this.setStateAsync('control.run', false, true);
      await this.runAnalysis('manual');
      return;
    }

    if (id === `${this.namespace}.memory.feedback`) {
      await this.processFeedback(String(state.val || '').trim());
    }
  }

  async processFeedback(feedback) {
    if (!this.pendingActions) {
      return;
    }

    const normalized = feedback.toUpperCase();
    if (normalized === 'JA') {
      await this.executeActions(this.pendingActions);
      this.pendingActions = null;
      await this.setStateAsync('memory.feedback', '', true);
    } else if (normalized === 'NEIN') {
      this.log.info('Aktionen wurden abgelehnt.');
      this.pendingActions = null;
      await this.setStateAsync('memory.feedback', '', true);
    } else if (normalized.startsWith('ÄNDERN')) {
      this.log.info(`Änderungswunsch: ${feedback}`);
    }
  }

  async runAnalysis(trigger) {
    if (this.runLock) {
      this.log.info('Analyse läuft bereits, neuer Trigger wird übersprungen.');
      return;
    }

    this.runLock = true;
    await this.setStateAsync('info.connection', true, true);

    try {
      const liveData = await this.collectLiveData();
      const historyData = await this.collectHistoryData();
      const aggregates = this.aggregateData(historyData);
      const recommendations = this.generateRecommendations(liveData, aggregates);
      const gptInsights = await this.callOpenAI(liveData, aggregates, recommendations);
      const reportText = this.buildReportText(liveData, aggregates, recommendations, gptInsights);
      const actions = this.buildActions(recommendations, gptInsights);

      await this.setStateAsync('report.last', reportText, true);
      await this.setStateAsync('report.actions', JSON.stringify(actions, null, 2), true);

      if (actions.length > 0) {
        if (this.config.dryRun) {
          await this.sendTelegramMessage('Dry-Run aktiv: Vorschläge liegen vor.', reportText);
        } else {
          await this.requestApproval(actions, reportText);
        }
      }

      await this.setStateAsync('info.lastError', '', true);
    } catch (error) {
      this.handleError('Analyse fehlgeschlagen', error);
    } finally {
      this.runLock = false;
      await this.setStateAsync('info.connection', false, true);
    }
  }

  async collectLiveData() {
    const data = {
      energy: {
        houseConsumption: await this.readNumber(this.config.energy.houseConsumption),
        gridPower: await this.readNumber(this.config.energy.gridPower),
        batterySoc: await this.readNumber(this.config.energy.batterySoc),
        batteryPower: await this.readNumber(this.config.energy.batteryPower),
        wallbox: await this.readNumber(this.config.energy.wallbox)
      },
      pvSources: await this.readTableNumbers(this.config.pvSources),
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
        boilerTemp: await this.readNumber(this.config.water.boilerTemp),
        circulation: await this.readState(this.config.water.circulation)
      },
      leaks: await this.readTableStates(this.config.leakSensors)
    };

    this.logDebug('Live data collected', data);
    return data;
  }

  async collectHistoryData() {
    const influxData = await this.collectHistoryFromConfig(this.config.history.influx, HOUR_MS);
    const mysqlData = await this.collectHistoryFromConfig(this.config.history.mysql, DAY_MS);

    return { influx: influxData, mysql: mysqlData };
  }

  async collectHistoryFromConfig(historyConfig, baseUnitMs) {
    if (!historyConfig.enabled || !historyConfig.instance) {
      return [];
    }

    const now = Date.now();
    const period = historyConfig.periodHours || historyConfig.periodDays || 0;
    const start = now - period * baseUnitMs;
    const end = now;
    const resolutionMin = Number(historyConfig.resolutionMin) || 15;
    const options = {
      start,
      end,
      aggregate: 'average',
      step: resolutionMin * MINUTE_MS
    };

    const results = [];
    for (const datapoint of historyConfig.datapoints || []) {
      if (!datapoint.objectId) {
        continue;
      }
      try {
        const data = await this.requestHistory(historyConfig.instance, datapoint.objectId, options);
        results.push({ ...datapoint, values: data || [] });
      } catch (error) {
        this.handleError(`Historische Daten konnten nicht geladen werden: ${datapoint.objectId}`, error, true);
      }
    }

    return results;
  }

  requestHistory(instance, id, options) {
    return new Promise((resolve, reject) => {
      this.sendTo(instance, 'getHistory', { id, options }, (result) => {
        if (result && result.error) {
          reject(new Error(result.error));
          return;
        }
        resolve((result && result.result) || []);
      });
    });
  }

  aggregateData(historyData) {
    const aggregateSeries = (series) => {
      if (!series || series.length === 0) {
        return null;
      }

      const values = series.map((entry) => Number(entry.val)).filter((value) => Number.isFinite(value));
      if (values.length === 0) {
        return null;
      }

      const sum = values.reduce((acc, value) => acc + value, 0);
      const min = Math.min(...values);
      const max = Math.max(...values);
      const avg = sum / values.length;

      const dayValues = [];
      const nightValues = [];
      for (const entry of series) {
        const timestamp = new Date(entry.ts);
        const hour = timestamp.getHours();
        if (hour >= DAY_START_HOUR && hour < NIGHT_START_HOUR) {
          dayValues.push(Number(entry.val));
        } else {
          nightValues.push(Number(entry.val));
        }
      }

      const dayAvg = this.averageNumbers(dayValues);
      const nightAvg = this.averageNumbers(nightValues);

      return { avg, min, max, dayAvg, nightAvg };
    };

    const aggregated = {
      influx: historyData.influx.map((series) => ({
        ...series,
        aggregate: aggregateSeries(series.values)
      })),
      mysql: historyData.mysql.map((series) => ({
        ...series,
        aggregate: aggregateSeries(series.values)
      }))
    };

    this.logDebug('Aggregations calculated', aggregated);
    return aggregated;
  }

  generateRecommendations(liveData, aggregates) {
    const recommendations = [];

    const pvTotal = liveData.pvSources.reduce((sum, entry) => sum + (entry.value || 0), 0);
    const houseConsumption = liveData.energy.houseConsumption || 0;
    const gridPower = liveData.energy.gridPower || 0;

    if (pvTotal > houseConsumption && gridPower < 0) {
      recommendations.push({
        category: 'energy',
        description: 'PV-Überschuss erkannt. Prüfe verschiebbare Verbraucher oder Heizung.',
        priority: 'high'
      });
    }

    if (liveData.energy.batterySoc !== null && liveData.energy.batterySoc < 20) {
      recommendations.push({
        category: 'energy',
        description: 'Batterie SOC niedrig. Tiefentladung vermeiden.',
        priority: 'high'
      });
    }

    const nightConsumption = this.findAggregate(aggregates, 'consumption', 'nightAvg');
    if (nightConsumption && nightConsumption > 0) {
      recommendations.push({
        category: 'energy',
        description: 'Nachtverbrauch analysieren und Standby-Verbrauch reduzieren.',
        priority: 'medium'
      });
    }

    for (const room of liveData.rooms) {
      if (room.temperature !== null && room.target !== null && room.temperature < room.target - 1) {
        recommendations.push({
          category: 'heating',
          description: `Raum ${room.name}: Temperatur unter Sollwert, Heizung prüfen.`,
          priority: 'medium'
        });
      }
    }

    if (liveData.temperature.outside !== null && liveData.temperature.outside < 2) {
      recommendations.push({
        category: 'heating',
        description: 'Außentemperatur niedrig. Frostschutz prüfen.',
        priority: 'high'
      });
    }

    if (liveData.water.daily !== null && liveData.water.daily > 0) {
      recommendations.push({
        category: 'water',
        description: 'Wasserverbrauch prüfen, unübliche Muster erkennen.',
        priority: 'low'
      });
    }

    if (liveData.leaks.some((leak) => leak.value === true)) {
      recommendations.push({
        category: 'water',
        description: 'Leckage-Sensor meldet Alarm. Sofort prüfen.',
        priority: 'high'
      });
    }

    this.logDebug('Recommendations generated', recommendations);
    return recommendations;
  }

  findAggregate(aggregates, roleHint, field) {
    const series = [...aggregates.influx, ...aggregates.mysql].find((item) =>
      item.role && item.role.includes(roleHint)
    );
    return series && series.aggregate ? series.aggregate[field] : null;
  }

  async callOpenAI(liveData, aggregates, recommendations) {
    if (!this.openaiClient) {
      return null;
    }

    const payload = {
      liveData,
      aggregates,
      recommendations,
      policy: await this.getStateAsync('memory.policy')
    };

    const prompt = `Du bist ein Haus-Autopilot. Analysiere die Daten und gib Empfehlungen mit Begründung. Nenne fehlende Daten explizit.\n\n${JSON.stringify(payload, null, 2)}`;

    try {
      const response = await this.openaiClient.responses.create({
        model: this.config.model || 'gpt-4o',
        input: [
          {
            role: 'user',
            content: [
              {
                type: 'input_text',
                text: prompt
              }
            ]
          }
        ]
      });

      const outputText = response.output_text || this.extractOutputText(response);
      if (this.config.debug) {
        this.log.info(`[DEBUG] GPT request: ${this.trimLog(prompt)}`);
        this.log.info(`[DEBUG] GPT response: ${this.trimLog(outputText || '')}`);
      }
      return outputText || null;
    } catch (error) {
      this.handleError('OpenAI Anfrage fehlgeschlagen', error, true);
      return null;
    }
  }

  extractOutputText(response) {
    if (!response || !response.output) {
      return '';
    }
    const texts = [];
    for (const item of response.output) {
      for (const content of item.content || []) {
        if (content.type === 'output_text') {
          texts.push(content.text);
        }
      }
    }
    return texts.join('\n');
  }

  buildReportText(liveData, aggregates, recommendations, gptInsights) {
    const lines = [];
    lines.push(`Trigger: ${new Date().toISOString()}`);
    lines.push(`Modus: ${this.config.mode}`);
    lines.push(`Dry-Run: ${this.config.dryRun}`);
    lines.push(`PV gesamt: ${liveData.pvSources.reduce((sum, entry) => sum + (entry.value || 0), 0)}`);
    lines.push(`Hausverbrauch: ${liveData.energy.houseConsumption ?? 'n/a'}`);
    lines.push(`Batterie SOC: ${liveData.energy.batterySoc ?? 'n/a'}`);

    if (recommendations.length > 0) {
      lines.push('Empfehlungen:');
      for (const rec of recommendations) {
        lines.push(`- [${rec.priority}] ${rec.description}`);
      }
    } else {
      lines.push('Keine Empfehlungen verfügbar.');
    }

    if (gptInsights) {
      lines.push('GPT Analyse:');
      lines.push(gptInsights);
    }

    return lines.join('\n');
  }

  buildActions(recommendations, gptInsights) {
    const actions = recommendations.map((rec) => ({
      category: rec.category,
      description: rec.description,
      priority: rec.priority,
      status: 'proposed'
    }));

    if (gptInsights) {
      actions.push({
        category: 'gpt',
        description: gptInsights,
        priority: 'info',
        status: 'analysis'
      });
    }

    return actions;
  }

  async requestApproval(actions, reportText) {
    this.pendingActions = actions;
    await this.sendTelegramMessage(
      'Freigabe erforderlich: Antworten mit JA, NEIN oder ÄNDERN: <Text>',
      reportText
    );
  }

  async executeActions(actions) {
    this.log.info(`Aktionen freigegeben: ${actions.length}`);
  }

  async sendTelegramMessage(subject, reportText) {
    if (!this.config.telegram.enabled || !this.config.telegram.instance) {
      return;
    }

    const message = `${subject}\n\n${reportText}`;
    try {
      this.sendTo(this.config.telegram.instance, 'send', {
        text: message,
        chatId: this.config.telegram.chatId || undefined
      });
    } catch (error) {
      this.handleError('Telegram Versand fehlgeschlagen', error, true);
    }
  }

  async readNumber(id) {
    const state = await this.readState(id);
    if (state === null || state === undefined) {
      return null;
    }
    const value = Number(state);
    return Number.isFinite(value) ? value : null;
  }

  async readState(id) {
    if (!id) {
      return null;
    }
    try {
      const state = await this.getStateAsync(id);
      return state ? state.val : null;
    } catch (error) {
      this.handleError(`Konnte State nicht lesen: ${id}`, error, true);
      return null;
    }
  }

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
  }

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
  }

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
  }

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
  }

  averageNumbers(values) {
    const valid = values.filter((value) => Number.isFinite(value));
    if (valid.length === 0) {
      return null;
    }
    const sum = valid.reduce((acc, value) => acc + value, 0);
    return sum / valid.length;
  }

  logDebug(message, payload) {
    if (!this.config.debug) {
      return;
    }
    if (payload !== undefined) {
      this.log.info(`[DEBUG] ${message}: ${JSON.stringify(payload, null, 2)}`);
    } else {
      this.log.info(`[DEBUG] ${message}`);
    }
  }

  trimLog(text) {
    if (!text) {
      return '';
    }
    return text.length > GPT_LOG_TRIM ? `${text.slice(0, GPT_LOG_TRIM)}...` : text;
  }

  handleError(prefix, error, soft = false) {
    const message = `${prefix}: ${error.message}`;
    this.log.error(message);
    if (!soft) {
      this.setStateAsync('info.lastError', message, true).catch(() => {
        this.log.error('info.lastError konnte nicht gesetzt werden');
      });
    }
  }
}

if (module.parent) {
  module.exports = (options) => new AiAutopilot(options);
} else {
  new AiAutopilot();
}
