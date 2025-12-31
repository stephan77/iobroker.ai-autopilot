'use strict';

const utils = require('@iobroker/adapter-core');
const OpenAI = require('openai');

const MINUTE_MS = 60_000;
const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;
const DAY_START_HOUR = 6;
const NIGHT_START_HOUR = 22;
const GPT_LOG_TRIM = 800;
const DEFAULT_GRID_POWER_THRESHOLD = 500;

class AiAutopilot extends utils.Adapter {
  constructor(options = {}) {
    super({ ...options, name: 'ai-autopilot' });
    this.on('ready', () => this.onReady());
    this.on('stateChange', (id, state) => this.onStateChange(id, state));
    this.on('message', (obj) => this.onMessage(obj));
    this.on('unload', (callback) => this.onUnload(callback));

    this.running = false;
    this.intervalTimer = null;
    this.dailyReportTimer = null;
    this.pendingActions = null;
    this.openaiClient = null;
    this.feedbackHistory = [];
    this.lastContextSummary = null;
    this.awaitingTelegramInput = false;
  }

  async onReady() {
    try {
      await this.ensureStates();
      await this.setStateAsync('info.lastError', '', true);

      if (this.config.debug) {
        this.log.info('[DEBUG] Debug logging enabled');
      }

      if (this.config.openaiApiKey) {
        this.openaiClient = new OpenAI({ apiKey: this.config.openaiApiKey });
      }

      this.subscribeStates('control.run');
      this.subscribeStates('memory.feedback');

      await this.loadFeedbackHistory();
      this.startScheduler();

      this.log.info('AI Autopilot v0.5.8 ready');
    } catch (error) {
      this.handleError('Adapter-Start fehlgeschlagen', error);
    } finally {
      await this.setStateAsync('info.connection', true, true);
    }
  }

  onUnload(callback) {
    try {
      if (this.intervalTimer) {
        clearInterval(this.intervalTimer);
        this.intervalTimer = null;
      }
      if (this.dailyReportTimer) {
        clearInterval(this.dailyReportTimer);
        this.dailyReportTimer = null;
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

    await this.setObjectNotExistsAsync('report.dailyLastSent', {
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

    this.startDailyReportScheduler();

    if (this.config.mode === 'manual') {
      this.log.info('Manual mode active. Waiting for control.run.');
      return;
    }

    const intervalMin = Number(this.config.intervalMin) || 60;
    const intervalMs = intervalMin * MINUTE_MS;

    this.intervalTimer = setInterval(() => {
      this.runAnalysisWithLock('interval').catch((error) => {
        this.handleError('Interval analysis failed', error);
      });
    }, intervalMs);

    this.runAnalysisWithLock('startup').catch((error) => {
      this.handleError('Startup analysis failed', error);
    });
  }

  startDailyReportScheduler() {
    if (this.dailyReportTimer) {
      clearInterval(this.dailyReportTimer);
      this.dailyReportTimer = null;
    }

    this.dailyReportTimer = setInterval(() => {
      this.runDailyReportIfDue().catch((error) => {
        this.handleError('Daily report failed', error, true);
      });
    }, MINUTE_MS);

    this.runDailyReportIfDue().catch((error) => {
      this.handleError('Daily report failed', error, true);
    });
  }

  async runDailyReportIfDue() {
    if (!this.config.telegram?.enabled) {
      return;
    }

    const now = new Date();
    const schedule = this.getDailyReportSchedule();
    if (now.getHours() < schedule.hour || (now.getHours() === schedule.hour && now.getMinutes() < schedule.minute)) {
      return;
    }

    const todayStamp = this.formatLocalDateStamp(now);
    const lastSent = await this.readState('report.dailyLastSent');
    if (lastSent === todayStamp) {
      return;
    }

    const report = await this.buildDailyReport();
    if (!report) {
      return;
    }

    if (this.config.debug) {
      this.log.info(`[DEBUG] Sending daily Telegram report for ${todayStamp}`);
    }

    try {
      await this.sendTelegramMessage(report, { parseMode: 'Markdown' });
      await this.setStateAsync('report.dailyLastSent', todayStamp, true);
    } catch (error) {
      this.handleError('Daily report send failed', error, true);
    }
  }

  getDailyReportSchedule() {
    return { hour: 7, minute: 0 };
  }

  formatLocalDateStamp(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  async buildDailyReport() {
    const liveData = await this.collectLiveData();
    const historyData = await this.collectHistoryData();
    const houseSeries = this.findHistorySeries(historyData, this.config.energy?.houseConsumption, ['consumption']);
    const gridSeries = this.findHistorySeries(historyData, this.config.energy?.gridPower, ['grid']);
    const batterySeries = this.findHistorySeries(historyData, this.config.energy?.batterySoc, ['battery', 'soc']);
    const waterSeries = this.findHistorySeries(historyData, this.getWaterBaselineId(), ['water']);
    const outsideSeries = this.findHistorySeries(historyData, this.config.temperature?.outside, ['outside', 'temp']);

    const houseStats = this.computeSeriesStats(houseSeries?.values);
    const batteryStats = this.computeSeriesStats(batterySeries?.values);
    const waterStats = this.computeSeriesStats(waterSeries?.values);
    const outsideStats = this.computeSeriesStats(outsideSeries?.values);
    const insideAvgTemp = this.averageNumbers(
      (liveData.rooms || [])
        .map((room) => room.temperature)
        .filter((temp) => Number.isFinite(temp))
    );

    const pvEnergyTotal = await this.sumDailyPvEnergy(liveData);
    const gridImportKwh = this.calculateGridImportKwh(gridSeries?.values);
    const waterTotal = this.resolveWaterTotal(liveData, waterStats);
    const waterBreakdown = this.resolveWaterBreakdown(liveData);
    const nightWaterWarning = this.isNightWaterUsageWarning(waterSeries?.values);
    const frostDetected = this.isFrostDetected(outsideSeries?.values, liveData.temperature?.frostRisk);
    const actionSummary = await this.loadActionSummary();

    return this.buildDailyReportText({
      date: new Date(),
      energy: {
        avgConsumption: houseStats.avg,
        peakConsumption: houseStats.max,
        pvEnergyTotal: pvEnergyTotal,
        gridImportKwh: gridImportKwh,
        batteryMin: batteryStats.min,
        batteryMax: batteryStats.max
      },
      water: {
        total: waterTotal,
        hot: waterBreakdown.hot,
        cold: waterBreakdown.cold,
        nightWarning: nightWaterWarning
      },
      climate: {
        insideAvg: insideAvgTemp,
        outsideAvg: outsideStats.avg,
        frostDetected: frostDetected
      },
      actions: actionSummary
    });
  }

  findHistorySeries(historyData, id, roleHints = []) {
    const series = [...(historyData.influx?.series || []), ...(historyData.mysql?.series || [])];
    if (id) {
      const exact = series.find((entry) => entry && entry.id === id);
      if (exact) {
        return exact;
      }
    }

    const normalizedHints = roleHints.map((hint) => String(hint).toLowerCase());
    if (normalizedHints.length > 0) {
      const byRole = series.find((entry) => {
        const role = String(entry?.role || '').toLowerCase();
        return normalizedHints.every((hint) => role.includes(hint));
      });
      if (byRole) {
        return byRole;
      }
    }

    return null;
  }

  computeSeriesStats(values) {
    if (!Array.isArray(values) || values.length === 0) {
      return { avg: null, min: null, max: null };
    }
    const numbers = values.map((entry) => Number(entry.value)).filter((value) => Number.isFinite(value));
    if (numbers.length === 0) {
      return { avg: null, min: null, max: null };
    }
    const sum = numbers.reduce((total, value) => total + value, 0);
    return {
      avg: sum / numbers.length,
      min: Math.min(...numbers),
      max: Math.max(...numbers)
    };
  }

  async sumDailyPvEnergy(liveData) {
    let total = 0;
    if (Array.isArray(liveData.pvDailySources)) {
      total += liveData.pvDailySources.reduce((sum, entry) => sum + (Number(entry.value) || 0), 0);
    }
    for (const src of this.config.pvSources || []) {
      if (!src || !src.dailyObjectId) {
        continue;
      }
      const value = await this.readNumber(src.dailyObjectId);
      if (Number.isFinite(value)) {
        total += value;
      }
    }
    return total > 0 ? total : null;
  }

  calculateGridImportKwh(values) {
    if (!Array.isArray(values) || values.length < 2) {
      return null;
    }
    const sorted = [...values].sort((a, b) => a.ts - b.ts);
    let wattHours = 0;
    for (let i = 0; i < sorted.length - 1; i += 1) {
      const current = sorted[i];
      const next = sorted[i + 1];
      const value = Number(current.value);
      const deltaHours = (next.ts - current.ts) / HOUR_MS;
      if (!Number.isFinite(value) || !Number.isFinite(deltaHours) || deltaHours <= 0) {
        continue;
      }
      if (value > 0) {
        wattHours += value * deltaHours;
      }
    }
    if (!Number.isFinite(wattHours) || wattHours <= 0) {
      return null;
    }
    return wattHours / 1000;
  }

  getWaterBaselineId() {
    return this.config.water?.daily || this.config.water?.total || this.config.water?.flow || null;
  }

  resolveWaterTotal(liveData, waterStats) {
    if (Number.isFinite(liveData.water?.daily)) {
      return liveData.water.daily;
    }
    if (Number.isFinite(liveData.water?.total)) {
      return liveData.water.total;
    }
    if (Number.isFinite(waterStats?.avg)) {
      return waterStats.avg;
    }
    const additional = this.sumTableValues(liveData.water?.additionalSources || []);
    return additional > 0 ? additional : null;
  }

  resolveWaterBreakdown(liveData) {
    return {
      hot: Number.isFinite(liveData.water?.hotWater) ? liveData.water.hotWater : null,
      cold: Number.isFinite(liveData.water?.coldWater) ? liveData.water.coldWater : null
    };
  }

  isNightWaterUsageWarning(values) {
    if (!Array.isArray(values) || values.length === 0) {
      return false;
    }
    const dayValues = [];
    const nightValues = [];
    for (const entry of values) {
      const timestamp = new Date(entry.ts);
      const hour = timestamp.getHours();
      const value = Number(entry.value);
      if (!Number.isFinite(value)) {
        continue;
      }
      if (hour >= DAY_START_HOUR && hour < NIGHT_START_HOUR) {
        dayValues.push(value);
      } else {
        nightValues.push(value);
      }
    }
    const dayAvg = this.averageNumbers(dayValues);
    const nightAvg = this.averageNumbers(nightValues);
    if (!Number.isFinite(nightAvg)) {
      return false;
    }
    if (!Number.isFinite(dayAvg)) {
      return nightAvg > 0;
    }
    return nightAvg > dayAvg * 0.5;
  }

  isFrostDetected(values, frostRiskState) {
    if (frostRiskState === true || frostRiskState === 'true' || frostRiskState === 1) {
      return true;
    }
    if (!Array.isArray(values) || values.length === 0) {
      return false;
    }
    const stats = this.computeSeriesStats(values);
    return Number.isFinite(stats.min) && stats.min <= 0;
  }

  async loadActionSummary() {
    const state = await this.getStateAsync('report.actions');
    if (!state || !state.val) {
      return { proposed: 0, approved: 0, rejected: 0, executed: 0 };
    }
    try {
      const actions = JSON.parse(state.val);
      if (!Array.isArray(actions)) {
        return { proposed: 0, approved: 0, rejected: 0, executed: 0 };
      }
      const summary = { proposed: 0, approved: 0, rejected: 0, executed: 0 };
      for (const action of actions) {
        const status = String(action?.status || 'proposed').toLowerCase();
        summary.proposed += 1;
        if (status === 'approved') {
          summary.approved += 1;
        }
        if (status === 'rejected') {
          summary.rejected += 1;
        }
        if (status === 'executed') {
          summary.executed += 1;
        }
      }
      return summary;
    } catch (error) {
      this.handleError('Daily report actions parsing failed', error, true);
      return { proposed: 0, approved: 0, rejected: 0, executed: 0 };
    }
  }

  buildDailyReportText(data) {
    if (!data) {
      return '';
    }
    const dateLabel = data.date.toLocaleDateString('de-DE');
    const lines = [
      '📊🏠 *Tagesbericht – AI Autopilot*',
      `🗓️ ${dateLabel}`,
      '',
      '⚡ *Energie*',
      `- Ø Verbrauch: ${this.formatWatts(data.energy.avgConsumption)}`,
      `- Spitze: ${this.formatWatts(data.energy.peakConsumption)}`,
      `- PV: ${this.formatKwh(data.energy.pvEnergyTotal)}`,
      `- Netz: ${this.formatKwh(data.energy.gridImportKwh)}`,
      `- Batterie: ${this.formatSocRange(data.energy.batteryMin, data.energy.batteryMax)}`,
      '',
      '💧 *Wasser*',
      `- Gesamt: ${this.formatLiters(data.water.total)}`
    ];

    if (Number.isFinite(data.water.hot)) {
      lines.push(`- Warmwasser: ${this.formatLiters(data.water.hot)}`);
    }
    if (Number.isFinite(data.water.cold)) {
      lines.push(`- Kaltwasser: ${this.formatLiters(data.water.cold)}`);
    }
    if (data.water.nightWarning) {
      lines.push('- ⚠️ Nachtverbrauch erhöht');
    }

    lines.push(
      '',
      '🌡️ *Klima*',
      `- Innen: ${this.formatTemperature(data.climate.insideAvg)}`,
      `- Außen: ${this.formatTemperature(data.climate.outsideAvg)}`
    );

    if (data.climate.frostDetected) {
      lines.push('- ❄️ Frost erkannt');
    }

    lines.push(
      '',
      '🤖 *Aktionen*',
      `- Vorgeschlagen: ${data.actions.proposed}`,
      `- Freigegeben: ${data.actions.approved}`,
      `- Abgelehnt: ${data.actions.rejected}`,
      `- Umgesetzt: ${data.actions.executed}`
    );

    return lines.join('\n');
  }

  async onStateChange(id, state) {
    if (!state) return;

    // IGNORIERE ACK EVENTS
    if (state.ack) return;

    if (id === this.namespace + '.control.run' && state.val === true) {
      if (this.config.debug) {
        this.log.info('[DEBUG] Trigger received: control.run');
      }

      // SOFORT zurücksetzen (Impuls!)
      await this.setStateAsync('control.run', false, true);
      if (this.config.debug) {
        this.log.info('[DEBUG] control.run reset');
      }

      // Mehrfachlauf verhindern
      if (this.running) {
        this.log.warn('Analysis already running, trigger ignored');
        return;
      }

      this.running = true;

      try {
        await this.runAnalysis();   // <-- HIER MUSS GPT AUFGERUFEN WERDEN
      } catch (e) {
        this.log.error('Analysis failed: ' + e.message);
      } finally {
        this.running = false;
      }
    }

    if (id === `${this.namespace}.memory.feedback`) {
      await this.processFeedback(String(state.val || '').trim());
    }
  }

  async onMessage(obj) {
    if (!obj) {
      return;
    }

    if (this.config.debug) {
      this.log.info(`[DEBUG] Telegram message received: ${JSON.stringify(obj)}`);
    }

    const payload = obj.message || obj;
    const callbackData =
      payload.callback_data || payload.data || payload?.message?.data || payload?.message?.callback_data;
    const text = payload.text || payload?.message?.text;

    if (callbackData) {
      await this.handleTelegramCallback(String(callbackData));
      return;
    }

    if (typeof text === 'string' && text.trim().length > 0) {
      await this.handleTelegramText(text.trim());
    }
  }

  async processFeedback(feedback) {
    if (!this.pendingActions) {
      return;
    }

    const normalized = feedback.toUpperCase();
    if (normalized === 'JA') {
      await this.finalizeApproval('approved');
    } else if (normalized === 'NEIN') {
      await this.finalizeApproval('rejected');
    } else if (normalized.startsWith('ÄNDERN')) {
      this.log.info(`Änderungswunsch: ${feedback}`);
    }
  }

  async runAnalysisWithLock(trigger) {
    if (this.running) {
      this.log.warn('Analysis already running, trigger ignored');
      return;
    }

    this.running = true;
    try {
      await this.runAnalysis(trigger);
    } finally {
      this.running = false;
    }
  }

  async runAnalysis(trigger) {
    let reportText = '';
    let finalActions = [];
    let lastErrorMessage = '';
    try {
      if (this.config.debug) {
        this.log.info(`[DEBUG] Config summary: ${JSON.stringify(this.buildConfigSummary())}`);
      }
      const liveData = await this.collectLiveData();
      const historyData = await this.collectHistoryData();
      const aggregates = this.aggregateData(historyData);
      const context = await this.buildContext(liveData, aggregates);
      if (this.config.debug) {
        this.log.info(
          `[DEBUG] History points loaded: ${JSON.stringify({
            influx: historyData.influx?.pointsLoaded || 0,
            mysql: historyData.mysql?.pointsLoaded || 0
          })}`
        );
        this.log.info(
          `[DEBUG] History baselines: ${JSON.stringify(context.history.baselines, null, 2)}`
        );
        this.log.info(
          `[DEBUG] History deviations: ${JSON.stringify(context.history.deviations, null, 2)}`
        );
      }
      let skipAnalysis = false;
      if (context.live.energy.length === 0) {
        reportText = 'No energy sources configured – analysis skipped.';
        skipAnalysis = true;
      }
      let energySummary = this.buildEmptySummary();
      if (!skipAnalysis) {
        energySummary = this.buildEnergySummary(context.live.energy);
        if (this.config.debug) {
          this.log.info(`[DEBUG] Energy summary: ${JSON.stringify(energySummary, null, 2)}`);
        }
        const missingEnergyValues = this.getMissingEnergyValues(context.live.energy, energySummary);
        if (missingEnergyValues.length > 0) {
          reportText = `Energy summary missing values for roles: ${missingEnergyValues.join(', ')}`;
          skipAnalysis = true;
        }
      }

      context.summary = energySummary;
      const liveRuleActions = this.buildLiveRuleActions(context);

      if (!skipAnalysis) {
        this.lastContextSummary = this.buildFeedbackContext(context, energySummary);
        const recommendations = this.generateRecommendations(liveData, aggregates, energySummary);
        if (this.config.debug) {
          this.log.info(`[DEBUG] Context built: ${JSON.stringify(this.redactContext(context))}`);
        }

        let gptInsights = 'OpenAI not configured - GPT analysis skipped.';
        if (this.openaiClient) {
          gptInsights = await this.generateGptInsights(context, energySummary, recommendations);
        }
        reportText = this.buildReportText(liveData, aggregates, recommendations, gptInsights, energySummary);
      }

      const historyDeviationActions = this.buildDeviationActions(context);
      const gptSuggestedActions = await this.refineActionsWithGpt(
        context.history.deviations,
        historyDeviationActions
      );
      finalActions = this.dedupeActions([
        ...liveRuleActions,
        ...historyDeviationActions,
        ...gptSuggestedActions
      ]);

      lastErrorMessage = '';
    } catch (error) {
      this.handleError('Analyse fehlgeschlagen', error);
      if (!reportText) {
        reportText = `Analyse fehlgeschlagen: ${error.message}`;
      }
      lastErrorMessage = `Analyse fehlgeschlagen: ${error.message}`;
    } finally {
      await this.setStateAsync('report.last', reportText, true);
      await this.setStateAsync('report.actions', JSON.stringify(finalActions, null, 2), true);

      if (finalActions.length > 0 && this.config.telegram.enabled) {
        await this.requestApproval(finalActions, reportText);
      }

      await this.setStateAsync('info.lastError', lastErrorMessage, true);
      if (this.config.debug) {
        this.log.info('[DEBUG] Analysis finished');
      }
    }
  }

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
  }

  async collectHistoryData() {
    const influxData = await this.collectHistoryFromConfig(this.config.history.influx, HOUR_MS);
    const mysqlData = await this.collectHistoryFromConfig(this.config.history.mysql, DAY_MS);

    return { influx: influxData, mysql: mysqlData };
  }

  async buildContext(liveData, aggregates) {
    await this.loadFeedbackHistory();
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
      learning: {
        feedback: this.feedbackHistory
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
    const isNight = nowHour < DAY_START_HOUR || nowHour >= NIGHT_START_HOUR;
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

    if (this.config.debug) {
      this.log.info(`[DEBUG] LIVE ENERGY CONTEXT: ${JSON.stringify(context.live.energy, null, 2)}`);
      this.log.info(`[DEBUG] LIVE WATER CONTEXT: ${JSON.stringify(context.live.water, null, 2)}`);
      this.log.info(`[DEBUG] LIVE TEMPERATURE CONTEXT: ${JSON.stringify(context.live.temperature, null, 2)}`);
    }

    return context;
  }

  async collectHistoryFromConfig(historyConfig, baseUnitMs) {
    if (!historyConfig || !historyConfig.enabled || !historyConfig.instance) {
      return { series: [], pointsLoaded: 0 };
    }

    const now = Date.now();
    const period =
      historyConfig.timeframeHours ||
      historyConfig.timeframeDays ||
      historyConfig.periodHours ||
      historyConfig.periodDays ||
      0;
    if (!period) {
      return { series: [], pointsLoaded: 0 };
    }
    const start = now - period * baseUnitMs;
    const end = now;
    const resolutionMin = Number(historyConfig.resolutionMinutes ?? historyConfig.resolutionMin) || 15;
    const options = {
      start,
      end,
      aggregate: 'average',
      step: resolutionMin * MINUTE_MS
    };

    const results = [];
    let pointsLoaded = 0;
    const datapoints =
      Array.isArray(historyConfig.dataPoints) && historyConfig.dataPoints.length > 0
        ? historyConfig.dataPoints
        : historyConfig.datapoints || [];
    for (const datapoint of datapoints) {
      if (datapoint.enabled === false) {
        continue;
      }
      const id = datapoint.id || datapoint.objectId;
      if (!id) {
        continue;
      }
      const data = await this.requestHistoryWithTimeout(historyConfig.instance, id, options);
      const values = this.normalizeHistoryValues(data);
      pointsLoaded += values.length;
      results.push({
        ...datapoint,
        id,
        values
      });
    }

    return { series: results, pointsLoaded };
  }

  buildPvDescription(src) {
    const parts = [src.name, src.orientation, src.description].filter(Boolean);
    return parts.join(' | ');
  }

  buildConfigSummary() {
    return {
      mode: this.config.mode,
      dryRun: this.config.dryRun,
      energySources: (this.config.energySources || []).length,
      pvSources: (this.config.pvSources || []).length,
      pvDailySources: (this.config.pvDailySources || []).length,
      waterSources: (this.config.waterSources || []).length,
      rooms: (this.config.rooms || []).length,
      heaters: (this.config.heaters || []).length,
      history: {
        influx: Boolean(this.config.history?.influx?.enabled),
        mysql: Boolean(this.config.history?.mysql?.enabled)
      }
    };
  }

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
  }

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

  async requestHistoryWithTimeout(instance, id, options, timeoutMs = 10000) {
    let timeoutId;
    const timeoutPromise = new Promise((resolve) => {
      timeoutId = setTimeout(() => resolve({ timeout: true, data: [] }), timeoutMs);
    });

    try {
      const result = await Promise.race([
        this.requestHistory(instance, id, options).then((data) => ({ timeout: false, data })),
        timeoutPromise
      ]);

      if (result.timeout) {
        this.log.warn(`Historische Datenabfrage Timeout (${timeoutMs} ms): ${id}`);
        if (this.config.debug) {
          this.log.info(`[DEBUG] History request timed out for ${id}`);
        }
        return [];
      }

      return result.data || [];
    } catch (error) {
      this.log.warn(`Historische Daten konnten nicht geladen werden: ${id} (${error.message})`);
      return [];
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  aggregateData(historyData) {
    const aggregateSeries = (series) => {
      if (!series || series.length === 0) {
        return {
          avg: null,
          min: null,
          max: null,
          last: null,
          dayAvg: null,
          nightAvg: null
        };
      }

      const values = series.map((entry) => Number(entry.value)).filter((value) => Number.isFinite(value));
      if (values.length === 0) {
        return {
          avg: null,
          min: null,
          max: null,
          last: null,
          dayAvg: null,
          nightAvg: null
        };
      }

      const sum = values.reduce((acc, value) => acc + value, 0);
      const min = Math.min(...values);
      const max = Math.max(...values);
      const avg = sum / values.length;
      const last = series.reduce((latest, entry) => {
        if (!latest || entry.ts > latest.ts) {
          return entry;
        }
        return latest;
      }, null);

      const dayValues = [];
      const nightValues = [];
      for (const entry of series) {
        const timestamp = new Date(entry.ts);
        const hour = timestamp.getHours();
        if (hour >= DAY_START_HOUR && hour < NIGHT_START_HOUR) {
          dayValues.push(Number(entry.value));
        } else {
          nightValues.push(Number(entry.value));
        }
      }

      const dayAvg = this.averageNumbers(dayValues);
      const nightAvg = this.averageNumbers(nightValues);

      return { avg, min, max, last: last ? Number(last.value) : null, dayAvg, nightAvg };
    };

    const aggregated = {
      influx: (historyData.influx?.series || []).map((series) => ({
        ...series,
        aggregate: aggregateSeries(series.values)
      })),
      mysql: (historyData.mysql?.series || []).map((series) => ({
        ...series,
        aggregate: aggregateSeries(series.values)
      }))
    };

    this.logDebug('Aggregations calculated', aggregated);
    return aggregated;
  }

  generateRecommendations(liveData, aggregates, energySummary) {
    const recommendations = [];

    const pvTotal = energySummary.pvPower || 0;
    const gridPower = energySummary.gridPower || 0;
    const houseConsumption = energySummary.houseConsumption || 0;
    const batterySoc = energySummary.batterySoc;

    if (pvTotal > houseConsumption && gridPower < 0) {
      recommendations.push({
        category: 'energy',
        description: 'PV-Überschuss erkannt. Prüfe verschiebbare Verbraucher oder Heizung.',
        priority: 'high'
      });
    }

    if (batterySoc !== null && batterySoc < 20) {
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

  async withTimeout(promise, timeoutMs, label) {
    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        const error = new Error(`${label} timeout after ${timeoutMs} ms`);
        error.code = 'ETIMEDOUT';
        reject(error);
      }, timeoutMs);
    });

    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  async refineActionsWithGpt(deviations, actions) {
    if (!this.openaiClient || actions.length === 0) {
      return actions;
    }

    const payload = {
      deviations,
      actions: actions.map((action) => ({
        id: action.id,
        category: action.category,
        title: action.title,
        description: action.description,
        reason: action.reason,
        severity: action.severity,
        deviationRef: action.deviationRef
      }))
    };

    const prompt =
      'Du bist ein Assistent für einen Haus-Autopiloten. Verfeinere ausschließlich die Wortwahl ' +
      'der Aktionsfelder title, description und reason. Erfinde keine neuen Aktionen, ändere keine IDs, ' +
      'Kategorien, Severity, Status oder Referenzen. Gib ausschließlich ein JSON-Array zurück, in dem ' +
      'jede Zeile ein Objekt mit id, title, description, reason enthält.\n\n' +
      JSON.stringify(payload, null, 2);

    try {
      if (this.config.debug) {
        this.log.info('[DEBUG] GPT action refinement request sent');
      }
      const response = await this.withTimeout(
        this.openaiClient.responses.create({
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
        }),
        15000,
        'GPT action refinement'
      );

      const outputText = response.output_text || this.extractOutputText(response);
      if (this.config.debug) {
        this.log.info(`[DEBUG] GPT refinement request: ${this.trimLog(prompt)}`);
        this.log.info(`[DEBUG] GPT refinement response: ${this.trimLog(outputText || '')}`);
      }

      const refinements = this.parseJsonArray(outputText);
      if (!Array.isArray(refinements)) {
        return actions;
      }

      const refinementMap = new Map();
      for (const entry of refinements) {
        if (!entry || typeof entry.id !== 'string') {
          continue;
        }
        refinementMap.set(entry.id, entry);
      }

      const updatedActions = actions.map((action) => {
        const refinement = refinementMap.get(action.id);
        if (!refinement) {
          return action;
        }
        return {
          ...action,
          title: typeof refinement.title === 'string' ? refinement.title : action.title,
          description: typeof refinement.description === 'string' ? refinement.description : action.description,
          reason: typeof refinement.reason === 'string' ? refinement.reason : action.reason
        };
      });

      this.logDebug('GPT action refinement applied', updatedActions);
      return updatedActions;
    } catch (error) {
      this.log.warn(`GPT Aktionstext-Verfeinerung fehlgeschlagen: ${error.message}`);
      return actions;
    }
  }

  async generateGptInsights(context, energySummary, recommendations) {
    if (!this.openaiClient) {
      return 'OpenAI not configured - GPT analysis skipped.';
    }

    const prompt =
      'Du bist ein Assistent für einen Haus-Autopiloten. ' +
      'Gib eine kurze, prägnante Zusammenfassung (max. 3 Sätze) mit den wichtigsten Beobachtungen ' +
      'zu Energie-, Wasser- und Temperaturdaten. Keine Aufzählungen. ' +
      'Consider previous approved and rejected actions to adapt recommendations.\n\n' +
      JSON.stringify(
        {
          context: {
            summary: energySummary,
            live: {
              energy: context.live.energy,
              water: context.live.water,
              temperature: context.live.temperature
            },
            recommendations,
            learning: {
              feedback: context.learning?.feedback || []
            }
          },
        },
        null,
        2
      );

    try {
      if (this.config.debug) {
        this.log.info('[DEBUG] GPT insights request sent');
      }
      const response = await this.withTimeout(
        this.openaiClient.responses.create({
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
        }),
        15000,
        'GPT insights'
      );

      const outputText = response.output_text || this.extractOutputText(response);
      if (this.config.debug) {
        this.log.info(`[DEBUG] GPT insights response: ${this.trimLog(outputText || '')}`);
      }
      return outputText && outputText.trim()
        ? outputText.trim()
        : 'GPT insights not available.';
    } catch (error) {
      this.log.warn(`GPT Insights fehlgeschlagen: ${error.message}`);
      return 'GPT insights not available.';
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

  parseJsonArray(text) {
    if (!text || typeof text !== 'string') {
      return null;
    }
    const match = text.match(/\[[\s\S]*\]/);
    if (!match) {
      return null;
    }
    try {
      return JSON.parse(match[0]);
    } catch (error) {
      this.handleError('GPT JSON konnte nicht geparst werden', error, true);
      return null;
    }
  }

  buildReportText(liveData, aggregates, recommendations, gptInsights, energySummary) {
    const lines = [];
    lines.push(`Trigger: ${new Date().toISOString()}`);
    lines.push(`Modus: ${this.config.mode}`);
    lines.push(`Dry-Run: ${this.config.dryRun}`);
    lines.push(`PV gesamt: ${energySummary.pvPower}`);
    lines.push(`PV Tagesenergie: ${energySummary.pvDailyEnergy}`);
    lines.push(`Hausverbrauch: ${energySummary.houseConsumption}`);
    lines.push(`Batterie SOC: ${energySummary.batterySoc ?? 'n/a'}`);
    if (liveData.water.hotWater !== null || liveData.water.coldWater !== null) {
      lines.push(`Warmwasser Verbrauch: ${liveData.water.hotWater ?? 'n/a'}`);
      lines.push(`Kaltwasser Verbrauch: ${liveData.water.coldWater ?? 'n/a'}`);
    }
    if (liveData.water.additionalSources.length > 0) {
      lines.push(`Weitere Wasserquellen: ${this.sumTableValues(liveData.water.additionalSources)}`);
    }
    if (liveData.water.flowSources.length > 0) {
      lines.push(`Wasserströmung gesamt: ${this.sumTableValues(liveData.water.flowSources)}`);
    }

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

  buildDeviationActions(context) {
    const deviations = Array.isArray(context.history?.deviations) ? context.history.deviations : [];
    const baseId = Date.now();
    let index = 1;
    const actions = [];

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
        title: mapping.title,
        description: mapping.description,
        reason: mapping.reason,
        severity: this.mapDeviationSeverity(deviation.severity),
        status: 'proposed',
        source: 'deviation',
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
  }

  buildLiveRuleActions(context) {
    const actions = [];
    const baseId = Date.now();
    let index = 1;
    const batterySoc = context?.summary?.batterySoc;
    const outsideTemp = this.getOutsideTemperature(context?.live?.temperature || []);
    const gridPower = context?.summary?.gridPower;
    const pvPower = context?.summary?.pvPower;
    const configuredThreshold = Number(this.config.energy?.gridPowerThreshold);
    const gridThreshold = Number.isFinite(configuredThreshold)
      ? configuredThreshold
      : DEFAULT_GRID_POWER_THRESHOLD;

    if (Number.isFinite(batterySoc) && batterySoc < 20) {
      actions.push({
        id: `${baseId}-${index++}`,
        category: 'battery',
        type: 'protect_battery',
        priority: 'high',
        title: 'Batterie schützen',
        description: 'Batterie-SOC unter 20 %. Entladung reduzieren oder Reserve schützen.',
        reason: `Live-Regel: Batterie-SOC ${batterySoc}% < 20%.`,
        severity: 'high',
        status: 'proposed',
        source: 'live',
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
        severity: 'high',
        status: 'proposed',
        source: 'live',
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
        severity: 'medium',
        status: 'proposed',
        source: 'live',
        deviationRef: 'live:reduce_load'
      });
    }

    this.logDebug('Live rule actions derived', actions);
    return actions;
  }

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
        title: 'Standby-Verbrauch reduzieren',
        description: 'Erhöhter Nachtverbrauch erkannt. Standby-Verbraucher prüfen und reduzieren.',
        reason: deviationDescription
      };
      this.logDebug('Deviation mapping rule fired: energy-night', { deviation, action });
      return action;
    }

    if (category === 'energy' && type === 'peak') {
      const action = {
        category: 'energy',
        title: 'Lastspitzen vermeiden',
        description: 'Hohe Lastspitze erkannt. Flexible Verbraucher zeitlich verschieben.',
        reason: deviationDescription
      };
      this.logDebug('Deviation mapping rule fired: energy-peak', { deviation, action });
      return action;
    }

    if (type === 'anomaly' && Number.isFinite(batterySoc) && batterySoc < 20) {
      const action = {
        category: 'battery',
        title: 'Batterie schützen',
        description: 'Batterie-SOC unter 20 %. Entladung reduzieren oder Reserve schützen.',
        reason: deviationDescription
      };
      this.logDebug('Deviation mapping rule fired: battery-low-soc', { deviation, action });
      return action;
    }

    if (category === 'water' && type === 'night') {
      const action = {
        category: 'water',
        title: 'Mögliche Wasserleckage prüfen',
        description: 'Nächtlicher Wasserverbrauch über Baseline. Leitungen und Geräte prüfen.',
        reason: deviationDescription
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
        title: 'Heizungsregelung prüfen',
        description: 'Außentemperatur höher als Innentemperatur. Heizungsregelung prüfen.',
        reason: deviationDescription
      };
      this.logDebug('Deviation mapping rule fired: heating-inefficiency', { deviation, action });
      return action;
    }

    const fallbackCategory = this.normalizeActionCategory(category);
    const action = {
      category: fallbackCategory,
      title: 'Abweichung prüfen',
      description: 'Eine Abweichung wurde erkannt. Bitte Ursache prüfen.',
      reason: deviationDescription
    };
    this.logDebug('Deviation mapping rule fired: fallback', { deviation, action });
    return action;
  }

  mapDeviationSeverity(severity) {
    switch (severity) {
      case 'critical':
        return 'high';
      case 'warn':
        return 'medium';
      case 'info':
      default:
        return 'info';
    }
  }

  normalizeActionCategory(category) {
    const allowed = new Set(['energy', 'heating', 'water', 'pv', 'battery', 'comfort']);
    if (allowed.has(category)) {
      return category;
    }
    return 'comfort';
  }

  isDuplicateAction(action, existing) {
    return existing.some(
      (entry) =>
        entry.id === action.id ||
        (entry.category === action.category &&
          entry.title === action.title &&
          entry.deviationRef === action.deviationRef)
    );
  }

  dedupeActions(actions) {
    const uniqueMap = new Map();
    for (const action of actions || []) {
      if (!action) {
        continue;
      }
      const key =
        action.id || `${action.category || 'unknown'}:${action.title || 'action'}:${action.deviationRef || ''}`;
      if (uniqueMap.has(key)) {
        this.logDebug('Duplicate action replaced', action);
      }
      uniqueMap.set(key, action);
    }
    return Array.from(uniqueMap.values());
  }

  async requestApproval(actions, reportText) {
    this.pendingActions = actions;
    const approvalText = this.buildApprovalMessage(actions, reportText);
    await this.sendTelegramMessage(approvalText, { includeKeyboard: true, parseMode: 'Markdown' });
  }

  async executeActions(actions) {
    const approvedActions = actions.filter((action) => action.status === 'approved');
    this.log.info(`Aktionen freigegeben: ${approvedActions.length}`);
    const handlers = this.getActionHandlers();

    for (const action of approvedActions) {
      const handler = handlers[action.category];
      try {
        if (!handler) {
          action.executionResult = { status: 'skipped', reason: 'noHandler' };
        } else {
          const result = await handler(action);
          action.executionResult = result || { status: 'success' };
        }
      } catch (error) {
        action.executionResult = { status: 'error', message: error.message };
        this.handleError(`Aktion fehlgeschlagen: ${action.description}`, error, true);
      } finally {
        action.status = 'executed';
      }
    }

    await this.setStateAsync('report.actions', JSON.stringify(actions, null, 2), true);
    await this.storeFeedbackEntries(
      approvedActions.map((action) =>
        this.buildFeedbackEntry(action, 'approved', action.executionResult)
      )
    );
  }

  buildApprovalMessage(actions, reportText) {
    const analysisLabel = this.buildAnalysisLabel(actions);
    const timestamp = new Date();
    const timeLabel = timestamp.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
    const dateLabel = timestamp.toLocaleDateString('de-DE');
    const modeLabel = this.config.mode || 'auto';
    const dryRunLabel = this.config.dryRun ? 'Ja' : 'Nein';
    const lines = [
      '🤖🏠 *AI-Autopilot – Entscheidung erforderlich*',
      '',
      `🕒 *Zeitstempel:* ${dateLabel} ${timeLabel}`,
      `🧪 *Dry-Run:* ${dryRunLabel}`,
      `⚡ *Analyse:* ${analysisLabel}`,
      `⚙️ *Modus:* ${modeLabel}`,
      '',
      '📝 *Zusammenfassung:*',
      '```',
      reportText || 'Keine Zusammenfassung verfügbar.',
      '```',
      '',
      '🔎 *Vorgeschlagene Maßnahmen:*',
      ''
    ];

    for (const action of actions) {
      lines.push(this.formatActionLine(action));
    }

    lines.push('', '_Bitte auswählen:_');
    return lines.join('\n');
  }

  updateActionStatuses(actions, status) {
    for (const action of actions) {
      action.status = status;
    }
  }

  getActionHandlers() {
    return {
      energy: async (action) => this.handleEnergyAction(action),
      heating: async (action) => this.handleHeatingAction(action),
      water: async (action) => this.handleWaterAction(action)
    };
  }

  async handleEnergyAction(action) {
    this.log.info(`Energie-Aktion: ${action.description}`);
    return { status: 'success', message: 'Energie-Aktion protokolliert' };
  }

  async handleHeatingAction(action) {
    this.log.info(`Heizungs-Aktion: ${action.description}`);
    return { status: 'success', message: 'Heizungs-Aktion protokolliert' };
  }

  async handleWaterAction(action) {
    this.log.info(`Wasser-Aktion: ${action.description}`);
    return { status: 'success', message: 'Wasser-Aktion protokolliert' };
  }

  async sendTelegramMessage(text, options = {}) {
    if (!this.config.telegram.enabled || !this.config.telegram.instance) {
      if (this.config.debug && this.config.telegram.enabled) {
        this.log.info('[DEBUG] Telegram enabled but instance missing.');
      }
      return;
    }

    if (!this.config.telegram.chatId) {
      this.log.warn('Telegram chatId fehlt. Nachricht wird ohne chatId gesendet.');
    }

    const { includeKeyboard = false, parseMode } = options;

    try {
      if (this.config.debug) {
        this.log.info(`[DEBUG] Telegram send: ${text}`);
      }
      const payload = {
        text,
        chatId: this.config.telegram.chatId || undefined
      };
      if (parseMode) {
        payload.parse_mode = parseMode;
      }

      if (includeKeyboard) {
        payload.reply_markup = {
          inline_keyboard: [
            [{ text: '✅ JA', callback_data: 'approve_all' }],
            [{ text: '❌ NEIN', callback_data: 'reject_all' }],
            [{ text: '✏️ ÄNDERN', callback_data: 'modify' }]
          ]
        };
      }

      this.sendTo(this.config.telegram.instance, 'send', payload);
    } catch (error) {
      this.handleError('Telegram Versand fehlgeschlagen', error, true);
    }
  }

  async handleTelegramCallback(callbackData) {
    if (this.config.debug) {
      this.log.info(`[DEBUG] Telegram callback received: ${callbackData}`);
    }

    if (!this.pendingActions) {
      this.log.warn('Telegram callback received but no pending actions are available.');
      return;
    }

    if (callbackData === 'approve_all') {
      await this.finalizeApproval('approved');
      return;
    }

    if (callbackData === 'reject_all') {
      await this.finalizeApproval('rejected');
      return;
    }

    if (callbackData === 'modify') {
      this.awaitingTelegramInput = true;
      await this.sendTelegramMessage(
        '✏️ Bitte sende deinen Änderungswunsch als Text. Ich speichere ihn als Feedback.'
      );
    }
  }

  async handleTelegramText(text) {
    if (this.awaitingTelegramInput) {
      this.awaitingTelegramInput = false;
      this.log.info(`Änderungswunsch: ${text}`);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Telegram modify text received: ${text}`);
      }
      return;
    }

    if (this.pendingActions) {
      await this.processFeedback(text);
    }
  }

  async finalizeApproval(decision) {
    if (!this.pendingActions) {
      return;
    }

    if (decision === 'approved') {
      this.updateActionStatuses(this.pendingActions, 'approved');
      if (this.config.dryRun) {
        this.log.info('Dry-Run aktiv: Aktionen werden nicht ausgeführt.');
        for (const action of this.pendingActions) {
          action.executionResult = { status: 'skipped', reason: 'dryRun' };
        }
        await this.setStateAsync('report.actions', JSON.stringify(this.pendingActions, null, 2), true);
        await this.storeFeedbackEntries(
          this.pendingActions.map((action) =>
            this.buildFeedbackEntry(action, 'approved', action.executionResult)
          )
        );
      } else {
        await this.executeActions(this.pendingActions);
      }
      this.pendingActions = null;
      return;
    }

    if (decision === 'rejected') {
      this.updateActionStatuses(this.pendingActions, 'rejected');
      for (const action of this.pendingActions) {
        action.executionResult = { status: 'skipped', reason: 'rejected' };
      }
      await this.setStateAsync('report.actions', JSON.stringify(this.pendingActions, null, 2), true);
      await this.storeFeedbackEntries(
        this.pendingActions.map((action) =>
          this.buildFeedbackEntry(action, 'rejected', action.executionResult)
        )
      );
      this.log.info('Aktionen wurden abgelehnt.');
      this.pendingActions = null;
    }
  }

  formatActionLine(action) {
    const severity = String(action.severity || 'info').toUpperCase();
    const categoryEmoji = this.getCategoryEmoji(action.category);
    const emoji = this.getSeverityEmoji(action.severity);
    const title = action.title || 'Aktion';
    const description = action.description ? ` (${action.description})` : '';
    return `- ${categoryEmoji} ${emoji} *${title}*${description} _[${severity}]_`;
  }

  getSeverityEmoji(severity) {
    switch (String(severity || '').toLowerCase()) {
      case 'high':
      case 'critical':
        return '🔥';
      case 'medium':
      case 'warn':
        return '⚠️';
      default:
        return 'ℹ️';
    }
  }

  getCategoryEmoji(category) {
    switch (String(category || '').toLowerCase()) {
      case 'energy':
        return '⚡';
      case 'heating':
        return '🔥';
      case 'water':
        return '💧';
      case 'pv':
        return '☀️';
      case 'battery':
        return '🔋';
      case 'comfort':
        return '🛋️';
      default:
        return '📌';
    }
  }

  buildAnalysisLabel(actions) {
    const categories = new Set((actions || []).map((action) => action.category).filter(Boolean));
    const labelMap = {
      energy: 'Energie',
      heating: 'Heizung',
      water: 'Wasser',
      pv: 'PV',
      battery: 'Batterie',
      comfort: 'Komfort'
    };
    const labels = Array.from(categories)
      .map((category) => labelMap[category] || category)
      .filter(Boolean);
    return labels.length > 0 ? labels.join(' & ') : 'System';
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

  sumTableValues(table) {
    if (!Array.isArray(table)) {
      return 0;
    }
    return table.reduce((sum, entry) => sum + (Number(entry.value) || 0), 0);
  }

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
  }

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
  }

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

  normalizeHistoryValues(data) {
    if (!Array.isArray(data)) {
      return [];
    }
    const values = [];
    for (const entry of data) {
      const ts = Number(entry.ts ?? entry.timestamp ?? entry.time);
      const value = Number(entry.val ?? entry.value);
      if (!Number.isFinite(ts) || !Number.isFinite(value)) {
        continue;
      }
      values.push({ ts, value });
    }
    values.sort((a, b) => a.ts - b.ts);
    return values;
  }

  getHistoryCategory(role) {
    const normalized = String(role || '').toLowerCase();
    if (normalized.includes('water')) {
      return 'water';
    }
    if (normalized.includes('temp') || normalized.includes('temperature') || normalized.includes('outside')) {
      return 'temperature';
    }
    return 'energy';
  }

  sumLiveRole(entries, role) {
    const values = (entries || [])
      .filter((entry) => entry && entry.role === role)
      .map((entry) => Number(entry.value))
      .filter((value) => Number.isFinite(value));
    if (values.length === 0) {
      return null;
    }
    return values.reduce((sum, value) => sum + value, 0);
  }

  getOutsideTemperature(entries) {
    const outsideEntry = (entries || []).find((entry) => entry && entry.role === 'outside');
    const value = outsideEntry?.temperature;
    return Number.isFinite(value) ? value : null;
  }

  getAverageRoomTemperature(entries) {
    const temps = (entries || [])
      .filter((entry) => entry && entry.role === 'room' && Number.isFinite(entry.temperature))
      .map((entry) => entry.temperature);
    if (temps.length === 0) {
      return null;
    }
    const total = temps.reduce((sum, value) => sum + value, 0);
    return total / temps.length;
  }

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

  formatWatts(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    if (Math.abs(value) >= 1000) {
      return `${(value / 1000).toFixed(2)} kW`;
    }
    return `${Math.round(value)} W`;
  }

  formatKwh(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    return `${value.toFixed(1)} kWh`;
  }

  formatLiters(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    return `${Math.round(value)} l`;
  }

  formatTemperature(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    return `${value.toFixed(1)} °C`;
  }

  formatSocRange(minValue, maxValue) {
    const min = Number.isFinite(minValue) ? Math.round(minValue) : null;
    const max = Number.isFinite(maxValue) ? Math.round(maxValue) : null;
    if (min === null && max === null) {
      return 'n/a';
    }
    if (min !== null && max !== null) {
      return `${min}–${max} %`;
    }
    const value = min !== null ? min : max;
    return `${value} %`;
  }

  trimLog(text) {
    if (!text) {
      return '';
    }
    return text.length > GPT_LOG_TRIM ? `${text.slice(0, GPT_LOG_TRIM)}...` : text;
  }

  async loadFeedbackHistory() {
    try {
      const existing = await this.getStateAsync('memory.feedback');
      if (!existing || !existing.val) {
        this.feedbackHistory = [];
        return;
      }
      const parsed = JSON.parse(String(existing.val));
      if (Array.isArray(parsed)) {
        this.feedbackHistory = parsed.slice(-50);
      } else if (parsed && typeof parsed === 'object') {
        this.feedbackHistory = [parsed].slice(-50);
      } else {
        this.feedbackHistory = [];
      }
    } catch (error) {
      this.feedbackHistory = [];
      this.handleError('Feedback Historie konnte nicht geladen werden', error, true);
    }
  }

  buildFeedbackContext(context, energySummary) {
    const timeLabel = new Date(context.timestamp).toLocaleTimeString('de-DE', {
      hour: '2-digit',
      minute: '2-digit'
    });
    const outsideTemp = this.getOutsideTemperature(context?.live?.temperature || []);
    return {
      houseConsumption: energySummary.houseConsumption ?? null,
      pvPower: energySummary.pvPower ?? null,
      batterySoc: energySummary.batterySoc ?? null,
      outsideTemp,
      timeOfDay: timeLabel
    };
  }

  buildFeedbackEntry(action, approvalResult, executionResult) {
    const executed = this.wasActionExecuted(executionResult);
    return {
      actionId: String(action.id ?? ''),
      category: this.normalizeFeedbackCategory(action.category),
      decision: approvalResult === 'approved' ? 'approved' : 'rejected',
      executed,
      timestamp: new Date().toISOString(),
      context: this.lastContextSummary || {
        houseConsumption: null,
        pvPower: null,
        batterySoc: null,
        outsideTemp: null,
        timeOfDay: new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' })
      }
    };
  }

  async storeFeedbackEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }
    try {
      this.feedbackHistory = [...this.feedbackHistory, ...entries].slice(-50);
      await this.setStateAsync('memory.feedback', JSON.stringify(this.feedbackHistory, null, 2), true);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Feedback stored (${entries.length} entries)`);
      }
    } catch (error) {
      this.log.warn(`Feedback konnte nicht gespeichert werden: ${error.message}`);
    }
  }

  wasActionExecuted(executionResult) {
    const status = executionResult?.status;
    return status === 'success' || status === 'error';
  }

  normalizeFeedbackCategory(category) {
    const normalized = String(category || '').toLowerCase();
    const allowed = new Set(['energy', 'water', 'heating', 'pv', 'battery']);
    if (allowed.has(normalized)) {
      return normalized;
    }
    if (this.config.debug) {
      this.log.info(`[DEBUG] Unknown feedback category "${category}", defaulting to energy`);
    }
    return 'energy';
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
