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
    this.learningHistory = [];
    this.learningHistoryEntries = [];
    this.learningStats = {};
    this.lastContextSummary = null;
    this.lastHistoryDeviations = [];
    this.awaitingTelegramInput = false;
    this.pendingModifyActionId = null;
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
      this.subscribeStates('memory.learning');
      this.subscribeStates('memory.history');

      await this.loadFeedbackHistory();
      await this.loadLearningHistory();
      await this.loadLearningHistoryEntries();
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
        clearTimeout(this.dailyReportTimer);
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

    await this.setObjectNotExistsAsync('report.actionHistory', {
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

    await this.setObjectNotExistsAsync('memory.learning', {
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

    await this.setObjectNotExistsAsync('memory.history', {
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
      clearTimeout(this.dailyReportTimer);
      this.dailyReportTimer = null;
    }

    const dailyReportConfig = this.getDailyReportConfig();
    if (!dailyReportConfig.enabled) {
      this.logDebug('Daily report scheduler skipped (disabled)');
      return;
    }

    this.logDebug('Daily report scheduler initialized');
    this.scheduleNextDailyReport(dailyReportConfig);
  }

  scheduleNextDailyReport(dailyReportConfig) {
    const schedule = this.getNextDailyReportSchedule(dailyReportConfig);
    if (!schedule) {
      return;
    }
    const delay = Math.max(schedule.nextRun - Date.now(), 1000);
    this.logDebug('Daily report next run calculated', {
      nextRun: schedule.nextRun.toISOString(),
      timeZone: schedule.timeZone || 'system'
    });

    this.dailyReportTimer = setTimeout(() => {
      this.runDailyReportIfDue()
        .catch((error) => {
          this.handleError('Daily report failed', error, true);
        })
        .finally(() => {
          const refreshed = this.getDailyReportConfig();
          if (refreshed.enabled) {
            this.scheduleNextDailyReport(refreshed);
          }
        });
    }, delay);
  }

  async runDailyReportIfDue() {
    const dailyReportConfig = this.getDailyReportConfig();
    if (!dailyReportConfig.enabled) {
      this.logDebug('Daily report skipped (disabled)');
      return;
    }

    if (!this.config.telegram?.enabled) {
      this.logDebug('Daily report skipped (Telegram disabled)');
      return;
    }

    const now = new Date();
    const todayStamp = this.formatDateStamp(now, dailyReportConfig.timezone);
    const lastSent = await this.readState('report.dailyLastSent');
    if (lastSent === todayStamp) {
      this.logDebug('Daily report skipped (already sent)', { date: todayStamp });
      return;
    }

    const report = await this.buildDailyReport(dailyReportConfig, now);
    if (!report) {
      this.logDebug('Daily report skipped (empty report)');
      return;
    }

    try {
      await this.sendTelegramMessage(report, { parseMode: 'Markdown' });
      await this.setStateAsync('report.dailyLastSent', todayStamp, true);
      this.logDebug('Daily report sent', { date: todayStamp });
    } catch (error) {
      this.handleError('Daily report send failed', error, true);
    }
  }

  getDailyReportConfig() {
    const include = this.config.dailyReport?.include || {};
    return {
      enabled: Boolean(this.config.dailyReport?.enabled),
      time: this.config.dailyReport?.time || '08:00',
      timezone: this.normalizeTimeZone(this.config.dailyReport?.timezone),
      include: {
        summary: include.summary !== false,
        actions: include.actions !== false,
        learning: include.learning !== false,
        deviations: include.deviations !== false
      }
    };
  }

  normalizeTimeZone(timeZone) {
    const trimmed = String(timeZone || '').trim();
    if (!trimmed) {
      return null;
    }
    try {
      new Intl.DateTimeFormat('en-US', { timeZone: trimmed }).format(new Date());
      return trimmed;
    } catch (error) {
      this.log.warn(`Ungültige Zeitzone konfiguriert: ${trimmed}. System-Zeitzone wird verwendet.`);
      return null;
    }
  }

  getDailyReportScheduleParts(config) {
    const time = String(config?.time || '08:00');
    const match = time.match(/^(\d{1,2}):(\d{2})$/);
    if (!match) {
      return { hour: 8, minute: 0 };
    }
    const hour = Number(match[1]);
    const minute = Number(match[2]);
    if (!Number.isInteger(hour) || !Number.isInteger(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
      return { hour: 8, minute: 0 };
    }
    return { hour, minute };
  }

  getNextDailyReportSchedule(config) {
    if (!config?.enabled) {
      return null;
    }
    const { hour, minute } = this.getDailyReportScheduleParts(config);
    const now = new Date();
    const timeZone = config.timezone || null;
    let target;

    if (timeZone) {
      const nowParts = this.getDatePartsInTimeZone(now, timeZone);
      target = this.buildDateInTimeZone(
        {
          year: nowParts.year,
          month: nowParts.month,
          day: nowParts.day,
          hour,
          minute,
          second: 0
        },
        timeZone
      );
      if (target <= now) {
        const tomorrowParts = this.getDatePartsInTimeZone(new Date(now.getTime() + DAY_MS), timeZone);
        target = this.buildDateInTimeZone(
          {
            year: tomorrowParts.year,
            month: tomorrowParts.month,
            day: tomorrowParts.day,
            hour,
            minute,
            second: 0
          },
          timeZone
        );
      }
    } else {
      target = new Date(now);
      target.setHours(hour, minute, 0, 0);
      if (target <= now) {
        target.setDate(target.getDate() + 1);
      }
    }

    return { nextRun: target, timeZone };
  }

  getDatePartsInTimeZone(date, timeZone) {
    const formatter = new Intl.DateTimeFormat('en-US', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
    const parts = formatter.formatToParts(date);
    const values = {};
    for (const part of parts) {
      if (part.type !== 'literal') {
        values[part.type] = part.value;
      }
    }
    return {
      year: Number(values.year),
      month: Number(values.month),
      day: Number(values.day),
      hour: Number(values.hour),
      minute: Number(values.minute),
      second: Number(values.second)
    };
  }

  getTimeZoneOffset(date, timeZone) {
    const parts = this.getDatePartsInTimeZone(date, timeZone);
    const utcTime = Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second
    );
    return utcTime - date.getTime();
  }

  buildDateInTimeZone(parts, timeZone) {
    const utcGuess = new Date(Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second));
    const offset = this.getTimeZoneOffset(utcGuess, timeZone);
    return new Date(utcGuess.getTime() - offset);
  }

  formatDateStamp(date, timeZone) {
    const parts = timeZone ? this.getDatePartsInTimeZone(date, timeZone) : {
      year: date.getFullYear(),
      month: date.getMonth() + 1,
      day: date.getDate()
    };
    const month = String(parts.month).padStart(2, '0');
    const day = String(parts.day).padStart(2, '0');
    return `${parts.year}-${month}-${day}`;
  }

  async buildDailyReport(config, now = new Date()) {
    await this.loadLearningHistoryEntries();
    const actionHistory = await this.loadActionHistory();
    const timeZone = config.timezone || null;
    const rangeStart = new Date(now.getTime() - DAY_MS);
    const summary = this.lastContextSummary || this.buildEmptySummary();
    const deviations = Array.isArray(this.lastHistoryDeviations) ? this.lastHistoryDeviations : [];
    const recentActions = this.filterActionsByWindow(actionHistory, rangeStart);
    const recentLearning = this.filterLearningByWindow(this.learningHistoryEntries, rangeStart);

    return this.buildDailyReportText({
      timeZone,
      now,
      rangeStart,
      include: config.include,
      summary,
      deviations,
      actions: this.summarizeActions(recentActions),
      learning: this.summarizeLearning(recentLearning)
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

  async loadActionHistory() {
    const state = await this.getStateAsync('report.actionHistory');
    if (!state || !state.val) {
      return [];
    }
    try {
      const actions = JSON.parse(state.val);
      return Array.isArray(actions) ? actions : [];
    } catch (error) {
      this.handleError('Daily report action history parsing failed', error, true);
      return [];
    }
  }

  filterActionsByWindow(actions, sinceDate) {
    const since = sinceDate.getTime();
    return (actions || []).filter((action) => {
      const timestamp = this.getActionTimestamp(action);
      return Number.isFinite(timestamp) && timestamp >= since;
    });
  }

  filterLearningByWindow(entries, sinceDate) {
    const since = sinceDate.getTime();
    return (entries || []).filter((entry) => {
      const timestamp = Date.parse(entry?.timestamp);
      return Number.isFinite(timestamp) && timestamp >= since;
    });
  }

  getActionTimestamp(action) {
    if (!action) {
      return null;
    }
    const timestamps = action.timestamps || {};
    const candidates = [timestamps.executedAt, timestamps.decidedAt, timestamps.createdAt, action.updatedAt];
    for (const value of candidates) {
      const parsed = Date.parse(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
    return null;
  }

  summarizeActions(actions) {
    const summary = { proposed: 0, approved: 0, rejected: 0, executed: 0 };
    for (const action of actions || []) {
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
  }

  summarizeLearning(entries) {
    const summary = { approved: 0, rejected: 0, executed: 0, modified: 0, total: 0 };
    for (const entry of entries || []) {
      const decision = String(entry?.decision || entry?.userDecision || '').toLowerCase();
      if (!decision) {
        continue;
      }
      summary.total += 1;
      if (decision === 'approved') {
        summary.approved += 1;
      } else if (decision === 'rejected') {
        summary.rejected += 1;
      } else if (decision === 'executed') {
        summary.executed += 1;
      } else if (decision === 'modified') {
        summary.modified += 1;
      }
    }
    return summary;
  }

  buildDailyReportText(data) {
    if (!data) {
      return '';
    }
    const timeZone = data.timeZone || undefined;
    const dateLabel = data.now.toLocaleDateString('de-DE', timeZone ? { timeZone } : undefined);
    const timeLabel = data.now.toLocaleTimeString('de-DE', {
      hour: '2-digit',
      minute: '2-digit',
      ...(timeZone ? { timeZone } : {})
    });
    const rangeStartLabel = data.rangeStart.toLocaleString('de-DE', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      ...(timeZone ? { timeZone } : {})
    });
    const rangeEndLabel = data.now.toLocaleString('de-DE', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      ...(timeZone ? { timeZone } : {})
    });

    const lines = [
      '📊 *Daily Summary*',
      `🗓️ ${dateLabel} · ${timeLabel}`
    ];

    if (data.include.summary) {
      lines.push(
        '',
        '⚡ *Energy overview*',
        `- Hausverbrauch: ${this.formatWatts(data.summary.houseConsumption)}`,
        `- PV-Leistung: ${this.formatWatts(data.summary.pvPower)}`,
        `- Batterie SOC: ${this.formatSocRange(data.summary.batterySoc, data.summary.batterySoc)}`,
        `- Netzbezug: ${this.formatWatts(data.summary.gridPower)}`
      );
    }

    if (data.include.deviations) {
      const deviations = data.deviations || [];
      lines.push('', '⚠️ *Deviations*');
      if (deviations.length === 0) {
        lines.push('- Keine relevanten Abweichungen erkannt');
      } else {
        for (const deviation of deviations.slice(0, 5)) {
          lines.push(
            `- ${deviation.label || deviation.type || 'Abweichung'} (${deviation.description || 'Details verfügbar'})`
          );
        }
      }
    }

    if (data.include.learning) {
      lines.push(
        '',
        '🧠 *Learning feedback*',
        `- Genehmigt: ${data.learning.approved}`,
        `- Abgelehnt: ${data.learning.rejected}`,
        `- Umgesetzt: ${data.learning.executed}`,
        `- Geändert: ${data.learning.modified}`
      );
    }

    if (data.include.actions) {
      lines.push(
        '',
        '✅ *Actions taken / ❌ rejected*',
        `- Vorgeschlagen: ${data.actions.proposed}`,
        `- Freigegeben: ${data.actions.approved}`,
        `- Abgelehnt: ${data.actions.rejected}`,
        `- Umgesetzt: ${data.actions.executed}`
      );
    }

    lines.push(
      '',
      '🕒 *Time range covered*',
      `${rangeStartLabel} → ${rangeEndLabel}`
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
      await this.handleTelegramCallback({ callbackData: String(callbackData), payload });
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
      this.lastHistoryDeviations = Array.isArray(context.history?.deviations) ? context.history.deviations : [];
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
      let energySummary = this.buildEnergySummary(context.live.energy);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Energy summary: ${JSON.stringify(energySummary, null, 2)}`);
      }
      if (!skipAnalysis) {
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
      const refinedHistoryActions = skipAnalysis
        ? historyDeviationActions
        : await this.refineActionsWithGpt(context, historyDeviationActions);
      const gptSuggestedActions = skipAnalysis ? [] : await this.buildGptSuggestedActions(context);
      finalActions = this.dedupeActions([
        ...liveRuleActions,
        ...refinedHistoryActions,
        ...gptSuggestedActions
      ]);

      this.logDebug('Final merged actions', finalActions);

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
      if (this.config.debug) {
        this.log.info(`[DEBUG] Saved actions: ${JSON.stringify(finalActions, null, 2)}`);
      }

      if (finalActions.length > 0) {
        this.logDebug('Telegram send triggered', { actions: finalActions.length });
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
  }

  isHistoryEnabled() {
    return Boolean(this.config.history?.influx?.enabled || this.config.history?.mysql?.enabled);
  }

  buildHistoryDecisionContext(historyAggregates) {
    const pickNumber = (value) => (Number.isFinite(value) ? value : null);
    const hasNumbers = (aggregate) => {
      if (!aggregate) {
        return false;
      }
      return ['avg', 'min', 'max', 'last', 'dayAvg', 'nightAvg'].some((key) =>
        Number.isFinite(aggregate[key])
      );
    };
    const formatPercent = (value) =>
      Number.isFinite(value) ? Math.round(value * 10) / 10 : null;
    const detectTrend = (last, avg) => {
      if (!Number.isFinite(last) || !Number.isFinite(avg) || avg === 0) {
        return 'unknown';
      }
      if (last > avg * 1.05) {
        return 'rising';
      }
      if (last < avg * 0.95) {
        return 'falling';
      }
      return 'stable';
    };

    const houseAggregate = historyAggregates?.houseConsumption || null;
    const batteryAggregate = historyAggregates?.batterySoc || null;
    const waterAggregate = historyAggregates?.waterDaily || null;
    const outsideAggregate = historyAggregates?.outsideTemperature || null;

    const houseAvg = pickNumber(houseAggregate?.avg);
    const houseLast = pickNumber(houseAggregate?.last);
    const houseNightAvg = pickNumber(houseAggregate?.nightAvg);
    const deviationPercent = Number.isFinite(houseLast) && Number.isFinite(houseNightAvg) && houseNightAvg !== 0
      ? formatPercent(((houseLast - houseNightAvg) / houseNightAvg) * 100)
      : null;
    const houseTrend = detectTrend(houseLast, houseAvg);
    let houseDeviationReason = null;
    if (deviationPercent !== null && Math.abs(deviationPercent) >= 5) {
      const direction = deviationPercent > 0 ? 'above' : 'below';
      houseDeviationReason = `Night-time consumption is ${direction} the expected base load by ${Math.abs(
        deviationPercent
      )}%.`;
    }
    let houseSummary = 'No reliable historical house consumption pattern yet.';
    if (houseAvg !== null) {
      houseSummary = houseDeviationReason
        ? houseDeviationReason
        : `House consumption is ${houseTrend} compared to its historical average.`;
    }

    const batteryAvg = pickNumber(batteryAggregate?.avg);
    const batteryLast = pickNumber(batteryAggregate?.last);
    let batteryTrend = 'unknown';
    if (batteryAvg !== null && batteryAvg < 20) {
      batteryTrend = 'persistently_low';
    } else {
      batteryTrend = detectTrend(batteryLast, batteryAvg);
    }
    let batterySummary = 'No reliable battery SOC trend yet.';
    if (batteryAvg !== null) {
      batterySummary =
        batteryTrend === 'persistently_low'
          ? 'Battery SOC has remained critically low on average.'
          : `Battery SOC is ${batteryTrend} compared to its historical average.`;
    }

    const waterAvg = pickNumber(waterAggregate?.avg);
    const waterLast = pickNumber(waterAggregate?.last);
    const waterDeviation =
      waterAvg !== null && waterLast !== null ? Math.round((waterLast - waterAvg) * 10) / 10 : null;
    let waterSummary = 'No reliable daily water usage pattern yet.';
    if (waterAvg !== null) {
      if (waterDeviation !== null && Math.abs(waterDeviation) >= 1) {
        const direction = waterDeviation > 0 ? 'above' : 'below';
        waterSummary = `Daily water usage is ${direction} its historical average.`;
      } else {
        waterSummary = 'Daily water usage aligns with its historical average.';
      }
    }

    const outsideAvg = pickNumber(outsideAggregate?.avg);
    const outsideLast = pickNumber(outsideAggregate?.last);
    const outsideTrend = detectTrend(outsideLast, outsideAvg);
    let outsideSummary = 'No reliable outside temperature trend yet.';
    if (outsideAvg !== null) {
      outsideSummary = `Outside temperature is ${outsideTrend} compared to its historical average.`;
    }

    const historyDecision = {
      energy: {
        houseConsumption: {
          avg: houseAvg,
          dayAvg: pickNumber(houseAggregate?.dayAvg),
          nightAvg: houseNightAvg,
          min: pickNumber(houseAggregate?.min),
          max: pickNumber(houseAggregate?.max),
          last: houseLast,
          deviationPercent,
          deviationReason: houseDeviationReason,
          trend: houseTrend,
          summary: houseSummary
        }
      },
      battery: {
        soc: {
          avg: batteryAvg,
          min: pickNumber(batteryAggregate?.min),
          max: pickNumber(batteryAggregate?.max),
          last: batteryLast,
          trend: batteryTrend,
          summary: batterySummary
        }
      },
      water: {
        daily: {
          avg: waterAvg,
          min: pickNumber(waterAggregate?.min),
          max: pickNumber(waterAggregate?.max),
          deviation: waterDeviation,
          summary: waterSummary
        }
      },
      temperature: {
        outside: {
          avg: outsideAvg,
          min: pickNumber(outsideAggregate?.min),
          max: pickNumber(outsideAggregate?.max),
          trend: outsideTrend,
          summary: outsideSummary
        }
      }
    };

    const hasAnyAggregate =
      hasNumbers(houseAggregate) ||
      hasNumbers(batteryAggregate) ||
      hasNumbers(waterAggregate) ||
      hasNumbers(outsideAggregate);
    if (!hasAnyAggregate) {
      historyDecision.summary =
        'Historical data is enabled but does not yet provide reliable patterns.';
    }

    if (this.config.debug) {
      this.log.info(`[DEBUG] History decision context built: ${JSON.stringify(historyDecision, null, 2)}`);
      this.log.info(
        `[DEBUG] History decision deviations: ${JSON.stringify({ deviationPercent, waterDeviation }, null, 2)}`
      );
      this.log.info(
        `[DEBUG] History decision trends: ${JSON.stringify(
          {
            houseConsumption: houseTrend,
            batterySoc: batteryTrend,
            outsideTemperature: outsideTrend
          },
          null,
          2
        )}`
      );
      this.log.info(
        `[DEBUG] History decision summaries: ${JSON.stringify(
          {
            houseConsumption: houseSummary,
            batterySoc: batterySummary,
            waterDaily: waterSummary,
            outsideTemperature: outsideSummary,
            summary: historyDecision.summary || null
          },
          null,
          2
        )}`
      );
    }

    return historyDecision;
  }

  normalizeHistoryInstance(instance) {
    if (!instance) {
      return null;
    }
    if (instance.startsWith('mysql.')) {
      const mapped = instance.replace(/^mysql\./, 'sql.');
      if (this.config.debug) {
        this.log.info(`[DEBUG] History instance mapped from ${instance} to ${mapped}`);
      }
      return mapped;
    }
    if (instance.startsWith('sql.') || instance.startsWith('influxdb.')) {
      return instance;
    }
    return null;
  }

  async collectHistoryFromConfig(historyConfig, baseUnitMs) {
    if (!historyConfig || !historyConfig.enabled || !historyConfig.instance) {
      return { series: [], pointsLoaded: 0 };
    }

    const instance = this.normalizeHistoryInstance(historyConfig.instance);
    if (!instance) {
      this.log.warn(`History instance "${historyConfig.instance}" nicht unterstützt (nur influxdb.* oder sql.*).`);
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
      const data = await this.requestHistoryWithTimeout(instance, id, options);
      const values = this.normalizeHistoryValues(data);
      pointsLoaded += values.length;
      results.push({
        ...datapoint,
        id,
        values
      });
    }

    if (this.config.debug) {
      this.log.info(`[DEBUG] History source used: ${instance}`);
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

  async refineActionsWithGpt(context, actions) {
    if (!this.openaiClient || actions.length === 0) {
      return actions;
    }
    if (this.isHistoryEnabled() && !context?.decisionBasis?.history) {
      this.log.warn('History enabled but decision basis missing - skipping GPT refinement.');
      return actions;
    }

    const payload = {
      context: {
        summary: context?.summary,
        live: {
          energy: context?.live?.energy || [],
          pv: context?.live?.pv || [],
          water: context?.live?.water || [],
          temperature: context?.live?.temperature || []
        },
        decisionBasis: context?.decisionBasis || {}
      },
      actions: actions.map((action) => ({
        id: action.id,
        category: action.category,
        type: action.type,
        priority: action.priority,
        title: action.title,
        description: action.description,
        reason: action.reason,
        learningKey: action.learningKey
      }))
    };

    const prompt =
      'Du bist ein Assistent für einen Haus-Autopiloten. Verfeinere ausschließlich die Wortwahl ' +
      'der Aktionsfelder title, description und reason. Erfinde keine neuen Aktionen, ändere keine IDs, ' +
      'Kategorien, Prioritäten, Status oder Learning Keys. Gib ausschließlich ein JSON-Array zurück, in dem ' +
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

  async buildGptSuggestedActions(context) {
    if (!this.openaiClient) {
      return [];
    }
    if (this.isHistoryEnabled() && !context?.decisionBasis?.history) {
      this.log.warn('History enabled but decision basis missing - skipping GPT suggestions.');
      return [];
    }

    const actionContext = this.buildActionContext(context);
    const payload = {
      context: {
        summary: context?.summary,
        live: {
          energy: context?.live?.energy || [],
          pv: context?.live?.pv || [],
          water: context?.live?.water || [],
          temperature: context?.live?.temperature || []
        },
        decisionBasis: context?.decisionBasis || {},
        learning: {
          feedback: context?.learning?.feedback || [],
          stats: context?.learning?.stats || {}
        }
      },
      schema: {
        fields: [
          'id',
          'category',
          'type',
          'target',
          'value',
          'unit',
          'priority',
          'reason',
          'requiresApproval',
          'learningKey'
        ]
      }
    };

    const prompt =
      'Du bist ein Assistent für einen Haus-Autopiloten. Schlage zusätzliche Aktionen vor, ' +
      'die auf den Live-Daten basieren. Gib ausschließlich ein JSON-Array zurück. Jede Aktion muss ' +
      'die Felder id, category (energy|heating|water|pv|safety), type, priority (low|medium|high), reason, ' +
      'requiresApproval (boolean) und learningKey enthalten. Optional target, value, unit. ' +
      'Keine Freitexte außerhalb des JSON.\n\n' +
      JSON.stringify(payload, null, 2);

    try {
      if (this.config.debug) {
        this.log.info('[DEBUG] GPT suggested actions request sent');
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
        'GPT suggested actions'
      );

      const outputText = response.output_text || this.extractOutputText(response);
      if (this.config.debug) {
        this.log.info(`[DEBUG] GPT suggested actions response: ${this.trimLog(outputText || '')}`);
      }

      const suggestions = this.parseJsonArray(outputText);
      if (!Array.isArray(suggestions)) {
        return [];
      }

      const baseId = Date.now();
      let index = 1;
      const actions = [];
      for (const entry of suggestions) {
        const normalized = this.normalizeGptAction(entry, actionContext, baseId, index++);
        if (!normalized) {
          continue;
        }
        actions.push(normalized);
      }

      this.logDebug('GPT suggested actions derived', actions);
      return actions;
    } catch (error) {
      this.log.warn(`GPT Action-Vorschläge fehlgeschlagen: ${error.message}`);
      return [];
    }
  }

  async generateGptInsights(context, energySummary, recommendations) {
    if (!this.openaiClient) {
      return 'OpenAI not configured - GPT analysis skipped.';
    }
    if (this.isHistoryEnabled() && !context?.decisionBasis?.history) {
      this.log.warn('History enabled but decision basis missing - skipping GPT insights.');
      return 'GPT insights not available.';
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
              feedback: context.learning?.feedback || [],
              stats: context.learning?.stats || {}
            },
            decisionBasis: context?.decisionBasis || {}
          }
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

  buildActionContext(context) {
    const timestamp = context?.timestamp || new Date().toISOString();
    const summary = context?.summary || {};
    return {
      timestamp,
      batterySoc: summary.batterySoc ?? null,
      houseConsumption: summary.houseConsumption ?? null,
      outsideTemp: this.getOutsideTemperature(context?.live?.temperature || []),
      pvPower: summary.pvPower ?? null
    };
  }

  normalizeActionPriority(priority) {
    switch (String(priority || '').toLowerCase()) {
      case 'high':
        return 'high';
      case 'medium':
        return 'medium';
      default:
        return 'low';
    }
  }

  normalizeGptAction(entry, actionContext, baseId, index) {
    if (!entry || typeof entry !== 'object') {
      return null;
    }
    const type = typeof entry.type === 'string' ? entry.type.trim() : '';
    if (!type) {
      return null;
    }
    const id = typeof entry.id === 'string' && entry.id.trim() ? entry.id : `${baseId}-${index}`;
    const category = this.normalizeActionCategory(entry.category);
    const priority = this.normalizeActionPriority(entry.priority);
    const requiresApproval = typeof entry.requiresApproval === 'boolean' ? entry.requiresApproval : true;
    const learningKey =
      typeof entry.learningKey === 'string' && entry.learningKey.trim()
        ? entry.learningKey
        : `${category}_${type}`;
    const value =
      typeof entry.value === 'number' || typeof entry.value === 'boolean' ? entry.value : undefined;
    const unit = typeof entry.unit === 'string' ? entry.unit : undefined;
    return {
      id,
      category,
      type,
      target: entry.target,
      value,
      unit,
      priority,
      source: 'gpt',
      reason: typeof entry.reason === 'string' ? entry.reason : 'GPT Vorschlag',
      context: actionContext,
      requiresApproval,
      status: 'proposed',
      decision: null,
      timestamps: this.buildActionTimestamps(),
      learningKey,
      title: typeof entry.title === 'string' ? entry.title : this.formatActionTitle(type),
      description: typeof entry.description === 'string' ? entry.description : undefined
    };
  }

  formatActionTitle(type) {
    if (!type) {
      return 'Aktion';
    }
    return String(type).replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
  }

  describeAction(action) {
    return action.reason || action.description || action.type || 'Aktion';
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
  }

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
      : DEFAULT_GRID_POWER_THRESHOLD;
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
  }

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
  }

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
  }

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

  dedupeActions(actions) {
    const uniqueMap = new Map();
    for (const action of actions || []) {
      if (!action) {
        continue;
      }
      const key =
        action.id ||
        `${action.category || 'unknown'}:${action.type || 'action'}:${action.learningKey || ''}:${action.deviationRef || ''}`;
      if (uniqueMap.has(key)) {
        this.logDebug('Duplicate action replaced', action);
      }
      uniqueMap.set(key, action);
    }
    return Array.from(uniqueMap.values());
  }

  async requestApproval(actions, reportText) {
    const sentActions = this.markActionsSent(actions);
    this.pendingActions = sentActions;
    await this.persistActions(sentActions, 'Pending actions saved');
    const approvalText = this.buildApprovalMessage(actions, reportText);
    await this.sendTelegramMessage(approvalText, {
      includeKeyboard: true,
      parseMode: 'Markdown',
      actions: actions
    });
  }

  async executeActions(actions) {
    const approvedActions = actions.filter((action) => action.status === 'approved');
    this.log.info(`Aktionen freigegeben: ${approvedActions.length}`);
    for (const action of approvedActions) {
      await this.executeAction(action);
    }

    await this.persistActions(actions, 'Actions executed');
    await this.storeFeedbackEntries(
      approvedActions.map((action) =>
        this.buildFeedbackEntry(action, 'approved', action.executionResult)
      )
    );
    await this.storeLearningEntries(
      approvedActions.map((action) => this.buildLearningEntry(action, 'approved'))
    );
    await this.storeLearningHistoryEntries(
      approvedActions.map((action) => this.buildLearningHistoryEntry(action, 'executed'))
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
      this.applyActionStatusTransition(action, status);
    }
  }

  markActionsSent(actions) {
    for (const action of actions) {
      const normalized = this.normalizeActionForPersistence(action);
      Object.assign(action, normalized);
    }
    return actions;
  }

  buildTelegramKeyboard(actions) {
    const keyboard = [];
    for (const action of actions || []) {
      if (!action || !action.id) {
        continue;
      }
      keyboard.push([
        { text: '✅ Freigeben', callback_data: `action:${action.id}:approve` },
        { text: '❌ Ablehnen', callback_data: `action:${action.id}:reject` },
        { text: '✏️ Ändern', callback_data: `action:${action.id}:modify` }
      ]);
    }
    return keyboard.length > 0 ? keyboard : [];
  }

  buildActionTimestamps(existing = {}) {
    const now = new Date().toISOString();
    return {
      createdAt: existing.createdAt || now,
      decidedAt: existing.decidedAt || null,
      executedAt: existing.executedAt || null
    };
  }

  normalizeActionForPersistence(action) {
    if (!action || typeof action !== 'object') {
      return action;
    }
    const normalizedStatus = this.normalizeActionStatus(action.status);
    const timestamps = this.buildActionTimestamps(action.timestamps || {});
    const decision = action.decision ?? null;
    return {
      ...action,
      status: normalizedStatus,
      decision,
      timestamps,
      learningKey: action.learningKey || 'unknown'
    };
  }

  normalizeActionStatus(status) {
    const normalized = String(status || 'proposed').toLowerCase();
    if (['proposed', 'approved', 'rejected', 'executed', 'failed'].includes(normalized)) {
      return normalized;
    }
    if (normalized === 'sent') {
      return 'proposed';
    }
    return 'proposed';
  }

  applyActionDecision(action, decision) {
    if (!action) {
      return false;
    }
    const now = new Date().toISOString();
    action.decision = decision;
    action.timestamps = this.buildActionTimestamps(action.timestamps || {});
    action.timestamps.decidedAt = action.timestamps.decidedAt || now;
    this.logDebug('Action decision updated', {
      actionId: action.id,
      decision
    });
    return true;
  }

  applyActionStatusTransition(action, nextStatus) {
    if (!action) {
      return false;
    }
    const currentStatus = this.normalizeActionStatus(action.status);
    const targetStatus = this.normalizeActionStatus(nextStatus);
    if (!this.isActionTransitionAllowed(currentStatus, targetStatus)) {
      this.log.warn(
        `Ungültige Status-Transition für Aktion ${action.id}: ${currentStatus} -> ${targetStatus}`
      );
      return false;
    }

    const now = new Date().toISOString();
    action.status = targetStatus;
    action.timestamps = this.buildActionTimestamps(action.timestamps || {});

    if (targetStatus === 'approved') {
      action.decision = 'approved';
      action.timestamps.decidedAt = action.timestamps.decidedAt || now;
    }

    if (targetStatus === 'rejected') {
      action.decision = 'rejected';
      action.timestamps.decidedAt = action.timestamps.decidedAt || now;
    }

    if (targetStatus === 'executed' || targetStatus === 'failed') {
      action.timestamps.executedAt = action.timestamps.executedAt || now;
    }

    this.logDebug('Action status changed', {
      actionId: action.id,
      from: currentStatus,
      to: targetStatus
    });
    return true;
  }

  isActionTransitionAllowed(currentStatus, nextStatus) {
    const transitions = {
      proposed: ['approved', 'rejected'],
      approved: ['executed', 'failed'],
      rejected: [],
      executed: [],
      failed: []
    };
    if (currentStatus === nextStatus) {
      return true;
    }
    return (transitions[currentStatus] || []).includes(nextStatus);
  }

  async persistActions(actions, debugLabel) {
    try {
      const normalized = (actions || []).map((action) => this.normalizeActionForPersistence(action));
      await this.setStateAsync('report.actions', JSON.stringify(normalized, null, 2), true);
      await this.updateActionHistory(normalized);
      this.logDebug(debugLabel || 'Actions persisted', { count: normalized.length });
    } catch (error) {
      this.handleError('Aktionen konnten nicht gespeichert werden', error, true);
    }
  }

  async updateActionHistory(actions) {
    try {
      const existing = await this.loadActionHistory();
      const historyMap = new Map();
      for (const entry of existing) {
        const key = entry?.id || entry?.actionId || null;
        if (key) {
          historyMap.set(String(key), entry);
        }
      }

      for (const action of actions || []) {
        if (!action) {
          continue;
        }
        const key = action.id || action.actionId;
        if (!key) {
          continue;
        }
        historyMap.set(String(key), { ...action, updatedAt: new Date().toISOString() });
      }

      const merged = Array.from(historyMap.values()).slice(-500);
      await this.setStateAsync('report.actionHistory', JSON.stringify(merged, null, 2), true);
      this.logDebug('Action history updated', { count: merged.length });
    } catch (error) {
      this.handleError('Aktionen-Historie konnte nicht gespeichert werden', error, true);
    }
  }

  async persistLearningForDecision(action, status) {
    if (!action) {
      return;
    }
    const normalizedStatus = this.normalizeActionStatus(status);
    if (normalizedStatus === 'rejected') {
      await this.storeLearningHistoryEntries([this.buildLearningHistoryEntry(action, 'rejected')]);
      await this.storeLearningEntries([this.buildLearningEntry(action, 'rejected')]);
    }
    if (normalizedStatus === 'executed') {
      await this.storeLearningHistoryEntries([this.buildLearningHistoryEntry(action, 'executed')]);
      await this.storeLearningEntries([this.buildLearningEntry(action, 'approved')]);
    }
  }

  updatePendingAction(action) {
    if (!this.pendingActions) {
      return;
    }
    const pending = this.pendingActions.find((entry) => entry && String(entry.id) === String(action.id));
    if (pending) {
      Object.assign(pending, action);
    }
  }

  async storeActionLearningDecision(actionId, decision) {
    const actions = await this.loadActionsFromState();
    const action = actions.find((entry) => entry && String(entry.id) === String(actionId));
    if (!action) {
      return;
    }
    await this.storeLearningEntries([this.buildLearningEntry(action, decision)]);
  }

  getActionHandlers() {
    return {
      energy: async (action) => this.handleEnergyAction(action),
      heating: async (action) => this.handleHeatingAction(action),
      water: async (action) => this.handleWaterAction(action),
      pv: async (action) => this.handlePvAction(action),
      safety: async (action) => this.handleSafetyAction(action)
    };
  }

  async handleEnergyAction(action) {
    this.log.info(`Energie-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Energie-Aktion protokolliert' };
  }

  async handleHeatingAction(action) {
    this.log.info(`Heizungs-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Heizungs-Aktion protokolliert' };
  }

  async handleWaterAction(action) {
    this.log.info(`Wasser-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Wasser-Aktion protokolliert' };
  }

  async handlePvAction(action) {
    this.log.info(`PV-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'PV-Aktion protokolliert' };
  }

  async handleSafetyAction(action) {
    this.log.info(`Sicherheits-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Sicherheits-Aktion protokolliert' };
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

    const { includeKeyboard = false, parseMode, actions = [] } = options;

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
          inline_keyboard: this.buildTelegramKeyboard(actions)
        };
      }

      this.sendTo(this.config.telegram.instance, 'send', payload);
    } catch (error) {
      this.handleError('Telegram Versand fehlgeschlagen', error, true);
    }
  }

  async handleTelegramCallback(input = {}) {
    const callbackData = typeof input === 'string' ? input : input?.callbackData;
    const payload = typeof input === 'object' ? input?.payload : undefined;
    if (this.config.debug) {
      this.log.info(`[DEBUG] Telegram callback received: ${callbackData}`);
    }

    const actionMatch = String(callbackData || '').match(
      /^action:(.+):(approve|reject|modify|approved|rejected|executed)$/
    );
    if (actionMatch) {
      const actionId = actionMatch[1];
      const actionCommand = actionMatch[2];
      await this.handleActionCallback(actionId, actionCommand, payload);
      return;
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
  }

  async handleTelegramText(text) {
    if (this.awaitingTelegramInput) {
      this.awaitingTelegramInput = false;
      const actionId = this.pendingModifyActionId;
      this.pendingModifyActionId = null;
      this.log.info(`Änderungswunsch: ${text}`);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Telegram modify text received: ${text}`);
      }
      if (actionId) {
        await this.storeActionLearningDecision(actionId, 'modified');
      } else if (this.pendingActions) {
        await this.storeLearningEntries(
          this.pendingActions.map((action) => this.buildLearningEntry(action, 'modified'))
        );
      }
      return;
    }

    if (this.pendingActions) {
      await this.processFeedback(text);
    }
  }

  async loadActionsFromState() {
    const state = await this.getStateAsync('report.actions');
    if (!state || !state.val) {
      return [];
    }
    try {
      const parsed = JSON.parse(String(state.val));
      if (!Array.isArray(parsed)) {
        return [];
      }
      return parsed.map((action) => this.normalizeActionForPersistence(action));
    } catch (error) {
      this.handleError('Gespeicherte Aktionen konnten nicht geparst werden', error, true);
      return [];
    }
  }

  async updateActionStatusInState(actionId, status) {
    const actions = await this.loadActionsFromState();
    if (actions.length === 0) {
      this.log.warn('Keine gespeicherten Aktionen für Status-Update gefunden.');
      return;
    }
    const action = actions.find((entry) => entry && String(entry.id) === String(actionId));
    if (!action) {
      this.log.warn(`Aktion nicht gefunden für Status-Update: ${actionId}`);
      return;
    }

    const transitionApplied = this.applyActionStatusTransition(action, status);
    if (!transitionApplied) {
      return;
    }
    await this.persistActions(actions, 'Action status updated');
    if (this.pendingActions) {
      const pending = this.pendingActions.find((entry) => entry && String(entry.id) === String(actionId));
      if (pending) {
        this.applyActionStatusTransition(pending, status);
      }
    }

    await this.persistLearningForDecision(action, status);
  }

  async finalizeApproval(decision) {
    if (!this.pendingActions) {
      return;
    }

    if (decision === 'approved') {
      this.updateActionStatuses(this.pendingActions, 'approved');
      await this.executeActions(this.pendingActions);
      this.pendingActions = null;
      return;
    }

    if (decision === 'rejected') {
      this.updateActionStatuses(this.pendingActions, 'rejected');
      for (const action of this.pendingActions) {
        action.executionResult = { status: 'skipped', reason: 'rejected' };
      }
      await this.persistActions(this.pendingActions, 'Actions rejected');
      await this.storeFeedbackEntries(
        this.pendingActions.map((action) =>
          this.buildFeedbackEntry(action, 'rejected', action.executionResult)
        )
      );
      await this.storeLearningEntries(
        this.pendingActions.map((action) => this.buildLearningEntry(action, 'rejected'))
      );
      await this.storeLearningHistoryEntries(
        this.pendingActions.map((action) => this.buildLearningHistoryEntry(action, 'rejected'))
      );
      this.log.info('Aktionen wurden abgelehnt.');
      this.pendingActions = null;
    }
  }

  async handleActionCallback(actionId, actionCommand, payload) {
    const actions = await this.loadActionsFromState();
    if (actions.length === 0) {
      this.log.warn('Keine gespeicherten Aktionen für Telegram Callback vorhanden.');
      return;
    }

    const action = actions.find((entry) => entry && String(entry.id) === String(actionId));
    if (!action) {
      this.log.warn(`Aktion nicht gefunden für Telegram Callback: ${actionId}`);
      return;
    }

    const normalizedCommand = this.normalizeActionCommand(actionCommand);
    if (!normalizedCommand) {
      this.log.warn(`Unbekanntes Telegram Kommando: ${actionCommand}`);
      return;
    }

    if (normalizedCommand === 'modify') {
      this.applyActionDecision(action, 'modified');
      await this.persistActions(actions, 'Action modification requested');
      this.awaitingTelegramInput = true;
      this.pendingModifyActionId = actionId;
      await this.sendTelegramActionConfirmation(action, 'modified');
      await this.updateTelegramOriginalMessage(payload, '✏️ Modification requested');
      await this.storeActionLearningDecision(actionId, 'modified');
      return;
    }

    if (normalizedCommand === 'reject') {
      const transitioned = this.applyActionStatusTransition(action, 'rejected');
      if (!transitioned) {
        return;
      }
      action.executionResult = { status: 'skipped', reason: 'rejected' };
      await this.persistActions(actions, 'Action rejected');
      await this.sendTelegramActionConfirmation(action, 'rejected');
      await this.updateTelegramOriginalMessage(payload, '❌ Rejected');
      await this.storeFeedbackEntries([this.buildFeedbackEntry(action, 'rejected', action.executionResult)]);
      await this.persistLearningForDecision(action, 'rejected');
      this.updatePendingAction(action);
      return;
    }

    if (normalizedCommand === 'approve') {
      const transitioned = this.applyActionStatusTransition(action, 'approved');
      if (!transitioned) {
        return;
      }
      await this.executeAction(action);
      await this.persistActions(actions, 'Action approved');
      await this.sendTelegramActionConfirmation(action, 'approved');
      await this.updateTelegramOriginalMessage(payload, '✅ Approved');
      await this.storeFeedbackEntries([this.buildFeedbackEntry(action, 'approved', action.executionResult)]);
      await this.persistLearningForDecision(action, 'executed');
      this.updatePendingAction(action);
      return;
    }

    if (normalizedCommand === 'executed') {
      const transitioned = this.applyActionStatusTransition(action, 'executed');
      if (!transitioned) {
        return;
      }
      await this.persistActions(actions, 'Action executed');
      await this.sendTelegramActionConfirmation(action, 'executed');
      await this.updateTelegramOriginalMessage(payload, '✅ Executed');
      await this.storeFeedbackEntries([this.buildFeedbackEntry(action, 'approved', action.executionResult)]);
      await this.persistLearningForDecision(action, 'executed');
      this.updatePendingAction(action);
    }
  }

  normalizeActionCommand(actionCommand) {
    switch (String(actionCommand || '').toLowerCase()) {
      case 'approve':
      case 'approved':
        return 'approve';
      case 'reject':
      case 'rejected':
        return 'reject';
      case 'modify':
        return 'modify';
      case 'executed':
        return 'executed';
      default:
        return null;
    }
  }

  async sendTelegramActionConfirmation(action, decision) {
    const actionLabel = this.describeAction(action);
    const prefixMap = {
      approved: '✅ Approved',
      rejected: '❌ Rejected',
      modified: '✏️ Modification requested',
      executed: '✅ Executed'
    };
    const prefix = prefixMap[decision] || 'ℹ️ Update';
    const text = `${prefix}: ${actionLabel}`;
    await this.sendTelegramMessage(text);
  }

  async updateTelegramOriginalMessage(payload, updateLabel) {
    if (!payload || !payload.message || !payload.message.message_id) {
      return;
    }
    if (!this.config.telegram.enabled || !this.config.telegram.instance) {
      return;
    }
    const messageId = payload.message.message_id;
    const chatId = payload.message.chat?.id || this.config.telegram.chatId;
    const originalText = payload.message.text || '';
    if (!originalText) {
      return;
    }
    const updatedText = `${originalText}\n\n${updateLabel}`;
    try {
      this.sendTo(this.config.telegram.instance, 'editMessageText', {
        chatId,
        message_id: messageId,
        text: updatedText,
        parse_mode: 'Markdown'
      });
      this.logDebug('Telegram message updated', { messageId, updateLabel });
    } catch (error) {
      this.handleError('Telegram message update failed', error, true);
    }
  }

  async executeAction(action) {
    const handlers = this.getActionHandlers();
    const handler = handlers[action.category];
    this.logDebug('Action execution intent', {
      actionId: action.id,
      description: this.describeAction(action),
      dryRun: this.config.dryRun
    });

    try {
      if (this.config.dryRun) {
        action.executionResult = { status: 'skipped', reason: 'dryRun' };
      } else if (!handler) {
        action.executionResult = { status: 'skipped', reason: 'noHandler' };
      } else {
        const result = await handler(action);
        action.executionResult = result || { status: 'success' };
      }
      this.applyActionStatusTransition(action, 'executed');
    } catch (error) {
      action.executionResult = { status: 'error', message: error.message };
      this.applyActionStatusTransition(action, 'failed');
      this.handleError(`Aktion fehlgeschlagen: ${this.describeAction(action)}`, error, true);
    }
  }

  formatActionLine(action) {
    const priority = this.normalizeActionPriority(action.priority);
    const priorityLabel = priority.toUpperCase();
    const categoryEmoji = this.getCategoryEmoji(action.category);
    const emoji = this.getPriorityEmoji(priority);
    const title = action.title || this.formatActionTitle(action.type);
    const detail = action.reason || action.description;
    const description = detail ? ` (${detail})` : '';
    return `- ${categoryEmoji} ${emoji} *${title}*${description} _[${priorityLabel}]_`;
  }

  getPriorityEmoji(priority) {
    switch (String(priority || '').toLowerCase()) {
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
      case 'safety':
        return '🛡️';
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
      safety: 'Sicherheit'
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

  getLiveRoleValue(entries, role) {
    const entry = (entries || []).find(
      (item) => item && item.role === role && Number.isFinite(Number(item.value))
    );
    return entry ? Number(entry.value) : null;
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

  async loadLearningHistory() {
    try {
      const existing = await this.getStateAsync('memory.learning');
      if (!existing || !existing.val) {
        this.learningHistory = [];
        return;
      }
      const parsed = JSON.parse(String(existing.val));
      if (Array.isArray(parsed)) {
        this.learningHistory = parsed.slice(-200);
      } else if (parsed && typeof parsed === 'object') {
        this.learningHistory = [parsed].slice(-200);
      } else {
        this.learningHistory = [];
      }
    } catch (error) {
      this.learningHistory = [];
      this.handleError('Learning Historie konnte nicht geladen werden', error, true);
    }
  }

  async loadLearningHistoryEntries() {
    try {
      const existing = await this.getStateAsync('memory.history');
      if (!existing || !existing.val) {
        this.learningHistoryEntries = [];
        return;
      }
      const parsed = JSON.parse(String(existing.val));
      if (Array.isArray(parsed)) {
        this.learningHistoryEntries = parsed.slice(-500);
      } else if (parsed && typeof parsed === 'object') {
        this.learningHistoryEntries = [parsed].slice(-500);
      } else {
        this.learningHistoryEntries = [];
      }
    } catch (error) {
      this.learningHistoryEntries = [];
      this.handleError('Learning History konnte nicht geladen werden', error, true);
    }
  }

  aggregateLearningStats(entries) {
    const stats = {
      totals: { approved: 0, rejected: 0, executed: 0 },
      byKey: {}
    };
    for (const entry of entries || []) {
      if (!entry) {
        continue;
      }
      const decision = String(entry.decision || '').toLowerCase();
      if (!['approved', 'rejected', 'executed'].includes(decision)) {
        continue;
      }
      stats.totals[decision] += 1;
      const key = entry.learningKey || 'unknown';
      if (!stats.byKey[key]) {
        stats.byKey[key] = { approved: 0, rejected: 0, executed: 0, total: 0 };
      }
      stats.byKey[key][decision] += 1;
      stats.byKey[key].total += 1;
    }
    return stats;
  }

  buildFeedbackContext(context, energySummary) {
    const timeLabel = new Date(context.timestamp).toLocaleTimeString('de-DE', {
      hour: '2-digit',
      minute: '2-digit'
    });
    const outsideTemp = this.getOutsideTemperature(context?.live?.temperature || []);
    return {
      houseConsumption: energySummary.houseConsumption ?? null,
      gridPower: energySummary.gridPower ?? null,
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

  buildLearningEntry(action, userDecision) {
    const context = action?.context || {};
    const timestamp = context.timestamp || new Date().toISOString();
    const timeOfDay = new Date(timestamp).toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
    return {
      learningKey: String(action.learningKey || 'unknown'),
      actionType: String(action.type || 'unknown'),
      userDecision,
      decision: userDecision,
      context: {
        timeOfDay,
        batterySoc: context.batterySoc ?? null,
        outsideTemp: context.outsideTemp ?? null,
        pvPower: context.pvPower ?? null
      },
      contextSnapshot: { ...context },
      timestamp: new Date().toISOString()
    };
  }

  buildLearningHistoryEntry(action, decision) {
    const context = action?.context || {};
    return {
      learningKey: String(action.learningKey || 'unknown'),
      actionId: String(action.id || ''),
      decision,
      timestamp: new Date().toISOString(),
      context: {
        batterySoc: context.batterySoc ?? null,
        houseConsumption: context.houseConsumption ?? null,
        outsideTemp: context.outsideTemp ?? null
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

  async storeLearningEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }
    try {
      this.learningHistory = [...this.learningHistory, ...entries].slice(-200);
      await this.setStateAsync('memory.learning', JSON.stringify(this.learningHistory, null, 2), true);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Learning entry stored (${entries.length} entries)`);
      }
    } catch (error) {
      this.log.warn(`Learning konnte nicht gespeichert werden: ${error.message}`);
    }
  }

  async storeLearningHistoryEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }
    try {
      this.learningHistoryEntries = [...this.learningHistoryEntries, ...entries].slice(-500);
      await this.setStateAsync('memory.history', JSON.stringify(this.learningHistoryEntries, null, 2), true);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Learning entries stored (${entries.length} entries)`);
        this.log.info(`[DEBUG] Learning entries: ${JSON.stringify(entries, null, 2)}`);
      }
    } catch (error) {
      this.log.warn(`Learning History konnte nicht gespeichert werden: ${error.message}`);
    }
  }

  wasActionExecuted(executionResult) {
    const status = executionResult?.status;
    return status === 'success' || status === 'error';
  }

  normalizeFeedbackCategory(category) {
    const normalized = String(category || '').toLowerCase();
    const allowed = new Set(['energy', 'water', 'heating', 'pv', 'safety']);
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
