'use strict';

module.exports = () => ({
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
    const intervalMs = intervalMin * this.constants.MINUTE_MS;

    this.intervalTimer = setInterval(() => {
      this.runAnalysisWithLock('interval').catch((error) => {
        this.handleError('Interval analysis failed', error);
      });
    }, intervalMs);

    this.runAnalysisWithLock('startup').catch((error) => {
      this.handleError('Startup analysis failed', error);
    });
  },

  startDailyReportScheduler() {
    if (this.dailyReportTimer) {
      clearInterval(this.dailyReportTimer);
      this.dailyReportTimer = null;
    }

    const dailyReportConfig = this.getDailyReportConfig();
    if (!dailyReportConfig.enabled) {
      this.logDebug('Daily report scheduler skipped (disabled)');
      return;
    }

    this.logDebug('Daily report scheduler initialized');
    this.dailyReportTimer = setInterval(() => {
      this.runDailyReportIfDue().catch((error) => {
        this.handleError('Daily report failed', error, true);
      });
    }, this.constants.MINUTE_MS);
    this.runDailyReportIfDue().catch((error) => {
      this.handleError('Daily report failed', error, true);
    });
  },

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
    if (!this.isDailyReportScheduledDay(now, dailyReportConfig)) {
      this.logDebug('Daily report skipped (day not scheduled)');
      return;
    }

    if (!this.isDailyReportTimeMatch(now, dailyReportConfig)) {
      this.logDailyReportDebug('Daily report skipped – reason: wrong time');
      return;
    }

    const lastSentDate = await this.readLastDailyReportDate();
    if (lastSentDate && this.isSameCalendarDay(now, lastSentDate, dailyReportConfig.timezone)) {
      this.logDailyReportDebug('Daily report skipped – reason: already sent');
      return;
    }

    const report = await this.buildDailyReport(dailyReportConfig, now);
    if (!report) {
      this.logDebug('Daily report skipped (empty report)');
      return;
    }

    try {
      await this.sendTelegramMessage(report, { parseMode: 'Markdown' });
      await this.setStateAsync('meta.lastDailyReportTs', now.toISOString(), true);
      await this.setStateAsync('report.dailyLastSent', this.formatDateStamp(now, dailyReportConfig.timezone), true);
      this.logDailyReportDebug('Daily report sent successfully');
    } catch (error) {
      this.handleError('Daily report send failed', error, true);
    }
  },

  async buildDailyReport(config, now = new Date()) {
    await this.loadLearningHistoryEntries();
    const actionHistory = await this.loadActionHistory();
    const timeZone = config.timezone || null;
    const liveData = await this.collectLiveData();
    const historyData = this.isHistoryEnabled()
      ? await this.collectHistoryData()
      : { influx: { series: [], pointsLoaded: 0 }, mysql: { series: [], pointsLoaded: 0 } };
    const aggregates = this.aggregateData(historyData);
    const context = await this.buildContext(liveData, aggregates);
    const energySummary = this.buildEnergySummary(context.live.energy);
    const houseStats = this.resolveHouseConsumptionStats(aggregates, energySummary.houseConsumption);
    const batterySocStats = this.resolveBatterySocStats(aggregates, energySummary.batterySoc);
    const pvDailyEnergy = (await this.sumDailyPvEnergy(liveData)) ?? energySummary.pvDailyEnergy;
    const waterSeries = this.findHistorySeries(historyData, this.getWaterBaselineId(), ['water', 'flow']);
    const waterStats = this.computeSeriesStats(waterSeries?.values || []);
    const waterTotal = this.resolveWaterTotal(liveData, waterStats);
    const waterBreakdown = this.resolveWaterBreakdown(liveData);
    const energyShare = this.calculateEnergyShare(energySummary);
    const rangeStart = new Date(now.getTime() - this.constants.DAY_MS);
    const summary = this.lastContextSummary || this.buildEmptySummary();
    const deviations = Array.isArray(context.history?.deviations)
      ? context.history.deviations
      : Array.isArray(this.lastHistoryDeviations)
        ? this.lastHistoryDeviations
        : [];
    const recentActions = this.filterActionsByWindow(actionHistory, rangeStart);
    const recentLearning = this.filterLearningByWindow(this.learningHistoryEntries, rangeStart);

    return this.buildDailyReportText({
      timeZone,
      now,
      rangeStart,
      include: config.include,
      summary,
      liveData,
      energySummary,
      energyShare,
      houseStats,
      batterySocStats,
      pvDailyEnergy,
      waterTotal,
      waterBreakdown,
      deviations,
      actions: this.summarizeActions(recentActions),
      learning: this.summarizeLearning(recentLearning)
    });
  },

  filterActionsByWindow(actions, sinceDate) {
    const since = sinceDate.getTime();
    return (actions || []).filter((action) => {
      const timestamp = this.getActionTimestamp(action);
      return Number.isFinite(timestamp) && timestamp >= since;
    });
  },

  filterLearningByWindow(entries, sinceDate) {
    const since = sinceDate.getTime();
    return (entries || []).filter((entry) => {
      const timestamp = Date.parse(entry?.timestamp);
      return Number.isFinite(timestamp) && timestamp >= since;
    });
  },

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
  },

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
  },

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
  },

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

    const lines = [
      '🤖🏠 *AI Autopilot – Daily Report*',
      `📅 ${dateLabel} · ${timeLabel}`
    ];

    if (data.include.summary) {
      lines.push(
        '',
        '⚡ *Energy Summary*',
        `- Hausverbrauch Ø/Min/Max: ${this.formatWatts(data.houseStats.avg)} / ${this.formatWatts(
          data.houseStats.min
        )} / ${this.formatWatts(data.houseStats.max)}`
      );
      if (data.energyShare) {
        lines.push(
          `- Quellenanteile: Netz ${data.energyShare.gridPct}% • PV ${data.energyShare.pvPct}% • Batterie ${data.energyShare.batteryPct}%`
        );
      }
      if (Number.isFinite(data.energySummary.gridPower)) {
        lines.push(`- Aktueller Netzbezug: ${this.formatWatts(data.energySummary.gridPower)}`);
      }
    }

    lines.push(
      '',
      '🔋 *Battery*',
      `- SOC Min/Max: ${this.formatSocRange(data.batterySocStats.min, data.batterySocStats.max)}`
    );

    if (Number.isFinite(data.pvDailyEnergy)) {
      lines.push('', '🌞 *PV*', `- Tagesenergie: ${this.formatKwh(data.pvDailyEnergy)}`);
    }

    if (Number.isFinite(data.waterTotal) || Number.isFinite(data.waterBreakdown?.hot)) {
      lines.push('', '🚰 *Water*');
      if (Number.isFinite(data.waterTotal)) {
        lines.push(`- Gesamt: ${this.formatLiters(data.waterTotal)}`);
      }
      if (Number.isFinite(data.waterBreakdown?.hot) || Number.isFinite(data.waterBreakdown?.cold)) {
        lines.push(
          `- Warm/Kalt: ${this.formatLiters(data.waterBreakdown.hot)} / ${this.formatLiters(
            data.waterBreakdown.cold
          )}`
        );
      }
    }

    if (data.include.deviations) {
      const deviations = data.deviations || [];
      lines.push('', '📈 *Deviations*');
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
        '🧠 *Learning Insights*',
        `- Genehmigt: ${data.learning.approved}`,
        `- Abgelehnt: ${data.learning.rejected}`,
        `- Umgesetzt: ${data.learning.executed}`,
        `- Geändert: ${data.learning.modified}`
      );
    }

    if (data.include.actions) {
      lines.push(
        '',
        '✅ *Actions*',
        `- Vorgeschlagen: ${data.actions.proposed}`,
        `- Freigegeben: ${data.actions.approved}`,
        `- Abgelehnt: ${data.actions.rejected}`,
        `- Umgesetzt: ${data.actions.executed}`
      );
    }

    return lines.join('\n');
  },

  resolveHouseConsumptionStats(aggregates, fallbackValue) {
    const aggregate = this.findAggregateByRole(aggregates, ['house', 'consumption']);
    return {
      avg: Number.isFinite(aggregate?.avg) ? aggregate.avg : fallbackValue,
      min: Number.isFinite(aggregate?.min) ? aggregate.min : fallbackValue,
      max: Number.isFinite(aggregate?.max) ? aggregate.max : fallbackValue
    };
  },

  resolveBatterySocStats(aggregates, fallbackValue) {
    const aggregate = this.findAggregateByRole(aggregates, ['battery', 'soc']);
    return {
      min: Number.isFinite(aggregate?.min) ? aggregate.min : fallbackValue,
      max: Number.isFinite(aggregate?.max) ? aggregate.max : fallbackValue
    };
  },

  findAggregateByRole(aggregates, roleHints) {
    const hints = (roleHints || []).map((hint) => String(hint).toLowerCase());
    const series = [...(aggregates?.influx || []), ...(aggregates?.mysql || [])].find((item) => {
      const role = String(item?.role || '').toLowerCase();
      return hints.some((hint) => role.includes(hint));
    });
    return series?.aggregate || null;
  },

  calculateEnergyShare(energySummary) {
    const gridImport = Number.isFinite(energySummary.gridPower) ? Math.max(0, energySummary.gridPower) : 0;
    const pvPower = Number.isFinite(energySummary.pvPower) ? Math.max(0, energySummary.pvPower) : 0;
    const batteryDischarge = Number.isFinite(energySummary.batteryPower) ? Math.max(0, -energySummary.batteryPower) : 0;
    const total = gridImport + pvPower + batteryDischarge;
    if (!Number.isFinite(total) || total <= 0) {
      return null;
    }
    const toPct = (value) => Math.round((value / total) * 100);
    return {
      gridPct: toPct(gridImport),
      pvPct: toPct(pvPower),
      batteryPct: toPct(batteryDischarge)
    };
  },

  isDailyReportTimeMatch(now, config) {
    const { hour, minute } = this.getDailyReportScheduleParts(config);
    if (config.timezone) {
      const nowParts = this.getDatePartsInTimeZone(now, config.timezone);
      return nowParts.hour === hour && nowParts.minute === minute;
    }
    return now.getHours() === hour && now.getMinutes() === minute;
  },

  isDailyReportScheduledDay(now, config) {
    const days = Array.isArray(config.days) ? config.days : [];
    if (days.length === 0) {
      return false;
    }
    const dayIndex = config.timezone ? this.getWeekdayIndexInTimeZone(now, config.timezone) : now.getDay();
    return days.includes(dayIndex);
  },

  async readLastDailyReportDate() {
    const value = await this.readState('meta.lastDailyReportTs');
    if (!value) {
      return null;
    }
    const parsed = Number.isFinite(Number(value)) ? new Date(Number(value)) : new Date(String(value));
    if (!Number.isFinite(parsed.getTime())) {
      return null;
    }
    return parsed;
  },

  isSameCalendarDay(first, second, timeZone) {
    if (!(first instanceof Date) || !(second instanceof Date)) {
      return false;
    }
    const firstStamp = this.formatDateStamp(first, timeZone);
    const secondStamp = this.formatDateStamp(second, timeZone);
    return firstStamp === secondStamp;
  },

  getWeekdayIndexInTimeZone(date, timeZone) {
    const label = new Intl.DateTimeFormat('en-US', { timeZone, weekday: 'short' }).format(date);
    const map = {
      Sun: 0,
      Mon: 1,
      Tue: 2,
      Wed: 3,
      Thu: 4,
      Fri: 5,
      Sat: 6
    };
    return map[label] ?? date.getDay();
  },

  logDailyReportDebug(message) {
    if (!this.config.debug) {
      return;
    }
    this.log.info(`[DEBUG] ${message}`);
  },

  formatWatts(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    if (Math.abs(value) >= 1000) {
      return `${(value / 1000).toFixed(2)} kW`;
    }
    return `${Math.round(value)} W`;
  },

  formatKwh(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    return `${value.toFixed(1)} kWh`;
  },

  formatLiters(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    return `${Math.round(value)} l`;
  },

  formatTemperature(value) {
    if (!Number.isFinite(value)) {
      return 'n/a';
    }
    return `${value.toFixed(1)} °C`;
  },

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
});
