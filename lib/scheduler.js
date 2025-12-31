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
  },

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
  },

  async buildDailyReport(config, now = new Date()) {
    await this.loadLearningHistoryEntries();
    const actionHistory = await this.loadActionHistory();
    const timeZone = config.timezone || null;
    const rangeStart = new Date(now.getTime() - this.constants.DAY_MS);
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
