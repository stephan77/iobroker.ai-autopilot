'use strict';

module.exports = () => ({
  getDailyReportConfig() {
    const legacyReport = this.config.dailyReport || {};
    const include = legacyReport.include || {};
    const newEnabled = this.config.dailyReportEnabled;
    const legacyEnabled = Boolean(legacyReport.enabled);
    const enabled = Boolean(newEnabled) || legacyEnabled;
    const hasNewTime = typeof this.config.dailyReportTime === 'string' && this.config.dailyReportTime.trim().length > 0;
    const time =
      (Boolean(newEnabled) && hasNewTime ? this.config.dailyReportTime : legacyReport.time) ||
      (hasNewTime ? this.config.dailyReportTime : null) ||
      '19:00';
    return {
      enabled,
      time,
      days: this.normalizeDailyReportDays(this.config.dailyReportDays),
      timezone: this.normalizeTimeZone(legacyReport.timezone),
      include: {
        summary: include.summary !== false,
        actions: include.actions !== false,
        learning: include.learning !== false,
        deviations: include.deviations !== false
      }
    };
  },

  normalizeDailyReportDays(days) {
    const fallback = [1, 2, 3, 4, 5];
    if (days === undefined || days === null) {
      return fallback;
    }
    const values = Array.isArray(days)
      ? days
      : String(days)
          .split(',')
          .map((entry) => entry.trim())
          .filter(Boolean);
    const normalized = values
      .map((entry) => Number(entry))
      .filter((value) => Number.isInteger(value) && value >= 0 && value <= 6);
    if (normalized.length === 0) {
      return fallback;
    }
    return [...new Set(normalized)];
  },

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
  },

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
  },

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
        const tomorrowParts = this.getDatePartsInTimeZone(new Date(now.getTime() + this.constants.DAY_MS), timeZone);
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
  },

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
  },

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
  },

  buildDateInTimeZone(parts, timeZone) {
    const utcGuess = new Date(Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second));
    const offset = this.getTimeZoneOffset(utcGuess, timeZone);
    return new Date(utcGuess.getTime() - offset);
  },

  formatDateStamp(date, timeZone) {
    const parts = timeZone ? this.getDatePartsInTimeZone(date, timeZone) : {
      year: date.getFullYear(),
      month: date.getMonth() + 1,
      day: date.getDate()
    };
    const month = String(parts.month).padStart(2, '0');
    const day = String(parts.day).padStart(2, '0');
    return `${parts.year}-${month}-${day}`;
  },

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
  },

  isHistoryEnabled() {
    return Boolean(this.config.history?.influx?.enabled || this.config.history?.mysql?.enabled);
  }
});
