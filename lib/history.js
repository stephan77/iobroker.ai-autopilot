'use strict';

module.exports = () => ({
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
  },

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
  },

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
  },

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
      const deltaHours = (next.ts - current.ts) / this.constants.HOUR_MS;
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
  },

  getWaterBaselineId() {
    return this.config.water?.daily || this.config.water?.total || this.config.water?.flow || null;
  },

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
  },

  resolveWaterBreakdown(liveData) {
    return {
      hot: Number.isFinite(liveData.water?.hotWater) ? liveData.water.hotWater : null,
      cold: Number.isFinite(liveData.water?.coldWater) ? liveData.water.coldWater : null
    };
  },

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
      if (hour >= this.constants.DAY_START_HOUR && hour < this.constants.NIGHT_START_HOUR) {
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
  },

  isFrostDetected(values, frostRiskState) {
    if (frostRiskState === true || frostRiskState === 'true' || frostRiskState === 1) {
      return true;
    }
    if (!Array.isArray(values) || values.length === 0) {
      return false;
    }
    const stats = this.computeSeriesStats(values);
    return Number.isFinite(stats.min) && stats.min <= 0;
  },

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
  },

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
      step: resolutionMin * this.constants.MINUTE_MS
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
  },

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
  },

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
  },

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
  },

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
        if (hour >= this.constants.DAY_START_HOUR && hour < this.constants.NIGHT_START_HOUR) {
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
  },

  async collectHistoryData() {
    const influxData = await this.collectHistoryFromConfig(this.config.history.influx, this.constants.HOUR_MS);
    const mysqlData = await this.collectHistoryFromConfig(this.config.history.mysql, this.constants.DAY_MS);

    return { influx: influxData, mysql: mysqlData };
  },

  getHistoryCategory(role) {
    const normalized = String(role || '').toLowerCase();
    if (normalized.includes('water')) {
      return 'water';
    }
    if (normalized.includes('temp') || normalized.includes('temperature') || normalized.includes('outside')) {
      return 'temperature';
    }
    return 'energy';
  },

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
});
