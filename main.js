'use strict';

const utils = require('@iobroker/adapter-core');
const OpenAI = require('openai');

const configMethods = require('./lib/config');
const stateMethods = require('./lib/state');
const liveContextMethods = require('./lib/liveContext');
const historyMethods = require('./lib/history');
const rulesMethods = require('./lib/rules');
const gptMethods = require('./lib/gpt');
const actionsMethods = require('./lib/actions');
const learningMethods = require('./lib/learning');
const telegramMethods = require('./lib/telegram');
const schedulerMethods = require('./lib/scheduler');

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
    this.constants = {
      MINUTE_MS,
      HOUR_MS,
      DAY_MS,
      DAY_START_HOUR,
      NIGHT_START_HOUR,
      GPT_LOG_TRIM,
      DEFAULT_GRID_POWER_THRESHOLD
    };
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

Object.assign(
  AiAutopilot.prototype,
  configMethods(),
  stateMethods(),
  liveContextMethods(),
  historyMethods(),
  rulesMethods(),
  gptMethods(),
  actionsMethods(),
  learningMethods(),
  telegramMethods(),
  schedulerMethods()
);

if (module.parent) {
  module.exports = (options) => new AiAutopilot(options);
} else {
  new AiAutopilot();
}
