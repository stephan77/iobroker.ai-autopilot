'use strict';

module.exports = () => ({
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

    await this.setObjectNotExistsAsync('actions.pending', {
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

    await this.setObjectNotExistsAsync('actions.approved', {
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

    await this.setObjectNotExistsAsync('actions.rejected', {
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

    await this.setObjectNotExistsAsync('actions.executed', {
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

    await this.setObjectNotExistsAsync('meta.running', {
      type: 'state',
      common: {
        type: 'boolean',
        role: 'indicator.running',
        read: true,
        write: false,
        def: false
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('meta.lastRun', {
      type: 'state',
      common: {
        type: 'string',
        role: 'value.time',
        read: true,
        write: false,
        def: ''
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('meta.lastTelegram', {
      type: 'state',
      common: {
        type: 'string',
        role: 'value.time',
        read: true,
        write: false,
        def: ''
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('meta.lastDailyReportTs', {
      type: 'state',
      common: {
        type: 'string',
        role: 'value.time',
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

    await this.setObjectNotExistsAsync('config.mode', {
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

    await this.setObjectNotExistsAsync('config.dryRun', {
      type: 'state',
      common: {
        type: 'boolean',
        role: 'indicator',
        read: true,
        write: false,
        def: false
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('config.intervalMin', {
      type: 'state',
      common: {
        type: 'number',
        role: 'value',
        read: true,
        write: false,
        def: 0
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('config.telegram.enabled', {
      type: 'state',
      common: {
        type: 'boolean',
        role: 'indicator',
        read: true,
        write: false,
        def: false
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('config.telegram.chatId', {
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

    await this.setObjectNotExistsAsync('config.dailyReport.enabled', {
      type: 'state',
      common: {
        type: 'boolean',
        role: 'indicator',
        read: true,
        write: false,
        def: false
      },
      native: {}
    });

    await this.setObjectNotExistsAsync('config.dailyReport.time', {
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

    await this.setObjectNotExistsAsync('config.dailyReport.days', {
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
  },
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
        await this.runAnalysis(); // <-- HIER MUSS GPT AUFGERUFEN WERDEN
      } catch (e) {
        this.log.error('Analysis failed: ' + e.message);
      } finally {
        this.running = false;
      }
    }

    if (id === `${this.namespace}.memory.feedback`) {
      await this.processFeedback(String(state.val || '').trim());
    }
  },

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
});
